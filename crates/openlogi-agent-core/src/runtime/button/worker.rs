//! The button runtime's worker thread: draining commands and inputs, settling long presses, and emitting the events the owner subscribed to.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use openlogi_hook::PointerTarget;
use tracing::debug;

use super::{
    ActionDispatchTarget, ActivePress, ButtonCommand, ButtonInput, ButtonRuntimeEvent, ButtonState,
    CancelReason, EVENT_QUEUE_CAPACITY, EndReason, PressBehavior, PressKey, PressToken,
    ShutdownRequest,
};

/// Worker-local state spanning commands: the invalidation generation the
/// worker has synchronized to, and the pointer query on-demand presses use.
pub(super) struct WorkerContext<'a> {
    shared_generation: &'a AtomicU64,
    generation: u64,
    pointer: &'a mut dyn FnMut() -> PointerTarget,
}

/// The outcome of admitting one queued input.
enum Admission {
    Accepted(ButtonInput),
    /// No pointer target could be resolved for an on-demand press.
    Rejected(PressKey),
    /// The generation was invalidated while the target was being queried.
    Stale,
}

impl<'a> WorkerContext<'a> {
    pub(super) fn new(
        shared_generation: &'a AtomicU64,
        pointer: &'a mut dyn FnMut() -> PointerTarget,
    ) -> Self {
        Self {
            generation: shared_generation.load(Ordering::Acquire),
            shared_generation,
            pointer,
        }
    }

    /// Whether an invalidation was published after the last synchronization.
    fn invalidated(&self) -> bool {
        self.shared_generation.load(Ordering::Acquire) != self.generation
    }

    fn synchronize(&mut self, state: &mut ButtonState, emit: &mut impl FnMut(ButtonRuntimeEvent)) {
        let current = self.shared_generation.load(Ordering::Acquire);
        if current != self.generation {
            emit_canceled(state.cancel_all(), CancelReason::Invalidated, emit);
            self.generation = current;
        }
    }

    /// Resolve an on-demand press to the target under the pointer now. The
    /// query may block, so an invalidation published meanwhile wins: stale
    /// input never starts a lifecycle.
    fn admit(&mut self, mut input: ButtonInput) -> Admission {
        let (ButtonInput::Down(press) | ButtonInput::Pulse(press)) = &mut input else {
            return Admission::Accepted(input);
        };
        if press.target != ActionDispatchTarget::PointerOnDemand {
            return Admission::Accepted(input);
        }
        let target = (self.pointer)();
        if self.invalidated() {
            return Admission::Stale;
        }
        if matches!(
            target,
            PointerTarget::Unavailable | PointerTarget::Unsupported
        ) {
            return Admission::Rejected(press.token.key.clone());
        }
        press.target = ActionDispatchTarget::AdmittedPointer(target);
        Admission::Accepted(input)
    }
}

pub(super) fn run_worker(
    events: &mpsc::Receiver<ButtonCommand>,
    shutdown: &mpsc::Receiver<ShutdownRequest>,
    context: WorkerContext<'_>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    run_worker_observing_wait(events, shutdown, context, emit, &mut |_| {});
}

pub(super) fn run_worker_observing_wait(
    events: &mpsc::Receiver<ButtonCommand>,
    shutdown: &mpsc::Receiver<ShutdownRequest>,
    mut context: WorkerContext<'_>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
    before_wait: &mut impl FnMut(Option<Instant>),
) {
    let mut state = ButtonState::default();
    loop {
        if finish_shutdown_if_requested(shutdown, &mut state, emit) {
            return;
        }
        let deadline = state.next_deadline();
        before_wait(deadline);
        let received = match deadline {
            Some(deadline) => {
                events.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => events
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
        };
        let command = match received {
            Ok(command) => Some(command),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                emit_canceled(state.cancel_all(), CancelReason::SourceEnded, emit);
                return;
            }
        };
        context.synchronize(&mut state, emit);
        let first_command = match command {
            Some(command) if !state.has_due_long_press(Instant::now()) => {
                process_command(&mut state, command, &mut context, emit);
                None
            }
            command => command,
        };
        if settle_due_long_presses(
            events,
            shutdown,
            &mut context,
            &mut state,
            first_command,
            emit,
        ) {
            return;
        }
    }
}

/// End on-demand presses whose admitted target left the pointer, once their
/// revalidation period is due. Queries nothing while no such press is active.
fn revalidate_due_pointer_presses(
    state: &mut ButtonState,
    context: &mut WorkerContext<'_>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    if state
        .pointer_check_due()
        .is_none_or(|due| due > Instant::now())
    {
        return;
    }
    let current = (context.pointer)();
    if context.invalidated() {
        // The next synchronization cancels every press, admitted or not.
        return;
    }
    // Rescheduling from completion rather than from the missed deadline
    // keeps a slow query from replaying a burst of catch-up checks.
    emit_canceled(
        state.revalidate_pointer(current, Instant::now()),
        CancelReason::Invalidated,
        emit,
    );
}

fn process_command(
    state: &mut ButtonState,
    command: ButtonCommand,
    context: &mut WorkerContext<'_>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    match command {
        ButtonCommand::Input { generation, input } if generation == context.generation => {
            match context.admit(input) {
                Admission::Accepted(input) => process_input(state, input, emit),
                Admission::Rejected(key) => {
                    debug!("no pointer target at admission — press runs no action");
                    // The rejected down edge still proves a lost release.
                    if let Some(stale) = state.release(&key) {
                        emit(ButtonRuntimeEvent::Ended {
                            press: stale,
                            reason: EndReason::Canceled(CancelReason::RepeatedDown),
                        });
                    }
                }
                Admission::Stale => {}
            }
        }
        ButtonCommand::Input { .. } | ButtonCommand::Wake => {}
        ButtonCommand::CancelStalePress(token) => {
            if let Some(press) = state.cancel_press(&token) {
                emit(ButtonRuntimeEvent::Ended {
                    press,
                    reason: EndReason::Canceled(CancelReason::StaleHold),
                });
            }
        }
        ButtonCommand::CancelSource(source) => {
            emit_canceled(
                state.cancel_source(&source),
                CancelReason::SourceEnded,
                emit,
            );
        }
        ButtonCommand::CancelHooks(reason) => {
            emit_canceled(state.cancel_hooks(), reason, emit);
        }
        ButtonCommand::CancelFocused => {
            emit_canceled(state.cancel_focused(), CancelReason::Invalidated, emit);
        }
        ButtonCommand::CancelPointerExcept(current) => {
            emit_canceled(
                state.cancel_pointer_except(current),
                CancelReason::Invalidated,
                emit,
            );
        }
    }
}

pub(super) fn settle_due_long_presses(
    events: &mpsc::Receiver<ButtonCommand>,
    shutdown: &mpsc::Receiver<ShutdownRequest>,
    context: &mut WorkerContext<'_>,
    state: &mut ButtonState,
    first_command: Option<ButtonCommand>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) -> bool {
    if finish_shutdown_if_requested(shutdown, state, emit) {
        return true;
    }
    // An event handler may have blocked while another thread invalidated
    // this generation. Observe that transition before any overdue timer.
    context.synchronize(state, emit);
    // Every turn settles here, so continuous input cannot starve the pointer
    // check, and a press that left its window ends before its long action.
    revalidate_due_pointer_presses(state, context, emit);
    let due_presses = state.due_long_presses(Instant::now());
    if due_presses.is_empty() {
        if let Some(command) = first_command {
            process_command(state, command, context, emit);
        }
        return false;
    }

    // Before firing an overdue timer, settle one channel-capacity snapshot.
    // FIFO ordering guarantees that this includes every command that was
    // already queued at the checkpoint. State transitions are separated from
    // synchronous handlers so unrelated actions cannot delay the deadline.
    let mut deferred = Vec::new();
    let queued_limit = if let Some(command) = first_command {
        process_command(state, command, context, &mut |event| {
            deferred.push(event);
        });
        if finish_deferred_shutdown_if_requested(shutdown, state, &due_presses, &mut deferred, emit)
        {
            return true;
        }
        context.synchronize(state, &mut |event| {
            deferred.push(event);
        });
        EVENT_QUEUE_CAPACITY - 1
    } else {
        EVENT_QUEUE_CAPACITY
    };
    for _ in 0..queued_limit {
        if finish_deferred_shutdown_if_requested(shutdown, state, &due_presses, &mut deferred, emit)
        {
            return true;
        }
        context.synchronize(state, &mut |event| {
            deferred.push(event);
        });
        let command = match events.try_recv() {
            Ok(command) => command,
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => {
                emit_canceled(
                    state.cancel_all(),
                    CancelReason::SourceEnded,
                    &mut |event| deferred.push(event),
                );
                emit_settled_events(deferred, &due_presses, emit);
                return true;
            }
        };
        process_command(state, command, context, &mut |event| {
            deferred.push(event);
        });
        if finish_deferred_shutdown_if_requested(shutdown, state, &due_presses, &mut deferred, emit)
        {
            return true;
        }
        context.synchronize(state, &mut |event| {
            deferred.push(event);
        });
    }

    emit_selected_long_presses(state, &due_presses, Instant::now(), &mut |event| {
        deferred.push(event);
    });
    emit_settled_events(deferred, &due_presses, emit);
    false
}

fn finish_deferred_shutdown_if_requested(
    shutdown: &mpsc::Receiver<ShutdownRequest>,
    state: &mut ButtonState,
    due_presses: &[PressToken],
    deferred: &mut Vec<ButtonRuntimeEvent>,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) -> bool {
    let Ok(request) = shutdown.try_recv() else {
        return false;
    };
    emit_canceled(state.cancel_all(), CancelReason::Shutdown, &mut |event| {
        deferred.push(event);
    });
    emit_settled_events(std::mem::take(deferred), due_presses, emit);
    let _ = request.done.send(());
    true
}

fn emit_settled_events(
    events: Vec<ButtonRuntimeEvent>,
    due_presses: &[PressToken],
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    let (deadline_events, ordered_events): (Vec<_>, Vec<_>) =
        events.into_iter().partition(|event| match event {
            ButtonRuntimeEvent::Triggered { press, .. }
            | ButtonRuntimeEvent::Ended { press, .. } => {
                matches!(press.behavior, PressBehavior::LongPressFired)
                    && due_presses.contains(press.token())
            }
            ButtonRuntimeEvent::Started(_) => false,
        });
    for event in deadline_events.into_iter().chain(ordered_events) {
        emit(event);
    }
}

fn finish_shutdown_if_requested(
    shutdown: &mpsc::Receiver<ShutdownRequest>,
    state: &mut ButtonState,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) -> bool {
    let Ok(request) = shutdown.try_recv() else {
        return false;
    };
    emit_canceled(state.cancel_all(), CancelReason::Shutdown, emit);
    let _ = request.done.send(());
    true
}

pub(super) fn process_input(
    state: &mut ButtonState,
    input: ButtonInput,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    match input {
        ButtonInput::Down(press) => {
            if let Some(stale) = state.press(press.clone()) {
                emit(ButtonRuntimeEvent::Ended {
                    press: stale,
                    reason: EndReason::Canceled(CancelReason::RepeatedDown),
                });
            }
            emit(ButtonRuntimeEvent::Started(press));
        }
        ButtonInput::Up { key, released_at } => {
            if let Some(mut press) = state.release(&key) {
                if let Some(action) = press.fire_long(released_at) {
                    emit(ButtonRuntimeEvent::Triggered {
                        press: press.clone(),
                        action,
                    });
                }
                emit_released(press, emit);
            }
        }
        ButtonInput::Pulse(press) => {
            if let Some(stale) = state.press(press.clone()) {
                emit(ButtonRuntimeEvent::Ended {
                    press: stale,
                    reason: EndReason::Canceled(CancelReason::RepeatedDown),
                });
            }
            emit(ButtonRuntimeEvent::Started(press.clone()));
            if let Some(press) = state.release(&press.token.key) {
                emit_released(press, emit);
            }
        }
        ButtonInput::TriggerWhilePressed { token, action } => {
            if let Some(press) = state.active(&token).cloned() {
                emit(ButtonRuntimeEvent::Triggered { press, action });
            }
        }
    }
}

pub(super) fn emit_selected_long_presses(
    state: &mut ButtonState,
    tokens: &[PressToken],
    now: Instant,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    for (press, action) in state.fire_selected_long_presses(tokens, now) {
        emit(ButtonRuntimeEvent::Triggered { press, action });
    }
}

fn emit_released(press: ActivePress, emit: &mut impl FnMut(ButtonRuntimeEvent)) {
    if let Some(action) = press.release_action().cloned() {
        emit(ButtonRuntimeEvent::Triggered {
            press: press.clone(),
            action,
        });
    }
    emit(ButtonRuntimeEvent::Ended {
        press,
        reason: EndReason::Released,
    });
}

pub(super) fn emit_canceled(
    presses: Vec<ActivePress>,
    reason: CancelReason,
    emit: &mut impl FnMut(ButtonRuntimeEvent),
) {
    for press in presses {
        emit(ButtonRuntimeEvent::Ended {
            press,
            reason: EndReason::Canceled(reason),
        });
    }
}
