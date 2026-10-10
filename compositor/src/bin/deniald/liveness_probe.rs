//! Compositor liveness heartbeat used to discriminate a dead process from a
//! blocked main thread during field diagnosis. The calloop dispatch site
//! touches the shared state once per loop iteration; a detached thread logs a
//! compact line every few seconds. If the heartbeat keeps logging while the
//! dispatch sequence stalls, the main thread is blocked inside (or before) a
//! dispatch instead of having exited.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub(super) struct DispatchLiveness {
    started: Instant,
    dispatch_seq: AtomicU64,
    last_dispatch_elapsed_ms: AtomicU64,
    in_flight: AtomicBool,
    /// Coarse main-loop phase marker (see the checkpoints in
    /// `run_flutter_event_loop`); the heartbeat reports it so a blocked body
    /// section is identifiable without any per-section logging.
    body_phase: AtomicU64,
}

impl DispatchLiveness {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            dispatch_seq: AtomicU64::new(0),
            last_dispatch_elapsed_ms: AtomicU64::new(0),
            in_flight: AtomicBool::new(false),
            body_phase: AtomicU64::new(0),
        }
    }

    pub(super) fn enter_dispatch(&self) {
        self.body_phase.store(1000, Ordering::Relaxed);
        self.in_flight.store(true, Ordering::Relaxed);
    }

    pub(super) fn set_phase(&self, phase: u64) {
        self.body_phase.store(phase, Ordering::Relaxed);
    }

    pub(super) fn exit_dispatch(&self) {
        self.last_dispatch_elapsed_ms.store(
            u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.dispatch_seq.fetch_add(1, Ordering::Relaxed);
        self.in_flight.store(false, Ordering::Relaxed);
    }

    pub(super) fn spawn_heartbeat(self: &std::sync::Arc<Self>) {
        let shared = std::sync::Arc::clone(self);
        let _ = std::thread::Builder::new()
            .name("liveness".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(2));
                let elapsed_ms =
                    u64::try_from(shared.started.elapsed().as_millis()).unwrap_or(u64::MAX);
                let last = shared.last_dispatch_elapsed_ms.load(Ordering::Relaxed);
                let stale = elapsed_ms.saturating_sub(last);
                tracing::info!(
                    elapsed_ms,
                    dispatch_seq = shared.dispatch_seq.load(Ordering::Relaxed),
                    in_flight = shared.in_flight.load(Ordering::Relaxed),
                    stale_ms = stale,
                    phase = shared.body_phase.load(Ordering::Relaxed),
                    "compositor liveness heartbeat"
                );
                // Side-channel heartbeat: the tracing writer shares the stderr
                // mutex with every other log site, so a wedged writer would
                // silence this line too. The direct file append survives that
                // and discriminates "logging wedged" from "threads dead".
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/deniald-alive.log")
                {
                    use std::io::Write as _;
                    let _ = writeln!(
                        file,
                        "{} seq={} in_flight={} stale_ms={}",
                        elapsed_ms,
                        shared.dispatch_seq.load(Ordering::Relaxed),
                        shared.in_flight.load(Ordering::Relaxed),
                        stale
                    );
                }
            });
    }
}
