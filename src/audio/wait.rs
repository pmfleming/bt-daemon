//! Bounded event-driven confirmation, independent of protocol synchronization.
use std::{cell::RefCell, rc::Rc, time::Duration};

use anyhow::{Context, Result};
use pipewire as pw;

#[derive(Clone)]
pub(super) struct Completion {
    state: Rc<RefCell<Option<Result<(), String>>>>,
    main_loop: pw::main_loop::MainLoopRc,
}

impl Completion {
    pub fn new(main_loop: &pw::main_loop::MainLoopRc) -> Self {
        Self {
            state: Rc::default(),
            main_loop: main_loop.clone(),
        }
    }

    pub fn succeed(&self) {
        if self.state.borrow().is_none() {
            *self.state.borrow_mut() = Some(Ok(()));
        }
        self.main_loop.quit();
    }

    pub fn fail(&self, error: impl Into<String>) {
        // A removal/error in the same dispatch batch invalidates confirmation.
        *self.state.borrow_mut() = Some(Err(error.into()));
        self.main_loop.quit();
    }

    pub fn watch_core(&self, core: &pw::core::CoreRc) -> pw::core::Listener {
        let completion = self.clone();
        core.add_listener_local()
            .error(move |id, _, code, message| {
                completion.fail(format!("PipeWire object {id} error ({code}): {message}"));
            })
            .register()
    }

    /// False means a deadline elapsed, not that a write was rejected or rolled back.
    pub fn wait(&self, timeout: Duration) -> Result<bool> {
        let expired = Rc::new(std::cell::Cell::new(false));
        let timer_expired = expired.clone();
        let timer_loop = self.main_loop.clone();
        let timer = self.main_loop.loop_().add_timer(move |_| {
            timer_expired.set(true);
            timer_loop.quit();
        });
        timer
            .update_timer(Some(timeout), None)
            .into_result()
            .context("arm PipeWire operation deadline")?;
        while self.state.borrow().is_none() && !expired.get() {
            self.main_loop.run();
        }
        match self.state.borrow().as_ref() {
            Some(Ok(())) => Ok(true),
            Some(Err(error)) => anyhow::bail!(error.clone()),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waits_for_late_events_and_distinguishes_timeout_from_failure() {
        super::super::initialize();
        let main_loop = pw::main_loop::MainLoopRc::new(None).unwrap();
        let completion = Completion::new(&main_loop);
        // An earlier loop wakeup (e.g. a sync reply) is not completion.
        let wake_loop = main_loop.clone();
        let wake = main_loop.loop_().add_timer(move |_| wake_loop.quit());
        wake.update_timer(Some(Duration::from_millis(1)), None)
            .into_result()
            .unwrap();
        let delayed = completion.clone();
        let timer = main_loop.loop_().add_timer(move |_| delayed.succeed());
        timer
            .update_timer(Some(Duration::from_millis(10)), None)
            .into_result()
            .unwrap();
        assert!(completion.wait(Duration::from_secs(2)).unwrap());
        let pending = Completion::new(&main_loop);
        assert!(!pending.wait(Duration::from_millis(5)).unwrap());
        for reason in ["device removed", "core disconnected", "write rejected"] {
            let failed = Completion::new(&main_loop);
            failed.succeed();
            failed.fail(reason);
            assert!(
                failed
                    .wait(Duration::from_secs(1))
                    .unwrap_err()
                    .to_string()
                    .contains(reason)
            );
        }
    }
}
