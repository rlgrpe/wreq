use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};

use http::Response;
use pin_project_lite::pin_project;
use wreq_proto::rt::{Sleep, Timer as _};

use super::{RedirectPause, body::TimeoutBody};
use crate::{
    error::{BoxError, Error, TimedOut},
    rt::Timer,
};

/// Tracks only callback wait; consumed transport time survives every redirect.
pub(super) struct RedirectTimeout {
    pause: RedirectPause,
    paused_at: Option<Instant>,
    total_deadline: Option<Instant>,
    read_deadline: Option<Instant>,
}

impl RedirectTimeout {
    pub(super) fn new(
        pause: RedirectPause,
        now: Instant,
        total_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
    ) -> Self {
        Self {
            pause,
            paused_at: None,
            total_deadline: total_timeout.map(|timeout| now + timeout),
            read_deadline: read_timeout.map(|timeout| now + timeout),
        }
    }

    fn update(
        &mut self,
        timer: &Timer,
        total: &mut Option<Pin<Box<dyn Sleep>>>,
        read: &mut Option<Pin<Box<dyn Sleep>>>,
    ) -> bool {
        let paused = self.pause.is_paused();
        if paused {
            self.paused_at.get_or_insert_with(|| timer.now());
        } else if let Some(started) = self.paused_at.take() {
            let elapsed = timer.now().duration_since(started);
            for (deadline, sleep) in [
                (&mut self.total_deadline, total),
                (&mut self.read_deadline, read),
            ] {
                if let (Some(deadline), Some(sleep)) = (deadline, sleep) {
                    *deadline += elapsed;
                    timer.reset(sleep, *deadline);
                }
            }
        }
        paused
    }
}

pin_project! {
    /// Waits for response headers, then moves the total timeout into the response body.
    pub struct ResponseFuture<Fut> {
        #[pin]
        pub(super) fut: Fut,
        pub(super) timer: Timer,
        pub(super) read_timeout: Option<Duration>,
        pub(super) read_timeout_fut: Option<Pin<Box<dyn Sleep>>>,
        pub(super) total_timeout_fut: Option<Pin<Box<dyn Sleep>>>,
        pub(super) redirect_timeout: Option<RedirectTimeout>,
    }
}

impl<Fut, ResBody, E> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Response<ResBody>, E>>,
    E: Into<BoxError>,
{
    type Output = Result<Response<TimeoutBody<ResBody>>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let paused = this.redirect_timeout.as_mut().is_some_and(|timeout| {
            timeout.update(this.timer, this.total_timeout_fut, this.read_timeout_fut)
        });

        // The total timer covers response headers and body. Poll it first so an
        // expired timeout wins, then move it into `TimeoutBody` below.
        if !paused
            && let Some(timeout) = this.total_timeout_fut.as_mut()
            && timeout.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(Error::request(TimedOut).into()));
        }

        // Before headers arrive, the read timer limits that wait. The body starts
        // and resets its own read timer after each successful frame.
        if !paused
            && let Some(timeout) = this.read_timeout_fut.as_mut()
            && timeout.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(Error::request(TimedOut).into()));
        }

        // Poll the request after both timers so every pending future registers the
        // current waker before `ready!` returns.
        let response = this.fut.poll(cx);
        if let Some(timeout) = this.redirect_timeout {
            let still_paused =
                timeout.update(this.timer, this.total_timeout_fut, this.read_timeout_fut);
            if paused && !still_paused {
                // Callback completion yields before the destination service is
                // polled. Rearm adjusted timers before that next poll.
                if this
                    .total_timeout_fut
                    .as_mut()
                    .is_some_and(|timeout| timeout.as_mut().poll(cx).is_ready())
                    || this
                        .read_timeout_fut
                        .as_mut()
                        .is_some_and(|timeout| timeout.as_mut().poll(cx).is_ready())
                {
                    return Poll::Ready(Err(Error::request(TimedOut).into()));
                }
            }
        }
        let response = ready!(response).map_err(Into::into)?;

        // Moving the running total timer preserves the original deadline.
        Poll::Ready(Ok(response.map(|body| {
            TimeoutBody::new(
                body,
                this.timer.clone(),
                *this.read_timeout,
                this.total_timeout_fut.take(),
            )
        })))
    }
}
