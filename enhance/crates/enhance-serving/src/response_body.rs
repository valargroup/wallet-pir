//! Keep admission charged until HTTP releases its buffered response.
use axum::body::{Body, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

struct GuardedBody<G, F> {
    bytes: Option<Bytes>,
    _guard: G,
    fence: F,
}

impl<G: Send + Unpin, F: Fn() -> Result<(), String> + Send + Unpin> HttpBody for GuardedBody<G, F> {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let Some(bytes) = self.bytes.take() else {
            return Poll::Ready(None);
        };
        Poll::Ready(Some(
            (self.fence)()
                .map(|()| Frame::data(bytes))
                .map_err(io::Error::other),
        ))
    }
    fn size_hint(&self) -> SizeHint {
        // Do not advertise end-of-stream until the transport releases the body.
        SizeHint::default()
    }
}

pub(crate) fn guarded<G, F>(bytes: Vec<u8>, guard: G, fence: F) -> Body
where
    G: Send + Unpin + 'static,
    F: Fn() -> Result<(), String> + Send + Unpin + 'static,
{
    Body::new(GuardedBody {
        bytes: Some(bytes.into()),
        _guard: guard,
        fence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::Semaphore;
    #[tokio::test]
    async fn admission_lasts_until_transport_drops_body() {
        let slots = Arc::new(Semaphore::new(1));
        let mut body = guarded(
            vec![1, 2, 3],
            slots.clone().acquire_owned().await.unwrap(),
            || Ok(()),
        );
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.into_data().unwrap().as_ref(), &[1, 2, 3]);
        assert_eq!(slots.available_permits(), 0);
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert_eq!(slots.available_permits(), 0);
        drop(body);
        assert_eq!(slots.available_permits(), 1);
    }
    #[tokio::test]
    async fn revocation_after_packing_prevents_response_release() {
        let revoked = Arc::new(AtomicBool::new(false));
        let fence = revoked.clone();
        let mut body = guarded(vec![7], (), move || {
            if fence.load(Ordering::SeqCst) {
                Err("revoked".into())
            } else {
                Ok(())
            }
        });
        revoked.store(true, Ordering::SeqCst);
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .is_err()
        );
    }
}
