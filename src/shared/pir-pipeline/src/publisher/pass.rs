use crate::{Input, Observer};
use std::future::Future;

/// A statically typed acquire → prepare → publish recipe. Work may borrow its
/// source or carry an owner permit through every phase. No phase is raced with
/// cancellation; products keep their existing incremental stop checks inside it.
pub struct PublicationPass<I, P, A> {
    acquire: I,
    prepare: P,
    publish: A,
}
impl<I, P, A> PublicationPass<I, P, A> {
    pub fn new<W, C, T, E, PF, AF>(acquire: I, prepare: P, publish: A) -> Self
    where
        I: Future<Output = Result<Input<W>, E>>,
        P: FnOnce(W) -> PF,
        PF: Future<Output = Result<Input<C>, E>>,
        A: FnOnce(C) -> AF,
        AF: Future<Output = Result<T, E>>,
    {
        Self {
            acquire,
            prepare,
            publish,
        }
    }
    pub async fn run<W, C, T, E, PF, AF>(self, observer: &mut impl Observer) -> Result<Input<T>, E>
    where
        I: Future<Output = Result<Input<W>, E>>,
        P: FnOnce(W) -> PF,
        PF: Future<Output = Result<Input<C>, E>>,
        A: FnOnce(C) -> AF,
        AF: Future<Output = Result<T, E>>,
    {
        let input = crate::acquire(self.acquire, observer).await?;
        crate::advance(input, self.prepare, self.publish, observer).await
    }
}
