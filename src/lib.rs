use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::stream::{FuturesUnordered, Stream, StreamExt};
use futures::FutureExt;
use pin_project::pin_project;
use tokio::task::{JoinError, JoinHandle};

#[pin_project]
pub struct TaskSet<T, I, U, F, Fut>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U>,
    Fut: Send + 'static,
    U: Send + 'static,
{
    tasks: FuturesUnordered<AbortOnDropHandle<U>>,
    input: I::IntoIter,
    f: F,
}

impl<T, I, U, F, Fut> TaskSet<T, I, U, F, Fut>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U>,
    Fut: Send + 'static,
    U: Send + 'static,
{
    pub async fn join_ignore(mut self) {
        while self.next().await.is_some() {}
    }

    pub async fn join_unordered(mut self) -> Vec<U> {
        let mut ret = vec![];
        while let Some(d) = self.next().await {
            ret.push(d)
        }
        ret
    }

    fn spawn_one(&mut self) {
        if let Some(t) = self.input.next() {
            let fut = (self.f)(t);
            let handle = tokio::spawn(fut);
            self.tasks.push(AbortOnDropHandle(handle));
        }
    }
}

impl<T, I, U, F, Fut, E> TaskSet<T, I, Result<U, E>, F, Fut>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<U, E>>,
    Fut: Send + 'static,
    U: Send + 'static,
    E: Send + 'static,
{
    pub async fn try_join_ignore(mut self) -> Result<(), E> {
        while let Some(d) = self.next().await {
            d?;
        }
        Ok(())
    }

    pub async fn try_join_unordered(mut self) -> Result<Vec<U>, E> {
        let mut ret = vec![];
        while let Some(d) = self.next().await {
            ret.push(d?);
        }
        Ok(ret)
    }
}

impl<T, I, U, F, Fut> Stream for TaskSet<T, I, U, F, Fut>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U>,
    Fut: Send + 'static,
    U: Send + 'static,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.tasks.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(d)) => match d {
                Ok(val) => {
                    self.spawn_one();
                    Poll::Ready(Some(val))
                }
                Err(err) => match err.try_into_panic() {
                    Ok(panic) => std::panic::resume_unwind(panic),
                    Err(_) => unreachable!(),
                },
            },
        }
    }
}

pub fn spawn_iter<T, I, U, F, Fut>(limit: usize, input: I, f: F) -> TaskSet<T, I, U, F, Fut>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U>,
    Fut: Send + 'static,
    U: Send + 'static,
{
    assert!(limit > 0);

    let tasks = FuturesUnordered::new();
    let mut input = input.into_iter();
    let mut spawned = 0;
    while let Some(t) = input.next() {
        let fut = f(t);
        let handle = tokio::spawn(fut);
        tasks.push(AbortOnDropHandle(handle));
        spawned += 1;
        if spawned >= limit {
            break;
        }
    }

    TaskSet { tasks, input, f }
}

#[derive(Debug)]
pub struct AbortOnDropHandle<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDropHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}
