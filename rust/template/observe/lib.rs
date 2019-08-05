use std::sync::Arc;

// The consumer can subscribe to the channel
// which acts as an observable of deltas.
pub trait Observable<T, E>
where T: Send, E:Send
{
    fn subscribe(&mut self, observer: Arc<dyn Observer<T, E> + Sync>) -> Box<dyn Subscription>;
}

// The channel is an observer of changes from
// a producer
pub trait Observer<T, E>: Send
where T: Send, E:Send
{
    fn on_start(&self) -> Result<(), E>;
    fn on_commit(&self) -> Result<(), E>;
    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E>;
    fn on_completed(&self) -> Result<(), E>;
    fn on_error(&self, error: E);
}

// Stop listening to changes from the observable
pub trait Subscription {
    fn unsubscribe(self: Box<Self>);
}
