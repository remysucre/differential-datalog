// The consumer can subscribe to the channel
// which acts as an observable of deltas.
pub trait Observable<T, E>
{
    fn subscribe(&mut self, observer: Box<dyn Observer<T, E>>) -> Box<dyn Subscription>;
}

// The channel is an observer of changes from
// a producer
pub trait Observer<T, E>
{
    fn on_start(&mut self) -> Result<(), E>;
    fn on_commit(&mut self) -> Result<(), E>;
    fn on_next(&mut self, item: T) -> Result<(), E>;
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        for upd in updates {
            self.on_next(upd)?;
        }
        Ok(())
    }
    fn on_completed(&mut self) -> Result<(), E>;
    fn on_error(&self, error: E);
}

// Stop listening to changes from the observable
pub trait Subscription {
    fn unsubscribe(self: Box<Self>);
}
