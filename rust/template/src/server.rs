use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};

use api::*;
use channel::*;

use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::fmt;

pub struct UpdatesSubscription {
    // This points to the observer field in the outlet,
    // and sets it to `None` upon unsubscribing.
    observer: Arc<Mutex<Option<Arc<dyn Observer<Update<super::Value>, String> + Sync>>>>
}

impl Subscription for UpdatesSubscription {
    fn unsubscribe(self: Box<Self>) {
        let mut observer = self.observer.lock().unwrap();
        *observer = None;
    }
}

pub struct DDlogServer
{
    prog: HDDlog,
    outlets: Vec<Outlet>,
    redirect: HashMap<RelId, RelId>
}

impl Debug for DDlogServer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DDlogServer")
    }
}

impl DDlogServer
{
    pub fn new(prog: HDDlog, redirect: HashMap<RelId, RelId>) -> Self {
        DDlogServer{prog: prog, outlets: Vec::new(), redirect: redirect}
    }

    pub fn stream(&mut self, tables: HashSet<RelId>) -> &mut Outlet {
        let outlet = Outlet{
            tables : tables,
            observer : Arc::new(Mutex::new(None))
        };
        self.outlets.push(outlet);
        self.outlets.last_mut().unwrap()
    }

    pub fn shutdown(self) -> Response<()> {
        self.prog.stop()?;
        for outlet in &self.outlets {
            let observer = outlet.observer.clone();
            let observer = observer.lock().unwrap();
            if let Some(ref observer) = *observer {
                observer.on_completed()?;
            }
        };
        Ok(())
    }
}

pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Arc<Mutex<Option<Arc<dyn Observer<Update<super::Value>, String> + Sync>>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    fn subscribe(&mut self,
                     observer: Arc<dyn Observer<Update<super::Value>, String> + Sync>)
                     -> Box<dyn Subscription>
    {
        self.observer = Arc::new(Mutex::new(Some(observer)));
        Box::new(UpdatesSubscription{
            observer: self.observer.clone()
        })
    }
}


impl Observer<Update<super::Value>, String> for DDlogServer
{
    fn on_start(&self) -> Response<()> {
        self.prog.transaction_start()
    }

    fn on_commit(&self) -> Response<()> {
        let changes = self.prog.transaction_commit_dump_changes()?;
        for change in changes.as_ref().iter() {
            println!{"Got {:?}", change};
        }
        for outlet in &self.outlets {
            let observer = outlet.observer.clone();
            let observer = observer.lock().unwrap();
            if let Some(ref observer) = *observer {
                let upds = outlet.tables.iter().flat_map(|table| {
                    changes.as_ref().get(table).unwrap().iter().map(move |(val, weight)| {
                        debug_assert!(*weight == 1 || *weight == -1);
                        if *weight == 1 {
                            Update::Insert{relid: *table, v: val.clone()}
                        } else {
                            Update::DeleteValue{relid: *table, v: val.clone()}
                        }
                    })
                });

                observer.on_start()?;
                observer.on_updates(Box::new(upds))?;
                observer.on_commit()?;
            }
        };
        Ok(())
    }

    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        self.prog.apply_valupdates(updates.map(|upd| match upd {
            Update::Insert{relid: relid, v: v} =>
                Update::Insert{
                    relid: *self.redirect.get(&relid).unwrap(),
                    v: v},
            Update::DeleteValue{relid: relid, v: v} =>
                Update::DeleteValue{
                    relid: *self.redirect.get(&relid).unwrap(),
                    v: v},
            _otherwise => panic!("Operation not allowed"),
        }))
    }

    fn on_completed(&self) -> Response<()> {
        Ok(())
    }
}
