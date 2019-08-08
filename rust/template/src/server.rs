use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};

use api::*;
use observe::*;

use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::fmt;

pub struct UpdatesSubscription {
    // This points to the observer field in the outlet,
    // and sets it to `None` upon unsubscribing.
    observer: Arc<Mutex<Option<Box<dyn Observer<Update<super::Value>, String> + Sync>>>>
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
    outlets: Vec<Arc<Mutex<Outlet>>>,
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

    pub fn add_stream(&mut self, tables: HashSet<RelId>) -> Arc<Mutex<Outlet>> {
        //let db = self.prog.db.clone();
        //let db = db.lock().unwrap();
        //if let Some(ref db) = *db {
        //    for relid in &tables {
        //        assert!(db.as_ref().contains_key(&relid),
        //        "Attempting to listen to non-existent table")
        //    }
        //} else {
        //    assert!(tables.is_empty(),
        //            "Attempting to listen to an empty DB")
        //}
        let outlet = Arc::new(Mutex::new(Outlet{
            tables : tables,
            observer : Arc::new(Mutex::new(None))
        }));
        self.outlets.push(outlet.clone());
        outlet.clone()
    }

    pub fn remove_stream(&mut self, outlet: Arc<Mutex<Outlet>>) {
        self.outlets.retain(|o| !Arc::ptr_eq(&o, &outlet));
    }

    pub fn shutdown(self) -> Response<()> {
        self.prog.stop()?;
        for outlet in &self.outlets {
            let outlet = outlet.lock().unwrap();
            let observer = outlet.observer.clone();
            let mut observer = observer.lock().unwrap();
            if let Some(ref mut observer) = *observer {
                observer.on_completed()?;
            }
        };
        Ok(())
    }
}

pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Arc<Mutex<Option<Box<dyn Observer<Update<super::Value>, String> + Sync>>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    fn subscribe(&mut self,
                     observer: Box<dyn Observer<Update<super::Value>, String> + Sync>)
                     -> Box<dyn Subscription>
    {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = Some(observer);
        Box::new(UpdatesSubscription{
            observer: self.observer.clone()
        })
    }
}

pub struct ADDlogServer(pub Arc<Mutex<DDlogServer>>);

impl Observer<Update<super::Value>, String> for ADDlogServer {
    fn on_start(&mut self) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_start()
    }

    fn on_commit(&mut self) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_commit()
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_updates(updates)
    }

    fn on_error(&self, error: String) {
        let s = self.0.clone();
        let s = s.lock().unwrap();
        s.on_error(error)
    }

    fn on_completed(&mut self) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_completed()
    }
}

impl Observer<Update<super::Value>, String> for DDlogServer
{
    fn on_start(&mut self) -> Response<()> {
        self.prog.transaction_start()
    }

    fn on_commit(&mut self) -> Response<()> {
        let changes = self.prog.transaction_commit_dump_changes()?;
        for change in changes.as_ref().iter() {
            println!{"Got {:?}", change};
        }
        for outlet in &self.outlets {
            let outlet = outlet.clone();
            let outlet = outlet.lock().unwrap();
            let observer = outlet.observer.clone();
            let mut observer = observer.lock().unwrap();
            if let Some(ref mut observer) = *observer {
                let upds = outlet.tables.iter().flat_map(|table| {
                    changes.as_ref().get(table).map(|t| {
                        t.iter().map(move |(val, weight)| {
                            debug_assert!(*weight == 1 || *weight == -1);
                            if *weight == 1 {
                                Update::Insert{relid: *table, v: val.clone()}
                            } else {
                                Update::DeleteValue{relid: *table, v: val.clone()}
                            }
                        })
                    })
                }).flatten();

                observer.on_start()?;
                observer.on_updates(Box::new(upds))?;
                observer.on_commit()?;
            }
        };
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        self.prog.apply_valupdates(updates.map(|upd| match upd {
            Update::Insert{relid: relid, v: v} =>
                Update::Insert{
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v: v},
            Update::DeleteValue{relid: relid, v: v} =>
                Update::DeleteValue{
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v: v},
            _otherwise => panic!("Operation not allowed"),
        }))
    }

    fn on_error(&self, error: String) {
        println!("error: {:?}", error);
    }

    fn on_completed(&mut self) -> Response<()> {
        Ok(())
    }
}
