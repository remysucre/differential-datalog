use fallible_iterator::FallibleIterator;
use channel::*;
use differential_datalog::program::{RelId, Update};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use api::*;
use std::iter;
use std::iter::FromIterator;
use std::collections::HashSet;

pub struct DDlogServer
{
    prog: HDDlog,
    outlets:  Vec<Outlet>
}

impl DDlogServer
{
    pub fn stream(&mut self, tables: HashSet<RelId>) {
        let outlet = Outlet{tables : tables, observer : None};
        self.outlets.push(outlet);
    }
}

pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Option<Box<dyn Observer<Update<super::Value>, String>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    fn subscribe(&mut self,
                 observer: Box<dyn Observer<Update<super::Value>, String>>) {
        self.observer = Some(observer);
        // TODO more than one subscribers
    }
}

impl Observer<Update<super::Value>, String> for DDlogServer
{
    fn on_start(&self) {
        self.prog.transaction_start();
    }

    fn on_commit(&self) {
        // TODO handle errors
        let changes = self.prog.transaction_commit_dump_changes();
        if let Ok(changes) = changes {

            // TODO actually use deltamap
            for outlet in &self.outlets {

                if let Some(observer) = &outlet.observer {
                    let upds = changes.as_ref().iter().filter_map(|(table_id, table_data)| {
                        if outlet.tables.contains(table_id) {
                            Some((table_id, table_data))
                        } else {
                            None
                        }
                    }).flat_map(|(table_id, table_data)| {
                        table_data.iter().map(move |(val, weight)| {
                            debug_assert!(*weight == 1 || *weight == -1);
                            if *weight == 1 {
                                Update::Insert{
                                    relid: *table_id,
                                    v: val.clone()
                                }
                            } else {
                                Update::DeleteValue{
                                    relid: *table_id,
                                    v: val.clone()
                                }
                            }
                        })
                    });

                    observer.on_start();
                    observer.on_updates(Box::new(upds));
                    observer.on_commit();
                }
            }
        }
    }

    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) {
        self.prog.apply_valupdates(updates);
    }

    fn on_completed(self) {
        self.prog.stop();
    }
}
