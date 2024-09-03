// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::customscan::path::{plan_custom_path, reparameterize_custom_path_by_child};
use crate::customscan::CustomScan;
use pgrx::{node_to_string, pg_sys, PgList, PgMemoryContexts};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};

#[derive(Debug)]
pub struct Args {
    pub root: *mut pg_sys::PlannerInfo,
    pub rel: *mut pg_sys::RelOptInfo,
    pub rti: pg_sys::Index,
    pub rte: *mut pg_sys::RangeTblEntry,
}

impl Args {
    pub fn root(&self) -> &pg_sys::PlannerInfo {
        unsafe { self.root.as_ref().expect("Args::root should not be null") }
    }

    pub fn rel(&self) -> &pg_sys::RelOptInfo {
        unsafe { self.rel.as_ref().expect("Args::rel should not be null") }
    }

    pub fn rte(&self) -> &pg_sys::RangeTblEntry {
        unsafe { self.rte.as_ref().expect("Args::rte should not be null") }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
#[repr(u32)]
pub enum Flags {
    /// #define CUSTOMPATH_SUPPORT_BACKWARD_SCAN	0x0001
    BackwardScan = 0x0001,

    /// #define CUSTOMPATH_SUPPORT_MARK_RESTORE		0x0002
    MarkRestore = 0x0002,

    /// #define CUSTOMPATH_SUPPORT_PROJECTION		0x0004
    Projection = 0x0004,
}

pub struct CustomPathBuilder {
    args: Args,
    flags: HashSet<Flags>,

    custom_path_node: pg_sys::CustomPath,

    custom_paths: PgList<pg_sys::Path>,
    custom_private: PgList<pg_sys::Node>,
}

impl Debug for CustomPathBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomPathBuilder")
            .field("args", &self.args)
            .field("flags", &self.flags)
            .field("path", &self.flags)
            .field(
                "custom_paths",
                &self
                    .custom_paths
                    .iter_ptr()
                    .map(|path| unsafe { node_to_string(path.cast()).unwrap_or("<NULL>") })
                    .collect::<Vec<_>>(),
            )
            .field(
                "custom_private",
                &self
                    .custom_private
                    .iter_ptr()
                    .map(|node| unsafe { node_to_string(node.cast()).unwrap_or("<NULL>") })
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl CustomPathBuilder {
    pub fn new<CS: CustomScan>(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        rti: pg_sys::Index,
        rte: *mut pg_sys::RangeTblEntry,
    ) -> CustomPathBuilder {
        Self {
            args: Args {
                root,
                rel,
                rti,
                rte,
            },
            flags: Default::default(),

            custom_path_node: pg_sys::CustomPath {
                path: pg_sys::Path {
                    type_: pg_sys::NodeTag::T_CustomPath,
                    pathtype: pg_sys::NodeTag::T_CustomScan,
                    parent: rel,
                    pathtarget: unsafe { *rel }.reltarget,
                    ..Default::default()
                },
                methods: PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(
                    pg_sys::CustomPathMethods {
                        CustomName: CS::NAME.as_ptr(),
                        PlanCustomPath: Some(plan_custom_path::<CS>),
                        ReparameterizeCustomPathByChild: Some(
                            reparameterize_custom_path_by_child::<CS>,
                        ),
                    },
                ),
                ..Default::default()
            },
            custom_paths: PgList::default(),
            custom_private: PgList::default(),
        }
    }

    pub fn args(&self) -> &Args {
        &self.args
    }

    //
    // convenience getters for type safety
    //

    pub fn base_restrict_info(&self) -> PgList<pg_sys::RestrictInfo> {
        unsafe { PgList::from_pg(self.args.rel().baserestrictinfo) }
    }

    //
    // public settings
    //

    pub fn clear_flags(mut self) -> Self {
        self.flags.clear();
        self
    }

    pub fn set_flag(mut self, flag: Flags) -> Self {
        self.flags.insert(flag);
        self
    }

    pub fn add_custom_path(mut self, path: *mut pg_sys::Path) -> Self {
        self.custom_paths.push(path);
        self
    }

    pub fn add_private_data(mut self, data: *mut pg_sys::Node) -> Self {
        self.custom_private.push(data);
        self
    }

    pub fn set_rows(mut self, rows: pg_sys::Cardinality) -> Self {
        self.custom_path_node.path.rows = rows;
        self
    }

    pub fn set_startup_cost(mut self, cost: pg_sys::Cost) -> Self {
        self.custom_path_node.path.startup_cost = cost;
        self
    }

    pub fn set_total_cost(mut self, cost: pg_sys::Cost) -> Self {
        self.custom_path_node.path.total_cost = cost;
        self
    }

    pub fn build(mut self) -> pg_sys::CustomPath {
        self.custom_path_node.custom_paths = self.custom_paths.into_pg();
        self.custom_path_node.custom_private = self.custom_private.into_pg();
        self.custom_path_node.flags = self
            .flags
            .into_iter()
            .fold(0, |acc, flag| acc | flag as u32);

        self.custom_path_node
    }
}