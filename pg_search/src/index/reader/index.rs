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

use crate::schema::SearchField;
use anyhow::Result;
use pgrx::{pg_sys, PgRelation};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::iter::Flatten;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::collector::{Collector, TopDocs};
use tantivy::index::Index;
use tantivy::query::QueryParser;
use tantivy::schema::FieldType;
use tantivy::{
    query::Query, Ctid, DocAddress, DocId, IndexReader, Order, ReloadPolicy, Score, Searcher,
    SegmentOrdinal, SegmentReader, TantivyDocument, TantivyError,
};
use tantivy::{snippet::SnippetGenerator, Executor};

use crate::index::{setup_tokenizers, BlockDirectoryType};
use crate::postgres::storage::block::CLEANUP_LOCK;
use crate::postgres::storage::buffer::{Buffer, BufferManager};
use crate::postgres::utils::ctid_to_u64;
use crate::query::SearchQueryInput;
use crate::schema::{SearchFieldName, SearchIndexSchema};

/// Represents a matching document from a tantivy search.  Typically it is returned as an Iterator
/// Item alongside the originating tantivy [`DocAddress`]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchIndexScore {
    pub ctid: u64,
    pub bm25: f32,
}

impl SearchIndexScore {
    #[inline]
    pub fn new(ctid: Ctid, score: Score) -> Self {
        Self {
            ctid: ctid_to_u64(ctid),
            bm25: score,
        }
    }
}

impl PartialOrd for SearchIndexScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.bm25.partial_cmp(&other.bm25)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// An iterator of the different styles of search results we can return
#[allow(clippy::large_enum_variant)]
#[derive(Default)]
pub enum SearchResults {
    #[default]
    None,

    TopNByScore(usize, std::vec::IntoIter<(OrderedScore, DocAddress, Ctid)>),

    TopNByField(usize, std::vec::IntoIter<(SearchIndexScore, DocAddress)>),

    #[allow(clippy::type_complexity)]
    BufferedChannel(
        std::iter::Flatten<crossbeam::channel::IntoIter<Vec<(SearchIndexScore, DocAddress)>>>,
    ),

    Channel(crossbeam::channel::IntoIter<(SearchIndexScore, DocAddress)>),

    SingleSegment(vec_collector::FruitStyle),
    AllSegments(Flatten<std::vec::IntoIter<vec_collector::FruitStyle>>),
}

#[derive(PartialEq, Clone)]
pub struct OrderedScore {
    dir: SortDirection,
    score: SearchIndexScore,
}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self.score.partial_cmp(&other.score);
        match self.dir {
            SortDirection::Desc => cmp,
            SortDirection::Asc => cmp.map(|o| o.reverse()),
        }
    }
}

impl Debug for SearchResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchResults::None => write!(f, "SearchResults::None"),
            SearchResults::TopNByScore(count, iter) => {
                write!(f, "SearchResults::TopNByScore({count}, {:?})", iter.len())
            }
            SearchResults::TopNByField(count, iter) => {
                write!(f, "SearchResults::TopNByField({count}, {:?})", iter.len())
            }
            SearchResults::BufferedChannel(iter) => {
                write!(f, "SearchResults::BufferedChannel(~{:?})", iter.size_hint())
            }
            SearchResults::Channel(iter) => {
                write!(f, "SearchResults::Channel(~{:?})", iter.size_hint())
            }
            SearchResults::SingleSegment(iter) => {
                write!(f, "SearchResults::SingleSegment({:?})", iter.size_hint())
            }
            SearchResults::AllSegments(iter) => {
                write!(f, "SearchResults::AllSegments({:?})", iter.size_hint())
            }
        }
    }
}

impl Iterator for SearchResults {
    type Item = (SearchIndexScore, DocAddress);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SearchResults::None => None,
            SearchResults::TopNByScore(_, iter) => iter
                .next()
                .map(|(OrderedScore { score, .. }, doc_address, _ctid)| (score, doc_address)),
            SearchResults::TopNByField(_, iter) => iter.next(),
            SearchResults::BufferedChannel(iter) => iter.next(),
            SearchResults::Channel(iter) => iter.next(),
            SearchResults::SingleSegment(iter) => iter.next(),
            SearchResults::AllSegments(iter) => iter.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SearchResults::None => (0, Some(0)),
            SearchResults::TopNByScore(_, iter) => iter.size_hint(),
            SearchResults::TopNByField(_, iter) => iter.size_hint(),
            SearchResults::BufferedChannel(iter) => iter.size_hint(),
            SearchResults::Channel(iter) => iter.size_hint(),
            SearchResults::SingleSegment(iter) => iter.size_hint(),
            SearchResults::AllSegments(iter) => iter.size_hint(),
        }
    }
}

impl SearchResults {
    pub fn len(&self) -> Option<usize> {
        match self {
            SearchResults::None => Some(0),
            SearchResults::TopNByScore(count, _) => Some(*count),
            SearchResults::TopNByField(count, _) => Some(*count),
            SearchResults::BufferedChannel(_) => None,
            SearchResults::Channel(_) => None,
            SearchResults::SingleSegment(_) => None,
            SearchResults::AllSegments(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct SearchIndexReader {
    index_oid: pg_sys::Oid,
    searcher: Searcher,
    schema: SearchIndexSchema,
    underlying_reader: IndexReader,
    underlying_index: Index,

    // [`Buffer`] has a Drop impl, so we hold onto the lock, but don't otherwise use it
    //
    // also, it's an Arc b/c if we're clone'd (we do derive it, after all), we only want this
    // buffer dropped once
    _cleanup_lock: Arc<Option<Buffer>>,
}

impl SearchIndexReader {
    pub fn open(
        index_relation: &PgRelation,
        directory_type: BlockDirectoryType,
        needs_cleanup_lock: bool,
    ) -> Result<Self> {
        // It is possible for index only scans and custom scans, which only check the visibility map
        // and do not fetch tuples from the heap, to suffer from the concurrent TID recycling problem.
        // This problem occurs due to a race condition: after vacuum is called, a concurrent index only or custom scan
        // reads in some dead ctids. ambulkdelete finishes immediately after, and Postgres updates its visibility map,
        //rendering those dead ctids visible. The concurrent scan then returns the wrong results.
        // To prevent this, ambulkdelete acquires an exclusive cleanup lock. Readers must also acquire this lock (shared)
        // to prevent a reader from reading dead ctids right before ambulkdelete finishes.
        let cleanup_lock = if needs_cleanup_lock {
            let bman = BufferManager::new(index_relation.oid(), false);
            Some(bman.get_buffer(CLEANUP_LOCK))
        } else {
            None
        };

        let directory = directory_type.directory(index_relation, false);
        let mut index = Index::open(directory)?;
        let schema = SearchIndexSchema::open(index.schema(), index_relation);

        setup_tokenizers(&mut index, index_relation);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();

        Ok(Self {
            index_oid: index_relation.oid(),
            searcher,
            schema,
            underlying_reader: reader,
            underlying_index: index,
            _cleanup_lock: Arc::new(cleanup_lock),
        })
    }

    pub fn key_field(&self) -> SearchField {
        self.schema.key_field()
    }

    pub fn query(&self, search_query_input: &SearchQueryInput) -> Box<dyn Query> {
        let mut parser = QueryParser::for_index(
            &self.underlying_index,
            self.schema
                .fields
                .iter()
                .map(|search_field| search_field.id.0)
                .collect::<Vec<_>>(),
        );
        search_query_input
            .clone()
            .into_tantivy_query(
                &(
                    unsafe { &PgRelation::with_lock(self.index_oid, pg_sys::AccessShareLock as _) },
                    &self.schema,
                ),
                &mut parser,
                &self.searcher,
            )
            .expect("must be able to parse query")
    }

    pub fn get_doc(&self, doc_address: DocAddress) -> tantivy::Result<TantivyDocument> {
        self.searcher.doc(doc_address)
    }

    /// Returns the index size, in bytes, according to tantivy
    pub fn byte_size(&self) -> Result<u64> {
        Ok(self
            .underlying_reader
            .searcher()
            .space_usage()
            .map(|space| space.total().get_bytes())?)
    }

    pub fn segment_readers(&self) -> &[SegmentReader] {
        self.searcher.segment_readers()
    }

    pub fn schema(&self) -> &SearchIndexSchema {
        &self.schema
    }

    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    pub fn validate_checksum(&self) -> Result<HashSet<PathBuf>> {
        Ok(self.underlying_index.validate_checksum()?)
    }

    pub fn snippet_generator(
        &self,
        field_name: &str,
        query: &SearchQueryInput,
    ) -> (tantivy::schema::Field, SnippetGenerator) {
        let field = self
            .schema
            .get_search_field(&SearchFieldName(field_name.into()))
            .expect("cannot generate snippet, field does not exist");

        match self.schema.schema.get_field_entry(field.into()).field_type() {
            FieldType::Str(_) => {
                let field:tantivy::schema::Field = field.into();
                let generator = SnippetGenerator::create(&self.searcher, &self.query(query), field)
                    .unwrap_or_else(|err| panic!("failed to create snippet generator for field: {field_name}... {err}"));
                (field, generator)
            }
            _ => panic!("failed to create snippet generator for field: {field_name}... can only highlight text fields")
        }
    }

    /// Search the Tantivy index for matching documents.
    ///
    /// The order of returned docs is unspecified.
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search(
        &self,
        need_scores: bool,
        _sort_segments_by_ctid: bool,
        query: &SearchQueryInput,
        _estimated_rows: Option<usize>,
    ) -> SearchResults {
        let collector = vec_collector::VecCollector::new(need_scores);
        let results = self.collect(query, collector, need_scores);
        SearchResults::AllSegments(results.into_iter().flatten())
    }

    /// Search a specific index segment for matching documents.
    ///
    /// The order of returned docs is unspecified.
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_segment(
        &self,
        need_scores: bool,
        segment_ord: SegmentOrdinal,
        query: &SearchQueryInput,
    ) -> SearchResults {
        let collector = vec_collector::VecCollector::new(need_scores);
        let weight = self
            .query(query)
            .weight(if need_scores {
                tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                }
            } else {
                tantivy::query::EnableScoring::Disabled {
                    schema: &self.schema.schema,
                    searcher_opt: Some(&self.searcher),
                }
            })
            .expect("weight should be constructable");
        let segment_reader = self.searcher.segment_reader(segment_ord);
        let results = collector
            .collect_segment(weight.as_ref(), segment_ord, segment_reader)
            .expect("single segment collection should succeed");
        SearchResults::SingleSegment(results.into_iter())
    }

    /// Search the Tantivy index for the "top N" matching documents.
    ///
    /// The documents are returned in score order.  Most relevant first if `sortdir` is [`SortDirection::Desc`],
    /// or least relevant first if it's [`SortDirection::Asc`].
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_top_n(
        &self,
        query: &SearchQueryInput,
        sort_field: Option<String>,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        if let Some(sort_field) = sort_field {
            self.top_by_field(query, sort_field, sortdir, n)
        } else {
            self.top_by_score(query, sortdir, n)
        }
    }

    #[allow(non_local_definitions)]
    fn top_by_field(
        &self,
        query: &SearchQueryInput,
        sort_field: String,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        impl From<SortDirection> for tantivy::Order {
            fn from(value: SortDirection) -> Self {
                match value {
                    SortDirection::Asc => Order::Asc,
                    SortDirection::Desc => Order::Desc,
                }
            }
        }

        let sort_field = self
            .schema
            .get_search_field(&SearchFieldName(sort_field.clone()))
            .expect("sort field should exist in index schema");

        let collector =
            TopDocs::with_limit(n).order_by_u64_field(sort_field.name.0.clone(), sortdir.into());

        let top_docs = self.collect(query, collector, true);
        let top_docs = top_docs
            .into_iter()
            .map(|(_, doc_address, ctid)| {
                (
                    SearchIndexScore {
                        ctid: ((ctid.0 as u64) << 16) | (ctid.1 as u64),
                        bm25: 1.0,
                    },
                    doc_address,
                )
            })
            .collect::<Vec<_>>();

        SearchResults::TopNByField(top_docs.len(), top_docs.into_iter())
    }

    fn top_by_score(
        &self,
        query: &SearchQueryInput,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        let collector =
            TopDocs::with_limit(n).tweak_score(move |_segment_reader: &tantivy::SegmentReader| {
                move |_doc: DocId, original_score: Score, ctid: Ctid| OrderedScore {
                    dir: sortdir,
                    score: SearchIndexScore {
                        ctid: ((ctid.0 as u64) << 16) | (ctid.1 as u64),
                        bm25: original_score,
                    },
                }
            });

        let top_docs = self.collect(query, collector, true);
        SearchResults::TopNByScore(top_docs.len(), top_docs.into_iter())
    }

    pub fn estimate_docs(&self, search_query_input: &SearchQueryInput) -> Option<usize> {
        let readers = self.searcher.segment_readers();
        let (ordinal, largest_reader) = readers
            .iter()
            .enumerate()
            .max_by_key(|(_, reader)| reader.num_docs())?;

        let collector = tantivy::collector::Count;
        let schema = self.schema.schema.clone();
        let query = self.query(search_query_input);
        let weight = match query.weight(tantivy::query::EnableScoring::Disabled {
            schema: &schema,
            searcher_opt: Some(&self.searcher),
        }) {
            // created the Weight, no problem
            Ok(weight) => weight,

            // got an error trying to create the weight.  This *likely* means
            // the query requires scoring, so try again with scoring enabled.
            // I've seen this with the `MoreLikeThis` query type.
            //
            // NB:  we could just return `None` here and let the caller deal with it?
            //      a deciding factor might be if users complain that query planning
            //      is too slow when they use constructs like `MoreLikeThis`
            Err(TantivyError::InvalidArgument(_)) => query
                .weight(tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                })
                .expect("creating a Weight from a Query should not fail"),

            // something completely unexpected happen, so just panic
            Err(e) => panic!("{:?}", e),
        };

        let count = collector
            .collect_segment(weight.as_ref(), ordinal as SegmentOrdinal, largest_reader)
            .expect("counting docs in the largest segment should not fail")
            .max(1); // want to assume at least 1 matching document
        let segment_doc_proportion =
            largest_reader.num_docs() as f64 / self.searcher.num_docs() as f64;

        Some((count as f64 / segment_doc_proportion).ceil() as usize)
    }

    pub fn collect<C: Collector + 'static>(
        &self,
        query: &SearchQueryInput,
        collector: C,
        need_scores: bool,
    ) -> C::Fruit {
        let owned_query = self.query(query);
        self.searcher
            .search_with_executor(
                &owned_query,
                &collector,
                &Executor::SingleThread,
                if need_scores {
                    tantivy::query::EnableScoring::Enabled {
                        searcher: &self.searcher,
                        statistics_provider: &self.searcher,
                    }
                } else {
                    tantivy::query::EnableScoring::Disabled {
                        schema: &self.schema.schema,
                        searcher_opt: Some(&self.searcher),
                    }
                },
            )
            .expect("search should not fail")
    }
}

mod vec_collector {
    use crate::index::reader::index::SearchIndexScore;
    use tantivy::collector::{Collector, SegmentCollector};
    use tantivy::{Ctid, DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

    #[derive(Default)]
    pub enum FruitStyle {
        #[default]
        Empty,
        Scored(
            SegmentOrdinal,
            std::vec::IntoIter<DocId>,
            std::vec::IntoIter<Score>,
            std::vec::IntoIter<Ctid>,
        ),
        Blocks(
            SegmentOrdinal,
            std::vec::IntoIter<std::vec::IntoIter<DocId>>,
            std::vec::IntoIter<std::vec::IntoIter<Ctid>>,
            std::vec::IntoIter<DocId>,
            std::vec::IntoIter<Ctid>, // ctids
        ),
    }

    impl Iterator for FruitStyle {
        type Item = (SearchIndexScore, DocAddress);

        fn next(&mut self) -> Option<Self::Item> {
            match self {
                FruitStyle::Empty => None,
                FruitStyle::Scored(segment_ord, doc, score, ctid) => {
                    let doc = doc.next()?;
                    let ctid = ctid.next()?;
                    let ctid = ((ctid.0 as u64) << 16) | (ctid.1 as u64);
                    assert!(ctid != 0);
                    let bm25 = score.next()?;
                    let doc_address = DocAddress::new(*segment_ord, doc);
                    let scored = SearchIndexScore { ctid, bm25 };
                    Some((scored, doc_address))
                }
                FruitStyle::Blocks(segment_ord, doc_blocks, ctid_blocks, doc, ctid) => loop {
                    match doc.next() {
                        None => {
                            *doc = doc_blocks.next()?;
                            *ctid = ctid_blocks.next()?;
                            continue;
                        }

                        Some(doc) => {
                            let ctid = ctid.next()?;
                            let ctid = ((ctid.0 as u64) << 16) | (ctid.1 as u64);
                            assert!(ctid != 0);
                            let doc_address = DocAddress::new(*segment_ord, doc);
                            let scored = SearchIndexScore { ctid, bm25: 0.0 };
                            return Some((scored, doc_address));
                        }
                    }
                },
            }
        }
    }

    /// A [`Collector`] that collects all matching documents into a [`Vec`].
    pub struct VecCollector {
        need_scores: bool,
    }

    impl VecCollector {
        pub fn new(need_scores: bool) -> Self {
            Self { need_scores }
        }
    }

    impl Collector for VecCollector {
        type Fruit = Vec<FruitStyle>;
        type Child = VecSegmentCollector;

        fn for_segment(
            &self,
            segment_local_id: SegmentOrdinal,
            _segment_reader: &SegmentReader,
        ) -> tantivy::Result<Self::Child> {
            Ok(VecSegmentCollector {
                segment_ord: segment_local_id,
                scored: (vec![], vec![], vec![]),
                doc_blocks: vec![],
                ctid_blocks: vec![],
            })
        }

        fn requires_scoring(&self) -> bool {
            self.need_scores
        }

        fn merge_fruits(
            &self,
            segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
        ) -> tantivy::Result<Self::Fruit> {
            // NB:  we never call this function, but best to implement it anyways
            Ok(segment_fruits)
        }
    }

    pub struct VecSegmentCollector {
        segment_ord: SegmentOrdinal,
        scored: (Vec<DocId>, Vec<Score>, Vec<Ctid>),
        doc_blocks: Vec<std::vec::IntoIter<DocId>>,
        ctid_blocks: Vec<std::vec::IntoIter<Ctid>>,
    }

    impl SegmentCollector for VecSegmentCollector {
        type Fruit = FruitStyle;

        fn collect(&mut self, doc: DocId, score: Score, ctid: Ctid) {
            self.scored.0.push(doc);
            self.scored.1.push(score);
            self.scored.2.push(ctid);
        }

        #[allow(clippy::unnecessary_to_owned)]
        fn collect_block(&mut self, docs: &[DocId], ctids: &[Ctid]) {
            self.doc_blocks.push(docs.to_vec().into_iter());
            self.ctid_blocks.push(ctids.to_vec().into_iter()); // ctids.to_vec().into_iter());
        }

        fn harvest(mut self) -> Self::Fruit {
            if !self.doc_blocks.is_empty() {
                if !self.scored.0.is_empty() {
                    self.doc_blocks.push(self.scored.0.into_iter());
                    self.ctid_blocks.push(self.scored.2.into_iter());
                }
                FruitStyle::Blocks(
                    self.segment_ord,
                    self.doc_blocks.into_iter(),
                    self.ctid_blocks.into_iter(),
                    vec![].into_iter(),
                    vec![].into_iter(),
                )
            } else {
                FruitStyle::Scored(
                    self.segment_ord,
                    self.scored.0.into_iter(),
                    self.scored.1.into_iter(),
                    self.scored.2.into_iter(),
                )
            }
        }
    }
}
