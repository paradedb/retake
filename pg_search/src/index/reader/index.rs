// Copyright (c) 2023-2025 Retake, Inc.
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

use crate::index::fast_fields_helper::FFType;
use crate::index::merge_policy::AllowedMergePolicy;
use crate::index::reader::index::scorer_iter::DeferredScorer;
use crate::index::{setup_tokenizers, BlockDirectoryType};
use crate::postgres::storage::block::CLEANUP_LOCK;
use crate::postgres::storage::buffer::{BufferManager, PinnedBuffer};
use crate::query::SearchQueryInput;
use crate::schema::SearchField;
use crate::schema::{SearchFieldName, SearchIndexSchema};
use anyhow::Result;
use pgrx::{pg_sys, PgRelation};
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::collector::{Collector, TopDocs};
use tantivy::index::{Index, SegmentId};
use tantivy::query::{BooleanQuery, EnableScoring, QueryParser, TermQuery};
use tantivy::schema::FieldType;
use tantivy::termdict::TermOrdinal;
use tantivy::Term;
use tantivy::{
    query::Query, DocAddress, DocId, DocSet, IndexReader, Order, ReloadPolicy, Score, Searcher,
    SegmentOrdinal, SegmentReader, TantivyDocument,
};
use tantivy::{snippet::SnippetGenerator, Executor};

/// Represents a matching document from a tantivy search.  Typically, it is returned as an Iterator
/// Item alongside the originating tantivy [`DocAddress`]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchIndexScore {
    pub ctid: u64,
    pub bm25: f32,
}

impl SearchIndexScore {
    #[inline]
    pub fn new(ctid: u64, score: Score) -> Self {
        Self { ctid, bm25: score }
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
    None,
}

impl From<SortDirection> for Order {
    fn from(value: SortDirection) -> Self {
        match value {
            SortDirection::Asc => Order::Asc,
            SortDirection::Desc => Order::Desc,
            SortDirection::None => Order::Asc,
        }
    }
}

pub type FastFieldCache = FxHashMap<SegmentOrdinal, FFType>;
/// An iterator of the different styles of search results we can return
#[allow(clippy::large_enum_variant)]
#[derive(Default)]
pub enum SearchResults {
    #[default]
    None,
    TopNByScore(
        Searcher,
        FastFieldCache,
        std::vec::IntoIter<(Score, DocAddress)>,
    ),
    TopNByTweakedScore(
        Searcher,
        FastFieldCache,
        std::vec::IntoIter<(TweakedScore, DocAddress)>,
    ),
    TopNByField(
        Searcher,
        FastFieldCache,
        std::vec::IntoIter<(TermOrdinal, DocAddress)>,
    ),
    SingleSegment(
        Searcher,
        SegmentOrdinal,
        Option<FFType>,
        scorer_iter::ScorerIter,
    ),
    AllSegments(Searcher, Option<FFType>, Vec<scorer_iter::ScorerIter>),
}

#[derive(PartialEq, Clone)]
pub struct TweakedScore {
    dir: SortDirection,
    score: Score,
}

impl PartialOrd for TweakedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self.score.partial_cmp(&other.score);
        match self.dir {
            SortDirection::Desc => cmp,
            SortDirection::Asc => cmp.map(|o| o.reverse()),
            SortDirection::None => Some(Ordering::Equal),
        }
    }
}

impl Iterator for SearchResults {
    type Item = (SearchIndexScore, DocAddress);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let (searcher, ff_lookup, (score, doc_address)) = match self {
            SearchResults::None => return None,
            SearchResults::TopNByScore(searcher, ff_lookup, iter) => {
                (searcher, ff_lookup, iter.next()?)
            }
            SearchResults::TopNByTweakedScore(searcher, ff_lookup, iter) => {
                let (score, doc_id) = iter.next()?;
                (searcher, ff_lookup, (score.score, doc_id))
            }
            SearchResults::TopNByField(searcher, ff_lookup, iter) => {
                let (_, doc_id) = iter.next()?;
                (searcher, ff_lookup, (1.0, doc_id))
            }
            SearchResults::SingleSegment(searcher, segment_ord, fftype, iter) => {
                let (score, doc_address) = iter.next()?;
                let ctid_ff = fftype.get_or_insert_with(|| {
                    FFType::new_ctid(searcher.segment_reader(*segment_ord).fast_fields())
                });
                let scored = SearchIndexScore {
                    ctid: ctid_ff
                        .as_u64(doc_address.doc_id)
                        .expect("ctid should be present"),
                    bm25: score,
                };
                return Some((scored, doc_address));
            }
            SearchResults::AllSegments(searcher, fftype, iters) => loop {
                let last = iters.last_mut()?;
                match last.next() {
                    Some((score, doc_address)) => {
                        let ctid_ff = fftype.get_or_insert_with(|| {
                            FFType::new_ctid(
                                searcher
                                    .segment_reader(doc_address.segment_ord)
                                    .fast_fields(),
                            )
                        });
                        let scored = SearchIndexScore {
                            ctid: ctid_ff
                                .as_u64(doc_address.doc_id)
                                .expect("ctid should be present"),
                            bm25: score,
                        };

                        return Some((scored, doc_address));
                    }
                    None => {
                        // last iterator is empty, so pop it off, clear the fast field type cache,
                        // and loop back around to get the next one
                        iters.pop();
                        *fftype = None;
                        continue;
                    }
                }
            },
        };

        let ctid_ff = ff_lookup.entry(doc_address.segment_ord).or_insert_with(|| {
            FFType::new_ctid(
                searcher
                    .segment_reader(doc_address.segment_ord)
                    .fast_fields(),
            )
        });
        let scored = SearchIndexScore {
            ctid: ctid_ff
                .as_u64(doc_address.doc_id)
                .expect("ctid should be present"),
            bm25: score,
        };
        Some((scored, doc_address))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SearchResults::None => (0, Some(0)),
            SearchResults::TopNByScore(_, _, iter) => iter.size_hint(),
            SearchResults::TopNByTweakedScore(_, _, iter) => iter.size_hint(),
            SearchResults::TopNByField(_, _, iter) => iter.size_hint(),
            SearchResults::SingleSegment(_, _, _, iter) => iter.size_hint(),
            SearchResults::AllSegments(_, _, iters) => {
                let hint = iters
                    .first()
                    .map(|iter| iter.size_hint())
                    .unwrap_or((0, Some(0)));
                (hint.0, hint.1.map(|n| n * iters.len()))
            }
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        match self {
            SearchResults::None => 0,
            SearchResults::TopNByScore(_, _, iter) => iter.count(),
            SearchResults::TopNByTweakedScore(_, _, iter) => iter.count(),
            SearchResults::TopNByField(_, _, iter) => iter.count(),
            SearchResults::SingleSegment(_, _, _, iter) => iter.count(),
            SearchResults::AllSegments(_, _, iters) => {
                iters.into_iter().map(|iter| iter.count()).sum()
            }
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

    // [`PinnedBuffer`] has a Drop impl, so we hold onto it but don't otherwise use it
    //
    // also, it's an Arc b/c if we're clone'd (we do derive it, after all), we only want this
    // buffer dropped once
    _cleanup_lock: Arc<Option<PinnedBuffer>>,
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
        //
        // It's sufficient, and **required** for parallel scans to operate correctly, for us to hold onto
        // a pinned but unlocked buffer.
        let cleanup_lock = if needs_cleanup_lock {
            let bman = BufferManager::new(index_relation.oid());
            Some(bman.pinned_buffer(CLEANUP_LOCK))
        } else {
            None
        };

        let directory = directory_type.directory(index_relation, AllowedMergePolicy::None);
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

    pub fn segment_ids(&self) -> HashSet<SegmentId> {
        self.searcher
            .segment_readers()
            .iter()
            .map(|r| r.segment_id())
            .collect()
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

        // 1) Build the "base query" from the user's input
        let inner_query = search_query_input
            .clone() // because `into_tantivy_query` currently takes ownership
            .into_tantivy_query(
                &(
                    unsafe { &PgRelation::with_lock(self.index_oid, pg_sys::AccessShareLock as _) },
                    &self.schema,
                ),
                &mut parser,
                &self.searcher,
            )
            .expect("must be able to parse query");

        if self.schema.has_nested() {
            println!("WE GOT NESTED");
            let is_parent_field = self
                .schema
                .schema
                .get_field("_is_parent")
                .expect("schema with nested fields must have internal 'is_parent' field");
            let is_parent_term = Term::from_field_bool(is_parent_field, true);
            let is_parent_query =
                TermQuery::new(is_parent_term, tantivy::schema::IndexRecordOption::Basic);
            Box::new(BooleanQuery::intersection(vec![
                Box::new(is_parent_query),
                inner_query.box_clone(),
            ]))
        } else {
            println!("NO GOT NESTED");
            inner_query
        }
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
        let query = self.query(query);
        let iters = self
            .searcher()
            .segment_readers()
            .iter()
            .enumerate()
            .map(move |(segment_ord, segment_reader)| {
                scorer_iter::ScorerIter::new(
                    DeferredScorer::new(
                        query.box_clone(),
                        need_scores,
                        segment_reader.clone(),
                        self.searcher.clone(),
                    ),
                    segment_ord as SegmentOrdinal,
                    segment_reader.clone(),
                )
            })
            .collect::<Vec<_>>();

        SearchResults::AllSegments(self.searcher.clone(), Default::default(), iters)
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
        segment_id: SegmentId,
        query: &SearchQueryInput,
    ) -> SearchResults {
        let query = self.query(query);
        let (segment_ord, segment_reader) = self
            .searcher
            .segment_readers()
            .iter()
            .enumerate()
            .find(|(_, reader)| reader.segment_id() == segment_id)
            .expect("segment {segment_id} should exist");
        let iter = scorer_iter::ScorerIter::new(
            DeferredScorer::new(
                query,
                need_scores,
                segment_reader.clone(),
                self.searcher.clone(),
            ),
            segment_ord as SegmentOrdinal,
            segment_reader.clone(),
        );
        SearchResults::SingleSegment(
            self.searcher.clone(),
            segment_ord as SegmentOrdinal,
            None,
            iter,
        )
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
        need_scores: bool,
    ) -> SearchResults {
        if let Some(sort_field) = sort_field {
            self.top_by_field(query, sort_field, sortdir, n)
        } else {
            self.top_by_score(query, sortdir, n, need_scores)
        }
    }

    /// Search the Tantivy index for the "top N" matching documents in a specific segment.
    ///
    /// The documents are returned in score order.  Most relevant first if `sortdir` is [`SortDirection::Desc`],
    /// or least relevant first if it's [`SortDirection::Asc`].
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_top_n_in_segment(
        &self,
        segment_id: SegmentId,
        query: &SearchQueryInput,
        sort_field: Option<String>,
        sortdir: SortDirection,
        n: usize,
        need_scores: bool,
    ) -> SearchResults {
        if let Some(sort_field) = sort_field {
            assert!(
                !need_scores,
                "cannot sort by field and get scores in the same query"
            );
            self.top_by_field_in_segment(segment_id, query, sort_field, sortdir, n)
        } else {
            self.top_by_score_in_segment(segment_id, query, sortdir, n, need_scores)
        }
    }

    fn top_by_field(
        &self,
        query: &SearchQueryInput,
        sort_field: String,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        let sort_field = self
            .schema
            .get_search_field(&SearchFieldName(sort_field.clone()))
            .expect("sort field should exist in index schema");

        let collector =
            TopDocs::with_limit(n).order_by_u64_field(sort_field.name.0.clone(), sortdir.into());
        let top_docs = self.collect(query, collector, true);
        SearchResults::TopNByField(
            self.searcher.clone(),
            Default::default(),
            top_docs.into_iter(),
        )
    }

    fn top_by_score(
        &self,
        query: &SearchQueryInput,
        sortdir: SortDirection,
        n: usize,
        need_scores: bool,
    ) -> SearchResults {
        match sortdir {
            // requires tweaking the score, which is a bit slower
            SortDirection::Asc => {
                let collector = TopDocs::with_limit(n).tweak_score(
                    move |_segment_reader: &tantivy::SegmentReader| {
                        move |_doc: DocId, original_score: Score| TweakedScore {
                            dir: sortdir,
                            score: original_score,
                        }
                    },
                );
                let top_docs = self.collect(query, collector, true);
                SearchResults::TopNByTweakedScore(
                    self.searcher.clone(),
                    Default::default(),
                    top_docs.into_iter(),
                )
            }

            // can use tantivy's score directly
            SortDirection::Desc => {
                let collector = TopDocs::with_limit(n);
                let top_docs = self.collect(query, collector, true);
                SearchResults::TopNByScore(
                    self.searcher.clone(),
                    Default::default(),
                    top_docs.into_iter(),
                )
            }

            SortDirection::None => self.search(need_scores, false, query, Some(n)),
        }
    }

    /// Search the Tantivy index for the "top N" matching documents (ordered by a field) in a specific segment.
    ///
    /// The documents are returned in field order.  Largest first if `sortdir` is [`SortDirection::Desc`],
    /// or smallest first if it's [`SortDirection::Asc`].
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    fn top_by_field_in_segment(
        &self,
        segment_id: SegmentId,
        query: &SearchQueryInput,
        sort_field: String,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        let (segment_ord, segment_reader) = self
            .searcher
            .segment_readers()
            .iter()
            .enumerate()
            .find(|(_, reader)| reader.segment_id() == segment_id)
            .expect("segment {segment_id} should exist");
        let sort_field = self
            .schema
            .get_search_field(&SearchFieldName(sort_field.clone()))
            .expect("sort field should exist in index schema");

        let collector =
            TopDocs::with_limit(n).order_by_u64_field(sort_field.name.0.clone(), sortdir.into());
        let query = self.query(query);
        let weight = query
            .weight(tantivy::query::EnableScoring::Enabled {
                searcher: &self.searcher,
                statistics_provider: &self.searcher,
            })
            .expect("creating a Weight from a Query should not fail");
        let top_docs = collector
            .collect_segment(
                weight.as_ref(),
                segment_ord as SegmentOrdinal,
                segment_reader,
            )
            .expect("should be able to collect top-n in segment");
        let top_docs = collector
            .merge_fruits(vec![top_docs])
            .expect("should be able to merge top-n in segment");
        SearchResults::TopNByField(
            self.searcher.clone(),
            Default::default(),
            top_docs.into_iter(),
        )
    }

    /// Search the Tantivy index for the "top N" matching documents (ordered by score) in a specific segment.
    ///
    /// The documents are returned in score order.  Most relevant first if `sortdir` is [`SortDirection::Desc`],
    /// or least relevant first if it's [`SortDirection::Asc`].
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    fn top_by_score_in_segment(
        &self,
        segment_id: SegmentId,
        query: &SearchQueryInput,
        sortdir: SortDirection,
        n: usize,
        _need_scores: bool,
    ) -> SearchResults {
        let (segment_ord, segment_reader) = self
            .searcher
            .segment_readers()
            .iter()
            .enumerate()
            .find(|(_, reader)| reader.segment_id() == segment_id)
            .expect("segment {segment_id} should exist");

        let query = self.query(query);
        let weight = query
            .weight(tantivy::query::EnableScoring::Enabled {
                searcher: &self.searcher,
                statistics_provider: &self.searcher,
            })
            .expect("creating a Weight from a Query should not fail");

        match sortdir {
            // requires tweaking the score, which is a bit slower
            SortDirection::Asc => {
                let collector = TopDocs::with_limit(n).tweak_score(
                    move |_segment_reader: &tantivy::SegmentReader| {
                        move |_doc: DocId, original_score: Score| TweakedScore {
                            dir: sortdir,
                            score: original_score,
                        }
                    },
                );
                let top_docs = collector
                    .collect_segment(
                        weight.as_ref(),
                        segment_ord as SegmentOrdinal,
                        segment_reader,
                    )
                    .expect("should be able to collect top-n in segment");

                let top_docs = collector
                    .merge_fruits(vec![top_docs])
                    .expect("should be able to merge top-n in segment");

                SearchResults::TopNByTweakedScore(
                    self.searcher.clone(),
                    Default::default(),
                    top_docs.into_iter(),
                )
            }

            // can use tantivy's score directly
            SortDirection::Desc => {
                let collector = TopDocs::with_limit(n);
                let top_docs = collector
                    .collect_segment(
                        weight.as_ref(),
                        segment_ord as SegmentOrdinal,
                        segment_reader,
                    )
                    .expect("should be able to collect top-n in segment");

                let top_docs = collector
                    .merge_fruits(vec![top_docs])
                    .expect("should be able to merge top-n in segment");

                SearchResults::TopNByScore(
                    self.searcher.clone(),
                    Default::default(),
                    top_docs.into_iter(),
                )
            }

            SortDirection::None => {
                let iter = scorer_iter::ScorerIter::new(
                    DeferredScorer::new(
                        query,
                        false,
                        segment_reader.clone(),
                        self.searcher.clone(),
                    ),
                    segment_ord as SegmentOrdinal,
                    segment_reader.clone(),
                );
                SearchResults::SingleSegment(
                    self.searcher.clone(),
                    segment_ord as SegmentOrdinal,
                    None,
                    iter,
                )
            }
        }
    }

    pub fn estimate_docs(&self, search_query_input: &SearchQueryInput) -> Option<usize> {
        let largest_reader = self
            .searcher
            .segment_readers()
            .iter()
            .max_by_key(|reader| reader.num_docs())?;
        let query = self.query(search_query_input);
        let weight = query
            .weight(enable_scoring(
                search_query_input.contains_more_like_this(),
                &self.searcher,
            ))
            .expect("weight should be constructable");
        let mut scorer = weight
            .scorer(largest_reader, 1.0)
            .expect("counting docs in the largest segment should not fail");

        // investigate the size_hint.  it will often give us a good enough value
        let mut count = scorer.size_hint() as usize;
        if count == 0 {
            // but when it doesn't, we need to do a full count
            count = scorer.count_including_deleted() as usize;
        }
        let segment_doc_proportion =
            largest_reader.num_docs() as f64 / self.searcher.num_docs() as f64;

        Some((count as f64 / segment_doc_proportion).ceil() as usize)
    }

    fn collect<C: Collector + 'static>(
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
                enable_scoring(need_scores, &self.searcher),
            )
            .expect("search should not fail")
    }
}

fn enable_scoring(need_scores: bool, searcher: &Searcher) -> EnableScoring {
    if need_scores {
        EnableScoring::enabled_from_searcher(searcher)
    } else {
        EnableScoring::disabled_from_searcher(searcher)
    }
}

mod scorer_iter {
    use crate::index::reader::index::enable_scoring;
    use std::sync::OnceLock;
    use tantivy::query::{Query, Scorer};
    use tantivy::{DocAddress, DocId, DocSet, Score, Searcher, SegmentOrdinal, SegmentReader};

    pub struct DeferredScorer {
        query: Box<dyn Query>,
        need_scores: bool,
        segment_reader: SegmentReader,
        searcher: Searcher,
        scorer: OnceLock<Box<dyn Scorer>>,
    }

    impl DeferredScorer {
        pub fn new(
            query: Box<dyn Query>,
            need_scores: bool,
            segment_reader: SegmentReader,
            searcher: Searcher,
        ) -> Self {
            Self {
                query,
                need_scores,
                segment_reader,
                searcher,
                scorer: Default::default(),
            }
        }

        #[track_caller]
        #[inline(always)]
        fn scorer_mut(&mut self) -> &mut Box<dyn Scorer> {
            self.scorer();
            self.scorer
                .get_mut()
                .expect("deferred scorer should have been initialized")
        }

        #[track_caller]
        #[inline(always)]
        fn scorer(&self) -> &dyn Scorer {
            self.scorer.get_or_init(|| {
                let weight = self
                    .query
                    .weight(enable_scoring(self.need_scores, &self.searcher))
                    .expect("weight should be constructable");

                weight
                    .scorer(&self.segment_reader, 1.0)
                    .expect("scorer should be constructable")
            })
        }
    }

    impl DocSet for DeferredScorer {
        #[inline(always)]
        fn advance(&mut self) -> DocId {
            self.scorer_mut().advance()
        }

        #[inline(always)]
        fn doc(&self) -> DocId {
            self.scorer().doc()
        }

        fn size_hint(&self) -> u32 {
            self.scorer().size_hint()
        }
    }

    impl Scorer for DeferredScorer {
        #[inline(always)]
        fn score(&mut self) -> Score {
            self.scorer_mut().score()
        }
    }

    pub struct ScorerIter {
        deferred: DeferredScorer,
        segment_ord: SegmentOrdinal,
        segment_reader: SegmentReader,
    }

    impl ScorerIter {
        pub fn new(
            scorer: DeferredScorer,
            segment_ord: SegmentOrdinal,
            segment_reader: SegmentReader,
        ) -> Self {
            Self {
                deferred: scorer,
                segment_ord,
                segment_reader,
            }
        }
    }

    impl Iterator for ScorerIter {
        type Item = (Score, DocAddress);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let doc_id = self.deferred.doc();

                if doc_id == tantivy::TERMINATED {
                    // we've read all the docs
                    return None;
                } else if self
                    .segment_reader
                    .alive_bitset()
                    .map(|alive_bitset| alive_bitset.is_alive(doc_id))
                    // if there's no alive_bitset, the doc is alive
                    .unwrap_or(true)
                {
                    // this doc is alive
                    let score = self.deferred.score();
                    let this = (score, DocAddress::new(self.segment_ord, doc_id));

                    // move to the next doc for the next iteration
                    self.deferred.advance();

                    // return the live doc
                    return Some(this);
                }

                // this doc isn't alive, move to the next doc and loop around
                self.deferred.advance();
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, Some(self.deferred.size_hint() as usize))
        }
    }
}
