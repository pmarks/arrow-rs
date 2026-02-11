// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Prefetch pipeline for parallel row group IO
//!
//! Fetches data for multiple row groups concurrently using cloned
//! [`AsyncFileReader`] instances, while the decoder processes the current
//! row group.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, StreamExt};

use crate::arrow::arrow_reader::RowFilter;
use crate::arrow::async_reader::AsyncFileReader;
use crate::arrow::ProjectionMask;
use crate::errors::Result;
use crate::file::metadata::ParquetMetaData;

/// Pre-computed plan for fetching one row group's data
pub(crate) struct FetchPlan {
    pub ranges: Vec<Range<u64>>,
}

/// Result of a completed prefetch for one row group
pub(crate) struct PrefetchedRowGroup {
    pub ranges: Vec<Range<u64>>,
    pub data: Vec<Bytes>,
}

/// Type-erased trait for spawning fetch futures.
///
/// This hides the concrete `AsyncFileReader` type parameter so that
/// `PrefetchPipeline` does not need to be generic.
trait PrefetchSpawner: Send {
    fn spawn_fetch(&mut self, plan: FetchPlan)
        -> BoxFuture<'static, Result<PrefetchedRowGroup>>;
}

/// Concrete spawner that clones the reader for each fetch.
struct ReaderSpawner<T: AsyncFileReader + Clone + Send + 'static> {
    reader: T,
}

impl<T: AsyncFileReader + Clone + Send + 'static> PrefetchSpawner for ReaderSpawner<T> {
    fn spawn_fetch(
        &mut self,
        plan: FetchPlan,
    ) -> BoxFuture<'static, Result<PrefetchedRowGroup>> {
        let mut reader = self.reader.clone();
        let ranges = plan.ranges;
        async move {
            let data = reader.get_byte_ranges(ranges.clone()).await?;
            Ok(PrefetchedRowGroup { ranges, data })
        }
        .boxed()
    }
}

/// Pipeline that prefetches row group data concurrently.
///
/// Maintains up to `max_in_flight` concurrent IO futures via
/// [`FuturesOrdered`], feeding completed data to the decoder.
pub(crate) struct PrefetchPipeline {
    in_flight: FuturesOrdered<BoxFuture<'static, Result<PrefetchedRowGroup>>>,
    ready: VecDeque<PrefetchedRowGroup>,
    pending_plans: VecDeque<FetchPlan>,
    max_in_flight: usize,
    spawner: Box<dyn PrefetchSpawner>,
}

impl PrefetchPipeline {
    /// Create a new pipeline and submit the initial batch of fetches.
    pub fn new<T: AsyncFileReader + Clone + Send + 'static>(
        reader: T,
        plans: Vec<FetchPlan>,
        count: usize,
    ) -> Self {
        let mut pipeline = Self {
            in_flight: FuturesOrdered::new(),
            ready: VecDeque::new(),
            pending_plans: plans.into(),
            max_in_flight: count,
            spawner: Box::new(ReaderSpawner { reader }),
        };
        pipeline.fill_in_flight();
        pipeline
    }

    /// Top up in-flight futures to `max_in_flight`.
    fn fill_in_flight(&mut self) {
        while self.in_flight.len() < self.max_in_flight {
            if let Some(plan) = self.pending_plans.pop_front() {
                let future = self.spawner.spawn_fetch(plan);
                self.in_flight.push_back(future);
            } else {
                break;
            }
        }
    }

    /// Poll-based attempt to satisfy the decoder's data needs.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(Ok(data)))` — prefetched data that covers the needed ranges
    /// - `Poll::Ready(Some(Err(e)))` — a prefetch failed
    /// - `Poll::Ready(None)` — pipeline exhausted (no more prefetched data available)
    /// - `Poll::Pending` — data is being fetched, wake when ready
    pub fn try_satisfy(
        &mut self,
        needed_ranges: &[Range<u64>],
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<PrefetchedRowGroup>>> {
        // First check if we already have a ready result that covers the needed ranges
        if let Some(front) = self.ready.front() {
            if ranges_covered_by(&front.ranges, needed_ranges) {
                return Poll::Ready(Some(Ok(self.ready.pop_front().unwrap())));
            }
            // Stale data from an earlier (already-processed) row group — discard it
            self.ready.pop_front();
            return self.try_satisfy(needed_ranges, cx);
        }

        // Poll the next in-flight future
        match self.in_flight.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(data))) => {
                self.fill_in_flight();
                if ranges_covered_by(&data.ranges, needed_ranges) {
                    Poll::Ready(Some(Ok(data)))
                } else {
                    // Stale — discard and try again
                    self.try_satisfy(needed_ranges, cx)
                }
            }
            Poll::Ready(Some(Err(e))) => {
                self.fill_in_flight();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // No more in-flight futures and no pending plans
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Async attempt to satisfy the decoder's data needs.
    ///
    /// Returns `Some(result)` if the pipeline has data (or an error),
    /// `None` if the pipeline is exhausted.
    pub async fn try_satisfy_async(
        &mut self,
        needed_ranges: &[Range<u64>],
    ) -> Option<Result<PrefetchedRowGroup>> {
        // First check ready queue
        if let Some(front) = self.ready.front() {
            if ranges_covered_by(&front.ranges, needed_ranges) {
                return Some(Ok(self.ready.pop_front().unwrap()));
            }
            // Stale — discard
            self.ready.pop_front();
            return Box::pin(self.try_satisfy_async(needed_ranges)).await;
        }

        // Await the next in-flight future
        let result = self.in_flight.next().await?;
        self.fill_in_flight();
        match result {
            Ok(data) => {
                if ranges_covered_by(&data.ranges, needed_ranges) {
                    Some(Ok(data))
                } else {
                    // Stale — discard and try again
                    Box::pin(self.try_satisfy_async(needed_ranges)).await
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// Checks whether the `available` ranges cover all `needed` ranges.
///
/// Each needed range must be fully contained within at least one available range.
/// This mirrors the containment check in [`PushBuffers::has_range`].
fn ranges_covered_by(available: &[Range<u64>], needed: &[Range<u64>]) -> bool {
    needed.iter().all(|need| {
        available
            .iter()
            .any(|avail| avail.start <= need.start && avail.end >= need.end)
    })
}

/// Compute the combined projection mask that covers both the output projection
/// and all filter predicate projections.
pub(crate) fn compute_combined_projection(
    projection: &ProjectionMask,
    filter: Option<&RowFilter>,
) -> ProjectionMask {
    let mut combined = projection.clone();
    if let Some(filter) = filter {
        for predicate in filter.predicates() {
            combined.union(predicate.projection());
        }
    }
    combined
}

/// Compute fetch plans for each row group.
///
/// For each row group, computes the byte ranges for all columns included
/// in the `combined_projection` (full column chunks, no page-level selection).
pub(crate) fn compute_fetch_plans(
    metadata: &Arc<ParquetMetaData>,
    row_groups: &[usize],
    combined_projection: &ProjectionMask,
) -> Vec<FetchPlan> {
    let num_columns = metadata.file_metadata().schema_descr().num_columns();
    row_groups
        .iter()
        .map(|&rg_idx| {
            let rg_metadata = metadata.row_group(rg_idx);
            let ranges = (0..num_columns)
                .filter(|&col_idx| combined_projection.leaf_included(col_idx))
                .map(|col_idx| {
                    let column = rg_metadata.column(col_idx);
                    let (start, length) = column.byte_range();
                    start..(start + length)
                })
                .collect();
            FetchPlan { ranges }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranges_covered_by() {
        // Single range covers single need
        assert!(ranges_covered_by(&[0..100], &[10..50]));
        // Single range doesn't cover
        assert!(!ranges_covered_by(&[0..50], &[10..60]));
        // Multiple available cover multiple needs
        assert!(ranges_covered_by(&[0..100, 200..300], &[10..50, 210..250]));
        // Empty needed is always covered
        assert!(ranges_covered_by(&[0..100], &[]));
        // Empty available doesn't cover anything
        assert!(!ranges_covered_by(&[], &[0..10]));
    }
}
