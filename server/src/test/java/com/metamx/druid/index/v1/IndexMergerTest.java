/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.index.v1;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 */
public class IndexMergerTest
{
  @Test
  public void testPersistCaseInsensitive() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist = IncrementalIndexTest.createCaseInsensitiveIndex(timestamp);

    final File tempDir = Files.createTempDir();
    try {
      MMappedIndex index = IndexIO.mapDir(IndexMerger.persist(toPersist, tempDir));

      Assert.assertEquals(2, index.getTimestamps().size());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
      Assert.assertEquals(0, index.getAvailableMetrics().size());
    }
    finally {
      tempDir.delete();
    }
  }

  @Test
  public void testPersistMergeCaseInsensitive() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    final IncrementalIndex toPersist1 = IncrementalIndexTest.createCaseInsensitiveIndex(timestamp);
    final IncrementalIndex toPersist2 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("DIm1", "DIM2"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "DIm1", "10000", "DIM2", "100000000")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dIM1", "dIm2"),
            ImmutableMap.<String, Object>of("DIm1", "1", "DIM2", "2", "dim1", "5", "dim2", "6")
        )
    );


    final File tempDir1 = Files.createTempDir();
    final File tempDir2 = Files.createTempDir();
    final File mergedDir = Files.createTempDir();
    try {
      MMappedIndex index1 = IndexIO.mapDir(IndexMerger.persist(toPersist1, tempDir1));

      Assert.assertEquals(2, index1.getTimestamps().size());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
      Assert.assertEquals(0, index1.getAvailableMetrics().size());

      MMappedIndex index2 = IndexIO.mapDir(IndexMerger.persist(toPersist2, tempDir2));

      Assert.assertEquals(2, index2.getTimestamps().size());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
      Assert.assertEquals(0, index2.getAvailableMetrics().size());

      MMappedIndex merged = IndexIO.mapDir(
          IndexMerger.mergeMMapped(Arrays.asList(index1, index2), new AggregatorFactory[]{}, mergedDir)
      );

      Assert.assertEquals(3, merged.getTimestamps().size());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
      Assert.assertEquals(0, merged.getAvailableMetrics().size());
    }
    finally {
      FileUtils.deleteQuietly(tempDir1);
      FileUtils.deleteQuietly(tempDir2);
      FileUtils.deleteQuietly(mergedDir);
    }
  }

  @Test
  public void testPersistMergeNonLexicographicDims() throws Exception
  {
    final long timestamp = new DateTime("2000-01-01").getMillis();
    final IncrementalIndex toPersist1 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});
    final IncrementalIndex toPersist2 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});

    final List<String> dimensions = ImmutableList.of("b", "a");

    final InputRow row1 = new MapBasedInputRow(
        timestamp,
        dimensions,
        ImmutableMap.<String, Object>of(
            "a", "x",
            "b", "y"
        )
    );

    final InputRow row2 = new MapBasedInputRow(
        timestamp,
        dimensions,
        ImmutableMap.<String, Object>of(
            "a", "y",
            "b", "x"
        )
    );

    toPersist1.add(row1);
    toPersist1.add(row2);
    toPersist2.add(row1);
    toPersist2.add(row2);

    final File tempDir1 = Files.createTempDir();
    final File tempDir2 = Files.createTempDir();
    final File mergedDir = Files.createTempDir();
    try {
      MMappedIndex index1 = IndexIO.mapDir(IndexMerger.persist(toPersist1, tempDir1));

      Assert.assertEquals("1.size", 2, index1.getTimestamps().size());
      Assert.assertEquals(0, index1.getAvailableMetrics().size());

      MMappedIndex index2 = IndexIO.mapDir(IndexMerger.persist(toPersist2, tempDir2));

      Assert.assertEquals("2.size", 2, index2.getTimestamps().size());
      Assert.assertEquals(0, index2.getAvailableMetrics().size());

      MMappedIndex merged = IndexIO.mapDir(
          IndexMerger.mergeMMapped(Arrays.asList(index1, index2), new AggregatorFactory[]{}, mergedDir)
      );

      Assert.assertEquals("merged.size", 2, merged.getTimestamps().size());
      Assert.assertEquals(dimensions, Lists.newArrayList(merged.getAvailableDimensions()));
      Assert.assertEquals(0, merged.getAvailableMetrics().size());
    }
    finally {
      FileUtils.deleteQuietly(tempDir1);
      FileUtils.deleteQuietly(tempDir2);
      FileUtils.deleteQuietly(mergedDir);
    }
  }

  @Test
  public void testPersistMergeDifferentDims() throws Exception
  {
    final long timestamp = new DateTime("2000-01-01").getMillis();
    final IncrementalIndex toPersist1 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});
    final IncrementalIndex toPersist2 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});

    final InputRow row1 = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("a"),
        ImmutableMap.<String, Object>of(
            "a", "y"
        )
    );

    final InputRow row2 = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("a", "b"),
        ImmutableMap.<String, Object>of(
            "a", "x",
            "b", "y"
        )
    );

    toPersist1.add(row1);
    toPersist1.add(row2);
    toPersist2.add(row1);
    toPersist2.add(row2);

    final File tempDir1 = Files.createTempDir();
    final File tempDir2 = Files.createTempDir();
    final File mergedDir = Files.createTempDir();
    try {
      MMappedIndex index1 = IndexIO.mapDir(IndexMerger.persist(toPersist1, tempDir1));

      Assert.assertEquals("1.size", 2, index1.getTimestamps().size());
      Assert.assertEquals(0, index1.getAvailableMetrics().size());

      MMappedIndex index2 = IndexIO.mapDir(IndexMerger.persist(toPersist2, tempDir2));

      Assert.assertEquals("2.size", 2, index2.getTimestamps().size());
      Assert.assertEquals(0, index2.getAvailableMetrics().size());

      MMappedIndex merged = IndexIO.mapDir(
          IndexMerger.mergeMMapped(Arrays.asList(index1, index2), new AggregatorFactory[]{}, mergedDir)
      );

      Assert.assertEquals("merged.size", 2, merged.getTimestamps().size());
      Assert.assertEquals(Lists.newArrayList("a", "b"), Lists.newArrayList(merged.getAvailableDimensions()));
      Assert.assertEquals(0, merged.getAvailableMetrics().size());
    }
    finally {
      FileUtils.deleteQuietly(tempDir1);
      FileUtils.deleteQuietly(tempDir2);
      FileUtils.deleteQuietly(mergedDir);
    }
  }
}
