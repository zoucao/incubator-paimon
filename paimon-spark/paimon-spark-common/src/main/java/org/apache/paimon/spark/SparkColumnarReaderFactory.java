/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SparkColumnarReaderFactory implements PartitionReaderFactory {

  private final ReadBuilder readBuilder;

  public SparkColumnarReaderFactory(ReadBuilder readBuilder) {
    this.readBuilder = readBuilder;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    throw new UnsupportedOperationException("Row-based reads are not supported in ColumnarReader");
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    RecordReader<org.apache.paimon.data.InternalRow> reader;
    try {
      reader = readBuilder.newRead().createReader(((SparkInputPartition) partition).split());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    RecordReaderIterator<org.apache.paimon.data.InternalRow> iterator = new RecordReaderIterator<>(reader);
    SparkInternalRow row = new SparkInternalRow(readBuilder.readType());
    new SparkInputPartitionReader(iterator, row);

    return null;
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
