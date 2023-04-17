package org.apache.paimon.spark;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class SparkColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

  @Override
  public boolean next() throws IOException {
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
