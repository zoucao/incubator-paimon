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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ColumnarRecordReader<T> implements RecordReader<T> {


    private final Queue<ConcatRecordReader.ReaderSupplier<T>> queue;

    private RecordReader<T> current;

    protected ColumnarRecordReader(List<ConcatRecordReader.ReaderSupplier<T>> readerFactories) {
        readerFactories.forEach(
            supplier ->
                Preconditions.checkNotNull(supplier, "Reader factory must not be null."));
        this.queue = new LinkedList<>(readerFactories);
    }

    public static <R> RecordReader<R> create(List<ConcatRecordReader.ReaderSupplier<R>> readers) throws IOException {
        return readers.size() == 1 ? readers.get(0).get() : new ColumnarRecordReader<>(readers);
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        while (true) {
            if (current != null) {
                RecordIterator<T> iterator = current.readBatch();
                if (iterator != null) {
                    return iterator;
                }
                current.close();
                current = null;
            } else if (queue.size() > 0) {
                current = queue.poll().get();
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
        }
    }
}
