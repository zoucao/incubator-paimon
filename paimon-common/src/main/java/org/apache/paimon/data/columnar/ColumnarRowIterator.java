/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecyclableIterator;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow> {

    private ColumnarRow rowData;
    private final RecordReader.RecordIterator<InternalRow> iterator;

    private int num;
    private int pos;

    public ColumnarRowIterator(RecordReader.RecordIterator<InternalRow> iterator) {
        super(null);
        this.iterator = iterator;
    }

    public void set(int num) {
        this.num = num;
        this.pos = 0;
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        while (true) {
            if (rowData == null) {
                InternalRow nextColumnar = iterator.next();
                if (nextColumnar == null) {
                    iterator.releaseBatch();
                    return null;
                } else {
                    Preconditions.
                        checkArgument(nextColumnar instanceof ColumnarRow,
                            "ColumnarRowIterator only accept ColumnarRow as input");
                    rowData = (ColumnarRow) nextColumnar;
                    set(rowData.getNumRows());
                }
            } else if (pos < num) {
                rowData.setRowId(pos++);
                return rowData;
            } else {
                rowData = null;
            }
        }
    }

    @Override
    public void releaseBatch() {
        if (iterator != null) {
            iterator.releaseBatch();
        }
    }
}
