/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.codecs.lucene80;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;

/**
 * Serializer for a {@link DocIdSetIterator}. This a refactor of
 * {@link Lucene80DocValuesConsumer#addSortedField(FieldInfo, DocValuesProducer)} line: 443
 */
public class DocValuesDocIdSetIteratorSerializer {
  private static final DocValuesDocIdSetIteratorSerializer INSTANCE = new DocValuesDocIdSetIteratorSerializer();

  public static DocValuesDocIdSetIteratorSerializer getInstance() {
    return INSTANCE;
  }

  private DocValuesDocIdSetIteratorSerializer() {
    // use singleton instance
  }

  public long write(IndexOutput dataOutput, DocIdSetIterator values, int numDocs, int maxDoc) throws IOException {
    if (numDocs == maxDoc) {
      return -1L;
    }
    long startFP = dataOutput.getFilePointer();

    short jumpTableEntryCount = IndexedDISI.writeBitSet(values, dataOutput, IndexedDISI.DEFAULT_DENSE_RANK_POWER);

    long metaStartFP = dataOutput.getFilePointer();
    dataOutput.writeVLong(startFP);
    // how many bytes to read
    dataOutput.writeVLong(metaStartFP - startFP);
    dataOutput.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    dataOutput.writeShort(jumpTableEntryCount);
    return metaStartFP;
  }

  public IOSupplier<DocValuesDocIdSetIterator> read(IndexInput blockInput, long startFP, int docCount) throws IOException {
    return new Reader(blockInput, startFP, docCount);
  }

  private static class Reader implements IOSupplier<DocValuesDocIdSetIterator> {
    private final long offset;
    private final long length;
    private final byte denseRankPower;
    private final int jumpTableEntryCount;

    private final int docCount;

    private final IndexInput blockInput;

    public Reader(IndexInput blockInput, long startFP, int docCount) throws IOException {
      this.blockInput = blockInput.clone();
      this.docCount = docCount;
      if (startFP == -1L) {
        this.offset = -1L;
        this.length = -1L;
        this.denseRankPower = 0;
        this.jumpTableEntryCount = 0;
      } else {
        this.blockInput.seek(startFP);
        this.offset = this.blockInput.readVLong();
        this.length = this.blockInput.readVLong();
        this.denseRankPower = this.blockInput.readByte();
        this.jumpTableEntryCount = this.blockInput.readShort();
      }
    }

    public DocValuesDocIdSetIterator get() throws IOException {
      if (offset == -1L) {
        //doc count == max doc
        return new DocValuesDocIdSetIterator.DenseIterator(docCount);
      }
      return new DocValuesDocIdSetIterator.SparseIterator(new IndexedDISI(blockInput, offset, length, jumpTableEntryCount, denseRankPower, docCount));
    }
  }
}