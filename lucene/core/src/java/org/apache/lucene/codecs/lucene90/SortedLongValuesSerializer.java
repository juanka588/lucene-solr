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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

/**
 * encodes efficiently an stream of sorted long values
 * @see SortedValuesWriter
 * This is a refactor of @code Lucene80DocValuesConsumer#addSortedField(FieldInfo, DocValuesProducer)} line: 680
 */
public class SortedLongValuesSerializer implements LongIterator.Serializer {
  private static final int BLOCK_SHIFT = 16;

  private static final SortedLongValuesSerializer INSTANCE = new SortedLongValuesSerializer();

  public static SortedLongValuesSerializer getInstance() {
    return INSTANCE;
  }

  private SortedLongValuesSerializer() {
    // use singleton instance
  }

  @Override
  public long write(IndexOutput dataOutput, LongIterator values) throws IOException {
    long totalSize = values.size();
    SortedValuesWriter writer = new SortedValuesWriter(dataOutput, totalSize, BLOCK_SHIFT);
    for (int i = 0; i < values.size(); i++) {
      writer.add(values.next());
    }
    return writer.finish();
  }

  @Override
  public IOSupplier<LongValues> read(IndexInput blockInput, long metadataFP) throws IOException {
    return new Reader(blockInput, metadataFP);
  }

  private static class Reader implements IOSupplier<LongValues> {
    private final SortedValuesWriter.Meta meta;

    private final IndexInput blockInput;

    public Reader(IndexInput blockInput, long metadataFP) throws IOException {
      if (metadataFP == -1L) {
        this.blockInput = null;
        this.meta = null;
        return;
      }
      this.blockInput = blockInput.clone();
      this.blockInput.seek(metadataFP);
      this.meta = SortedValuesWriter.Meta.read(this.blockInput);
    }

    @Override
    public LongValues get() throws IOException {
      if (meta == null) {
        return null;
      }
      return SortedValuesWriter.Meta.getInstance(meta, blockInput);
    }
  }
}