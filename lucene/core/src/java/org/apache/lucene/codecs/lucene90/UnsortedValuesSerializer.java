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
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * encodes efficiently an stream of unsorted long values using a {@link DirectWriter}
 * This a refactor of
 * @code Lucene80DocValuesConsumer#addSortedField(FieldInfo, DocValuesProducer)} line: 465
 */
public class UnsortedValuesSerializer implements LongIterator.Serializer {
  private static final UnsortedValuesSerializer INSTANCE = new UnsortedValuesSerializer();

  public static UnsortedValuesSerializer getInstance() {
    return INSTANCE;
  }

  private UnsortedValuesSerializer() {
    // use singleton instance
  }

  @Override
  public long write(IndexOutput dataOutput, LongIterator values) throws IOException {
    long startFP = dataOutput.getFilePointer();

    int numberOfBitsPerOrd = values.numBitsPerValue();

    DirectWriter writer = DirectWriter.getInstance(dataOutput, values.size(), numberOfBitsPerOrd);
    for (int i = 0; i < values.size(); i++) {
      writer.add(values.next());
    }
    writer.finish();

    long metaStartFP = dataOutput.getFilePointer();
    dataOutput.writeByte((byte) numberOfBitsPerOrd);
    dataOutput.writeVLong(startFP);
    // how many bytes to read
    dataOutput.writeVLong(metaStartFP - startFP);
    return metaStartFP;
  }

  @Override
  public IOSupplier<LongValues> read(IndexInput blockInput, long metadataStartFP) throws IOException {
    return new Reader(blockInput, metadataStartFP);
  }

  private static class Reader implements IOSupplier<LongValues> {
    private final long startFP;
    private final long length;
    private final byte numberOfBitsPerOrd;
    private final IndexInput blockInput;

    public Reader(IndexInput blockInput, long metadataStartFP) throws IOException {
      this.blockInput = blockInput.clone();

      this.blockInput.seek(metadataStartFP);
      this.numberOfBitsPerOrd = this.blockInput.readByte();
      this.startFP = this.blockInput.readVLong();
      this.length = this.blockInput.readVLong();
    }

    @Override
    public LongValues get() throws IOException {
      if (numberOfBitsPerOrd == 0) {
        return LongValues.ZEROES;
      }
      RandomAccessInput slice = blockInput.randomAccessSlice(startFP, length);
      return DirectReader.getInstance(slice, numberOfBitsPerOrd);
    }
  }
}