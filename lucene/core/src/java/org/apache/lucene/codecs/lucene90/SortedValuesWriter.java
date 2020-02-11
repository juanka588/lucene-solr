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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/***
 * copy of {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, but without requiring any meta data output.
 */
public class SortedValuesWriter {

  final Meta meta;
  final IndexOutput data;
  final long numValues;
  final long dataStartFP;
  final long[] buffer;
  int bufferSize;
  long count;
  boolean finished;

  long previous = Long.MIN_VALUE;

  public SortedValuesWriter(IndexOutput dataOut, long numValues, int blockShift) {
    this.data = dataOut;
    this.numValues = numValues;
    if (blockShift < 2 || blockShift > 30) {
      throw new IllegalArgumentException("blockShift must be in [3-30], got " + blockShift);
    }
    this.meta = new Meta(numValues, blockShift);

    final int blockSize = 1 << blockShift;
    this.buffer = new long[blockSize];
    this.bufferSize = 0;
    this.dataStartFP = dataOut.getFilePointer();
  }

  private void flush() throws IOException {
    assert bufferSize != 0;

    final float avgInc = (float) ((double) (buffer[bufferSize - 1] - buffer[0]) / Math.max(1, bufferSize - 1));
    for (int i = 0; i < bufferSize; ++i) {
      final long expected = (long) (avgInc * (long) i);
      buffer[i] -= expected;
    }

    long min = buffer[0];
    for (int i = 1; i < bufferSize; ++i) {
      min = Math.min(buffer[i], min);
    }

    long maxDelta = 0;
    for (int i = 0; i < bufferSize; ++i) {
      buffer[i] -= min;
      // use | will change nothing when it comes to computing required bits
      // but has the benefit of working fine with negative values too
      // (in case of overflow)
      maxDelta |= buffer[i];
    }

    long len = data.getFilePointer() - dataStartFP;

    byte bitsRequired = 0;
    if (maxDelta != 0) {
      bitsRequired = (byte) DirectWriter.unsignedBitsRequired(maxDelta);
      DirectWriter writer = DirectWriter.getInstance(data, bufferSize, bitsRequired);
      for (int i = 0; i < bufferSize; ++i) {
        writer.add(buffer[i]);
      }
      writer.finish();
    }
    meta.addBlock(min, avgInc, len, bitsRequired);

    bufferSize = 0;
  }

  /**
   * Write a new value. Note that data might not make it to storage until
   * {@link #finish()} is called.
   *
   * @throws IllegalArgumentException if values don't come in order
   */
  public void add(long v) throws IOException {
    if (v < previous) {
      throw new IllegalArgumentException("Values do not come in order: " + previous + ", " + v);
    }
    if (bufferSize == buffer.length) {
      flush();
    }
    buffer[bufferSize++] = v;
    previous = v;
    count++;
  }

  /**
   * This must be called exactly once after all values have been {@link #add(long) added}.
   * returns the metadata file pointer to read the encoded values
   */
  public long finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    if (finished) {
      throw new IllegalStateException("#finish has been called already");
    }
    if (bufferSize > 0) {
      flush();
    }
    long metadataStartFP = data.getFilePointer();
    meta.dataStartFP = dataStartFP;
    meta.dataLen = metadataStartFP - dataStartFP;
    meta.write(data);
    finished = true;

    return metadataStartFP;
  }

  public static class Meta implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Meta.class);

    final int blockShift;
    final int numBlocks;
    final long[] mins;
    final float[] avgs;
    final byte[] bpvs;
    final long[] offsets;

    long dataStartFP;
    long dataLen;
    int index;

    Meta(long numValues, int blockShift) {
      this.blockShift = blockShift;
      long numBlocks = numValues >>> blockShift;
      if ((numBlocks << blockShift) < numValues) {
        numBlocks += 1;
      }
      this.numBlocks = (int) numBlocks;
      this.mins = new long[this.numBlocks];
      this.avgs = new float[this.numBlocks];
      this.bpvs = new byte[this.numBlocks];
      this.offsets = new long[this.numBlocks];
      this.index = 0;
    }

    Meta(long dataStartFP, long dataLen, int numBlocks, int blockShift) {
      this.dataStartFP = dataStartFP;
      this.dataLen = dataLen;
      this.blockShift = blockShift;
      this.numBlocks = numBlocks;

      this.mins = new long[this.numBlocks];
      this.avgs = new float[this.numBlocks];
      this.bpvs = new byte[this.numBlocks];
      this.offsets = new long[this.numBlocks];
      this.index = 0;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(mins)
          + RamUsageEstimator.sizeOf(avgs)
          + RamUsageEstimator.sizeOf(bpvs)
          + RamUsageEstimator.sizeOf(offsets);
    }

    public void addBlock(long min, float avgInc, long len, byte bitsRequired) {
      mins[index] = min;
      avgs[index] = avgInc;
      offsets[index] = len;
      bpvs[index] = bitsRequired;

      index++;
    }

    public void write(IndexOutput dataOutput) throws IOException {
      dataOutput.writeVLong(dataStartFP);
      dataOutput.writeVLong(dataLen);

      dataOutput.writeVInt(numBlocks);
      dataOutput.writeVInt(blockShift);

      for (int i = 0; i < numBlocks; i++) {
        dataOutput.writeLong(mins[i]);
        dataOutput.writeInt(Float.floatToIntBits(avgs[i]));
        dataOutput.writeLong(offsets[i]);
        dataOutput.writeByte(bpvs[i]);
      }
    }

    public static Meta read(DataInput dataInput) throws IOException {
      long dataStartFP = dataInput.readVLong();
      long dataLen = dataInput.readVLong();

      int numBlocks = dataInput.readVInt();
      int blockShift = dataInput.readVInt();
      Meta meta = new Meta(dataStartFP, dataLen, numBlocks, blockShift);

      for (int i = 0; i < numBlocks; i++) {
        meta.mins[i] = dataInput.readLong();
        meta.avgs[i] = Float.intBitsToFloat(dataInput.readInt());
        meta.offsets[i] = dataInput.readLong();
        meta.bpvs[i] = dataInput.readByte();
      }

      return meta;
    }

    public static LongValues getInstance(Meta meta, IndexInput indexInput) throws IOException {
      RandomAccessInput data = indexInput.randomAccessSlice(meta.dataStartFP, meta.dataLen);
      final LongValues[] readers = new LongValues[meta.numBlocks];
      for (int i = 0; i < meta.numBlocks; ++i) {
        if (meta.bpvs[i] == 0) {
          readers[i] = LongValues.ZEROES;
        } else {
          readers[i] = DirectReader.getInstance(data, meta.bpvs[i], meta.offsets[i]);
        }
      }
      final int blockShift = meta.blockShift;

      final long[] mins = meta.mins;
      final float[] avgs = meta.avgs;
      return new LongValues() {

        @Override
        public long get(long index) {
          final int block = (int) (index >>> blockShift);
          final long blockIndex = index & ((1 << blockShift) - 1);
          final long delta = readers[block].get(blockIndex);
          return mins[block] + (long) (avgs[block] * blockIndex) + delta;
        }

      };
    }
  }
}