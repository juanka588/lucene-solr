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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene80.Lucene80BinaryConsumer;
import org.apache.lucene.codecs.lucene80.Lucene80SortedNumericConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.*;

public class CompoundDocValuesConsumer extends DocValuesConsumer {

  protected final IndexOutput indexOutput;
  protected final SegmentWriteState state;

  protected final ByteBuffersDataOutput fieldsMetadataOutput;
  protected final CompoundFieldMetadata.Serializer fieldMetadataSerializer;

  private Binary.Consumer binaryConsumer;
  private Numeric.Consumer numericConsumer;
  private SortedNumeric.Consumer sortedNumericConsumer;
  private Sorted.Consumer sortedConsumer;
  private SortedSet.Consumer sortedSetConsumer;

  private int fieldsNumber;
  private boolean isClosed;
  private boolean initialized;

  public CompoundDocValuesConsumer(SegmentWriteState state) throws IOException {
    this(state, NAME, EXTENSION, VERSION_CURRENT);
  }

  public CompoundDocValuesConsumer(SegmentWriteState state, String codecName, String extension, int versionCurrent) throws IOException {
    this.state = state;
    boolean success = false;
    IndexOutput indexOutput = null;
    try {
      String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
      indexOutput = state.directory.createOutput(termsName, state.context);
      CodecUtil.writeIndexHeader(indexOutput, codecName, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      fieldsMetadataOutput = new ByteBuffersDataOutput();
      fieldMetadataSerializer = new CompoundFieldMetadata.Serializer();
      this.indexOutput = indexOutput;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexOutput);
      }
    }
    initialized = false;
    isClosed = false;
  }

  protected Binary.Consumer createBinaryConsumer(SegmentWriteState state) {
    return new Lucene80BinaryConsumer(indexOutput, state.segmentInfo.maxDoc());
  }

  protected Sorted.Consumer createSortedConsumer(SegmentWriteState state) {
    return new SortedSetConsumer(state.segmentInfo.maxDoc());
  }

  protected SortedSet.Consumer createSortedSetConsumer(SegmentWriteState state) {
    return new SortedSetConsumer(state.segmentInfo.maxDoc());
  }

  protected Numeric.Consumer createNumericConsumer(SegmentWriteState state) {
    return new Lucene80SortedNumericConsumer(indexOutput, state.segmentInfo.maxDoc());
  }

  protected SortedNumeric.Consumer createSortedNumericConsumer(SegmentWriteState state) {
    return new Lucene80SortedNumericConsumer(indexOutput, state.segmentInfo.maxDoc());
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata fieldMetadata = numericConsumer.addNumericField(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldsNumber++;
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata fieldMetadata = binaryConsumer.addBinaryField(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldsNumber++;
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata fieldMetadata = sortedConsumer.addSortedField(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldsNumber++;
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata fieldMetadata = sortedNumericConsumer.addSortedNumericField(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldsNumber++;
    }
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata fieldMetadata = sortedSetConsumer.addSortedSetField(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldsNumber++;
    }
  }

  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }
    writeFieldsMetadata(fieldsNumber, fieldsMetadataOutput);

    IOUtils.close(indexOutput, binaryConsumer, numericConsumer, sortedNumericConsumer, sortedConsumer, sortedSetConsumer);
    isClosed = true;
  }

  protected void initializeIfNeeded() {
    if (!initialized) {
      binaryConsumer = createBinaryConsumer(state);
      numericConsumer = createNumericConsumer(state);
      sortedNumericConsumer = createSortedNumericConsumer(state);
      sortedSetConsumer = createSortedSetConsumer(state);
      sortedConsumer = createSortedConsumer(state);
      initialized = true;
    }
  }

  protected void writeFieldsMetadata(int fieldsNumber, ByteBuffersDataOutput fieldsOutput) throws IOException {
    long fieldsStartPosition = indexOutput.getFilePointer();
    indexOutput.writeVInt(fieldsNumber);
    fieldsOutput.copyTo(indexOutput);
    indexOutput.writeLong(fieldsStartPosition);
    CodecUtil.writeFooter(indexOutput);
  }
}