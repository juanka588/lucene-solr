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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene80.Lucene80BinaryProducer;
import org.apache.lucene.codecs.lucene80.Lucene80SortedNumericProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.Binary;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.EXTENSION;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.NAME;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.Numeric;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.Sorted;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.SortedNumeric;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.SortedSet;
import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.VERSION_CURRENT;

public class CompoundDocValuesProducer extends DocValuesProducer {

  protected final IndexInput indexInput;
  protected final Map<Integer, CompoundFieldMetadata> fieldMetadataMap;
  protected final SegmentReadState state;

  private Binary.Producer binaryProducer;
  private Numeric.Producer numericProducer;
  private SortedNumeric.Producer sortedNumericProducer;
  private Sorted.Producer sortedProducer;
  private SortedSet.Producer sortedSetProducer;

  private boolean initialized;

  public CompoundDocValuesProducer(SegmentReadState state) throws IOException {
    this(state, NAME, EXTENSION, VERSION_CURRENT, VERSION_CURRENT);
  }

  public CompoundDocValuesProducer(SegmentReadState state, String codecName, String extension, int versionStart, int versionCurrent) throws IOException {
    this.state = state;
    IndexInput indexInput = null;
    boolean success = false;
    try {
      String segmentName = state.segmentInfo.name;
      String termsName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, extension);
      indexInput = state.directory.openInput(termsName, state.context);

      CodecUtil.checkIndexHeader(indexInput, codecName, versionStart, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      CodecUtil.retrieveChecksum(indexInput);

      seekFieldsMetadata(indexInput);
      fieldMetadataMap = parseFieldsMetadata(indexInput);
      this.indexInput = indexInput;

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexInput);
      }
    }
    initialized = false;
  }

  protected void initializeIfNeeded() {
    if (!initialized) {
      sortedSetProducer = createSortedSetProducer(state);
      sortedProducer = createSortedProducer(state);
      sortedNumericProducer = createSortedNumericProducer(state);
      binaryProducer = createBinaryProducer(state);
      numericProducer = createNumericProducer(state);
      initialized = true;
    }
  }

  protected Numeric.Producer createNumericProducer(SegmentReadState state) {
    return new Lucene80SortedNumericProducer(indexInput, state.segmentInfo.maxDoc());
  }

  protected Binary.Producer createBinaryProducer(SegmentReadState state) {
    return new Lucene80BinaryProducer(indexInput, state.segmentInfo.maxDoc());
  }

  protected SortedNumeric.Producer createSortedNumericProducer(SegmentReadState state) {
    return new Lucene80SortedNumericProducer(indexInput, state.segmentInfo.maxDoc());
  }

  protected Sorted.Producer createSortedProducer(SegmentReadState state) {
    return new SortedSetProducer();
  }

  protected SortedSet.Producer createSortedSetProducer(SegmentReadState state) {
    return new SortedSetProducer();
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata compoundFieldMetadata = fieldMetadataMap.get(field.number);
    if (compoundFieldMetadata == null) {
      return DocValues.emptyNumeric();
    }
    return numericProducer.getNumeric(field, compoundFieldMetadata, indexInput);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata compoundFieldMetadata = fieldMetadataMap.get(field.number);
    if (compoundFieldMetadata == null) {
      return DocValues.emptyBinary();
    }
    return binaryProducer.getBinary(field, compoundFieldMetadata, indexInput);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata compoundFieldMetadata = fieldMetadataMap.get(field.number);
    if (compoundFieldMetadata == null) {
      return DocValues.emptySorted();
    }
    return sortedProducer.getSorted(field, compoundFieldMetadata, indexInput);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata compoundFieldMetadata = fieldMetadataMap.get(field.number);
    if (compoundFieldMetadata == null) {
      return DocValues.emptySortedNumeric(0);
    }
    return sortedNumericProducer.getSortedNumeric(field, compoundFieldMetadata, indexInput);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    initializeIfNeeded();
    CompoundFieldMetadata compoundFieldMetadata = fieldMetadataMap.get(field.number);
    if (compoundFieldMetadata == null) {
      return DocValues.emptySortedSet();
    }
    return sortedSetProducer.getSortedSet(field, compoundFieldMetadata, indexInput);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(indexInput);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(indexInput, binaryProducer, numericProducer, sortedNumericProducer, sortedProducer, sortedSetProducer);
  }

  @Override
  public long ramBytesUsed() {
    //TODO
    return 0;
  }

  /**
   * Positions the given {@link IndexInput} at the beginning of the fields metadata.
   */
  protected static void seekFieldsMetadata(IndexInput indexInput) throws IOException {
    indexInput.seek(indexInput.length() - CodecUtil.footerLength() - 8);
    indexInput.seek(indexInput.readLong());
  }

  /**
   * @param indexInput {@link IndexInput} must be positioned to the fields metadata
   *                   details by calling {@link #seekFieldsMetadata(IndexInput)} before this call.
   */
  protected Map<Integer, CompoundFieldMetadata> parseFieldsMetadata(IndexInput indexInput) throws IOException {
    Map<Integer, CompoundFieldMetadata> fieldMetadataMap = new HashMap<>();
    int fieldsNumber = indexInput.readVInt();
    CompoundFieldMetadata.Serializer metaFieldSerializer = new CompoundFieldMetadata.Serializer();
    for (int i = 0; i < fieldsNumber; i++) {
      CompoundFieldMetadata fieldMetadata = metaFieldSerializer.read(indexInput);
      fieldMetadataMap.put(fieldMetadata.getFieldId(), fieldMetadata);
    }
    return fieldMetadataMap;
  }
}