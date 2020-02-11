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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * This formats allows composition of doc values sub formats
 * <ul>
 *   <li>{@link Binary}</li>
 *   <li>{@link Numeric}</li>
 *   <li>{@link SortedNumeric}</li>
 *   <li>{@link Sorted}</li>
 *   <li>{@link SortedSet}</li>
 * </ul>
 *
 */
public class CompoundDocValuesFormat extends DocValuesFormat {

  public static final String NAME = "compoundDV";

  static final String EXTENSION = "cmpdv";

  static final int VERSION_CURRENT = 0;

  public CompoundDocValuesFormat() {
    super(NAME);
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new CompoundDocValuesConsumer(state);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new CompoundDocValuesProducer(state);
  }

  public interface Binary {

    interface Consumer extends Closeable {
      /**
       * Writes binary docvalues for a field.
       *
       * @param field          field information
       * @param valuesProducer Binary values to write.
       * @param indexOutput
       * @throws IOException if an I/O error occurred.
       */
      CompoundFieldMetadata addBinaryField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
    }

    interface Producer extends Closeable {
      /**
       * Returns {@link BinaryDocValues} for this field.
       * The returned instance need not be thread-safe: it will only be
       * used by a single thread.
       */
      BinaryDocValues getBinary(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException;
    }
  }

  public interface Numeric {
    interface Consumer extends Closeable {
      /**
       * Writes numeric docvalues for a field.
       *
       * @param field          field information
       * @param valuesProducer Numeric values to write.
       * @param indexOutput
       * @throws IOException if an I/O error occurred.
       */
      CompoundFieldMetadata addNumericField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
    }

    interface Producer extends Closeable {
      /**
       * Returns {@link NumericDocValues} for this field.
       * The returned instance need not be thread-safe: it will only be
       * used by a single thread.
       */
      NumericDocValues getNumeric(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException;
    }
  }

  public interface SortedNumeric {

    interface Consumer extends Closeable {
      /**
       * Writes pre-sorted numeric docvalues for a field
       *
       * @param field          field information
       * @param valuesProducer produces the values to write
       * @param indexOutput
       * @throws IOException if an I/O error occurred.
       */
      CompoundFieldMetadata addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
    }

    interface Producer extends Closeable {
      /**
       * Returns {@link SortedNumericDocValues} for this field.
       * The returned instance need not be thread-safe: it will only be
       * used by a single thread.
       */
      SortedNumericDocValues getSortedNumeric(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException;
    }
  }

  public interface Sorted {

    interface Consumer extends Closeable {
      /**
       * Writes pre-sorted binary docvalues for a field.
       *
       * @param field          field information
       * @param valuesProducer produces the values and ordinals to write
       * @param indexOutput
       * @throws IOException if an I/O error occurred.
       */
      CompoundFieldMetadata addSortedField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
    }

    interface Producer extends Closeable {
      /**
       * Returns {@link SortedDocValues} for this field.
       * The returned instance need not be thread-safe: it will only be
       * used by a single thread.
       */
      SortedDocValues getSorted(FieldInfo field, CompoundFieldMetadata fieldMetadata, IndexInput indexInput) throws IOException;
    }
  }

  public interface SortedSet {

    interface Consumer extends Closeable {
      /**
       * Writes pre-sorted set docvalues for a field
       *
       * @param field          field information
       * @param valuesProducer produces the values to write
       * @param indexOutput
       * @throws IOException if an I/O error occurred.
       */
      CompoundFieldMetadata addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
    }

    interface Producer extends Closeable {
      /**
       * Returns {@link SortedSetDocValues} for this field.
       * The returned instance need not be thread-safe: it will only be
       * used by a single thread.
       */
      SortedSetDocValues getSortedSet(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException;
    }
  }
}