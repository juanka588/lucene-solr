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

import org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat;
import org.apache.lucene.codecs.lucene90.CompoundFieldMetadata;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
/**
 * Numeric and SortedNumeric producer based on {@link Lucene80DocValuesProducer}
 */
public class Lucene80SortedNumericProducer extends Lucene80DocValuesProducer implements CompoundDocValuesFormat.Numeric.Producer, CompoundDocValuesFormat.SortedNumeric.Producer {

  public Lucene80SortedNumericProducer(IndexInput data, int maxDoc) {
    super(data, maxDoc);
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    NumericEntry entry = getNumericEntry(field, compoundFieldMetadata.getMetaStartFP(), indexInput);
    return getNumeric(entry);
  }

  private NumericEntry getNumericEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    NumericEntry numericEntry = numerics.get(field.name);
    if (numericEntry != null) {
      return numericEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    numericEntry = new NumericEntry();
    readNumeric(clone, numericEntry);

    numerics.put(field.name, numericEntry);

    return numericEntry;
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    SortedNumericEntry sortedNumericEntry = getSortedNumericEntry(field, compoundFieldMetadata.getMetaStartFP(), indexInput);
    return getSortedNumericDocValues(sortedNumericEntry);
  }

  private SortedNumericEntry getSortedNumericEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    SortedNumericEntry sortedNumericEntry = sortedNumerics.get(field.name);
    if (sortedNumericEntry != null) {
      return sortedNumericEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    sortedNumericEntry = readSortedNumeric(clone);

    sortedNumerics.put(field.name, sortedNumericEntry);

    return sortedNumericEntry;
  }

  @Override
  public void close() throws IOException {
    clearEntriesMap();
  }
}
