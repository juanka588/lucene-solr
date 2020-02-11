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

import org.apache.lucene.codecs.lucene80.DocValuesDocIdSetIterator;
import org.apache.lucene.codecs.lucene80.DocValuesDocIdSetIteratorSerializer;
import org.apache.lucene.codecs.lucene80.Lucene80TermsEnumSerializer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

public class SortedSetProducer implements CompoundDocValuesFormat.SortedSet.Producer, CompoundDocValuesFormat.Sorted.Producer {

  private final SortedSetFieldMetadata.Serializer fieldMetadataSerializer;

  private final Map<Integer, SortedSetDocValuesSupplier> fieldToSupplierMap;

  public SortedSetProducer() {
    fieldMetadataSerializer = SortedSetFieldMetadata.Serializer.getInstance();
    fieldToSupplierMap = new HashMap<>();
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    SortedSetDocValuesSupplier supplier = getDocValuesSupplier(field, compoundFieldMetadata, indexInput);
    return supplier.getSortedSet();
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    SortedSetDocValuesSupplier supplier = getDocValuesSupplier(field, compoundFieldMetadata, indexInput);
    return supplier.getSorted();
  }

  private SortedSetDocValuesSupplier getDocValuesSupplier(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    SortedSetDocValuesSupplier supplier = fieldToSupplierMap.get(field.number);
    if (supplier != null) {
      return supplier;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(compoundFieldMetadata.getMetaStartFP());
    SortedSetFieldMetadata fieldMetadata = fieldMetadataSerializer.read(clone, field);

    supplier = createSortedSetDocValuesSupplier(indexInput, fieldMetadata);

    fieldToSupplierMap.put(field.number, supplier);
    return supplier;
  }

  protected SortedSetDocValuesSupplier createSortedSetDocValuesSupplier(IndexInput indexInput, SortedSetFieldMetadata fieldMetadata) throws IOException {
    IOSupplier<DocValuesDocIdSetIterator> docValuesIteratorSupplier = getDISISerializer().read(indexInput, fieldMetadata.getDocumentIteratorStartFP(), fieldMetadata.getNumDocs());
    IOSupplier<LongValues> valuesSupplier = getValuesSerializer().read(indexInput, fieldMetadata.getValuesStartFP());
    IOSupplier<LongValues> documentBoundariesSupplier = getDocumentBoundariesSerializer().read(indexInput, fieldMetadata.getDocumentBoundariesStartFP());

    IOSupplier<TermsEnum> termsSupplier = getTermsEnumSerializer().read(indexInput, fieldMetadata);

    return new SortedSetDocValuesSupplier(
        fieldMetadata.getNumTerms(),
        docValuesIteratorSupplier,
        valuesSupplier,
        documentBoundariesSupplier,
        termsSupplier
    );
  }

  protected DocValuesDocIdSetIteratorSerializer getDISISerializer() {
    return DocValuesDocIdSetIteratorSerializer.getInstance();
  }

  protected LongIterator.Serializer getValuesSerializer() {
    return UnsortedValuesSerializer.getInstance();
  }

  protected LongIterator.Serializer getDocumentBoundariesSerializer() {
    return SortedLongValuesSerializer.getInstance();
  }

  protected TermsEnumSerializer getTermsEnumSerializer() {
    return Lucene80TermsEnumSerializer.getInstance();
  }

  @Override
  public void close() {
    fieldToSupplierMap.clear();
  }
}