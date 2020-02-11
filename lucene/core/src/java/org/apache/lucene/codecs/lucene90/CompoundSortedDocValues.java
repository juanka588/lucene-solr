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

import org.apache.lucene.codecs.lucene80.DocValuesDocIdSetIterator;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

public class CompoundSortedDocValues extends SortedDocValues implements Accountable {

  protected final TermsEnum termsEnum;

  protected final long numTerms;
  protected final DocValuesDocIdSetIterator docValuesIterator;
  protected final LongValues documentStoredOrdinals;
  protected final IOSupplier<TermsEnum> termsEnumSupplier;

  public CompoundSortedDocValues(long numTerms,
                                 DocValuesDocIdSetIterator docValuesIterator,
                                 LongValues documentStoredOrdinals,
                                 IOSupplier<TermsEnum> termsEnumSupplier) throws IOException {
    this.numTerms = numTerms;
    this.termsEnumSupplier = termsEnumSupplier;
    this.documentStoredOrdinals = documentStoredOrdinals;
    this.docValuesIterator = docValuesIterator;
    this.termsEnum = termsEnumSupplier.get();
  }

  @Override
  public int getValueCount() {
    return Math.toIntExact(numTerms);
  }

  @Override
  public BytesRef lookupOrd(int ord) throws IOException {
    termsEnum.seekExact(ord);
    return termsEnum.term();
  }

  @Override
  public int lookupTerm(BytesRef key) throws IOException {
    TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
    if (status == TermsEnum.SeekStatus.FOUND) {
      return Math.toIntExact(termsEnum.ord());
    }
    if (status == TermsEnum.SeekStatus.END) {
      return Math.toIntExact(-1 - numTerms);
    }
    return Math.toIntExact(-1 - termsEnum.ord());
  }

  @Override
  public TermsEnum termsEnum() throws IOException {
    return termsEnumSupplier.get();
  }

  @Override
  public long ramBytesUsed() {
    //TODO refine this.
    return 0L;
  }

  @Override
  public int docID() {
    return docValuesIterator.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return docValuesIterator.nextDoc();
  }

  @Override
  public int ordValue() {
    return (int) documentStoredOrdinals.get(docValuesIterator.index());
  }

  @Override
  public int advance(int target) throws IOException {
    return docValuesIterator.advance(target);
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    return docValuesIterator.advanceExact(target);
  }

  @Override
  public long cost() {
    return docValuesIterator.cost();
  }
}