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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

public class CompoundSortedSetDocValues extends SortedSetDocValues implements Accountable {

  protected final DocValuesDocIdSetIterator docValuesIterator;
  protected final LongValues documentStoredOrdinals;
  protected final LongValues documentBoundaries;

  protected final long numTerms;
  protected final IOSupplier<TermsEnum> termsEnumSupplier;
  protected final TermsEnum termsEnum;

  private boolean set;
  private long start;
  private long end = 0;

  public CompoundSortedSetDocValues(long numTerms, IOSupplier<TermsEnum> termsEnumSupplier,
                                    DocValuesDocIdSetIterator docValuesIterator,
                                    LongValues documentStoredOrdinals,
                                    LongValues documentBoundaries) throws IOException {
    this.numTerms = numTerms;
    this.termsEnumSupplier = termsEnumSupplier;
    this.docValuesIterator = docValuesIterator;
    this.documentStoredOrdinals = documentStoredOrdinals;
    this.documentBoundaries = documentBoundaries;
    this.termsEnum = termsEnumSupplier.get();
  }

  @Override
  public long getValueCount() {
    return numTerms;
  }

  @Override
  public BytesRef lookupOrd(long ord) throws IOException {
    termsEnum.seekExact(ord);
    return termsEnum.term();
  }

  @Override
  public long lookupTerm(BytesRef key) throws IOException {
    TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
    if (status == TermsEnum.SeekStatus.FOUND) {
      return termsEnum.ord();
    }
    if (status == TermsEnum.SeekStatus.END) {
      return -1L - numTerms;
    }
    return -1L - termsEnum.ord();
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
  public int nextDoc() throws IOException {
    set = false;
    return docValuesIterator.nextDoc();
  }

  @Override
  public int docID() {
    return docValuesIterator.docID();
  }

  @Override
  public long cost() {
    return docValuesIterator.cost();
  }

  @Override
  public int advance(int target) throws IOException {
    set = false;
    return docValuesIterator.advance(target);
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    set = false;
    return docValuesIterator.advanceExact(target);
  }

  @Override
  public long nextOrd() {
    if (!set) {
      final long index = docValuesIterator.index();
      final long start = documentBoundaries.get(index);
      this.start = start + 1;
      end = documentBoundaries.get(index + 1L);
      set = true;
      return documentStoredOrdinals.get(start);
    } else if (start == end) {
      return NO_MORE_ORDS;
    } else {
      return documentStoredOrdinals.get(start++);
    }
  }
}