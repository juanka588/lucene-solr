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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

public class SortedSetDocValuesSupplier implements Accountable {

  protected final long numTerms;
  protected final IOSupplier<DocValuesDocIdSetIterator> docValuesIteratorSupplier;
  protected final IOSupplier<LongValues> valuesSupplier;
  protected final IOSupplier<LongValues> documentBoundariesSupplier;

  protected final IOSupplier<TermsEnum> termsSupplier;

  public SortedSetDocValuesSupplier(long numTerms,
                                    IOSupplier<DocValuesDocIdSetIterator> docValuesIteratorSupplier,
                                    IOSupplier<LongValues> valuesSupplier,
                                    IOSupplier<LongValues> documentBoundariesSupplier,
                                    IOSupplier<TermsEnum> termsSupplier) {
    this.numTerms = numTerms;
    this.docValuesIteratorSupplier = docValuesIteratorSupplier;
    this.valuesSupplier = valuesSupplier;
    this.documentBoundariesSupplier = documentBoundariesSupplier;
    this.termsSupplier = termsSupplier;
  }

  @Override
  public long ramBytesUsed() {
    //TODO
    return 0;
  }

  public SortedDocValues getSorted() throws IOException {
    return new CompoundSortedDocValues(numTerms, docValuesIteratorSupplier.get(), valuesSupplier.get(), termsSupplier);
  }

  public SortedSetDocValues getSortedSet() throws IOException {
    LongValues documentBoundaries = documentBoundariesSupplier.get();
    if (documentBoundaries == null) {
      return DocValues.singleton(new CompoundSortedDocValues(numTerms, docValuesIteratorSupplier.get(), valuesSupplier.get(), termsSupplier));
    }
    return new CompoundSortedSetDocValues(numTerms, termsSupplier, docValuesIteratorSupplier.get(), valuesSupplier.get(), documentBoundaries);
  }
}