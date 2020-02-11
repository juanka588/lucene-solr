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

import org.apache.lucene.index.DocValuesIterator;

/**
 * Complete Document Id Set Iterator with all required method for doc values
 */
public abstract class DocValuesDocIdSetIterator extends DocValuesIterator {
  /**
   * equivalent to {@link IndexedDISI#index()}
   */
  public abstract long index();

  /**
   * Iterator created when all docs are present for a given field.
   */
  public static class DenseIterator extends DocValuesDocIdSetIterator {
    private final int maxDoc;
    private int doc = -1;

    public DenseIterator(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public long index() {
      return doc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      advance(target);
      return target == docID();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      doc = target;
      if (doc >= maxDoc) {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc;
    }
  }

  /**
   * Iterator based on {@link IndexedDISI}
   */
  public static class SparseIterator extends DocValuesDocIdSetIterator {
    private final IndexedDISI indexedDISI;

    public SparseIterator(IndexedDISI indexedDISI) {
      this.indexedDISI = indexedDISI;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return indexedDISI.advanceExact(target);
    }

    @Override
    public int docID() {
      return indexedDISI.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return indexedDISI.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return indexedDISI.advance(target);
    }

    @Override
    public long cost() {
      return indexedDISI.cost();
    }

    public long index() {
      return indexedDISI.index();
    }
  }
}