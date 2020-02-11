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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene80.DocValuesDocIdSetIteratorSerializer;
import org.apache.lucene.codecs.lucene80.Lucene80TermsEnumSerializer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat.*;
import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Consumer for writing Sorted and SortedSet fields
 * It is composed of:
 * <ul>
 *   <li>DocumentId Iterator Serializer: to traverse the list of document with values for the given field</li>
 *   <li>Values Serializer: to write the document ordinal(s)</li>
 *   <li>Document Boundaries Serializer: to write the document ordinals size</li>
 *   <li>TermsEnum Serializer: to write the terms dictionary</li>
 * </ul>
 */
public class SortedSetConsumer implements Sorted.Consumer, SortedSet.Consumer {

  protected final int maxDoc;

  protected SortedSetFieldMetadata.Serializer fieldMetadataSerializer;

  protected DocValuesDocIdSetIteratorSerializer docIdSetIteratorSerializer;
  protected LongIterator.Serializer valuesSerializer;
  protected LongIterator.Serializer documentBoundariesSerializer;
  protected TermsEnumSerializer termsEnumSerializer;

  private boolean initialized;

  public SortedSetConsumer(int maxDoc) {
    this.maxDoc = maxDoc;
    this.initialized = false;
  }

  protected SortedSetFieldMetadata.Serializer createFieldMetadataSerializer() {
    return SortedSetFieldMetadata.Serializer.getInstance();
  }

  protected DocValuesDocIdSetIteratorSerializer createDISIWriter() {
    return DocValuesDocIdSetIteratorSerializer.getInstance();
  }

  protected LongIterator.Serializer createValuesWriter() {
    return UnsortedValuesSerializer.getInstance();
  }

  protected LongIterator.Serializer createDocumentBoundariesWriter() {
    return SortedLongValuesSerializer.getInstance();
  }

  protected TermsEnumSerializer createTermsEnumWriter() {
    return Lucene80TermsEnumSerializer.getInstance();
  }

  private void initializeIfNeeded() {
    if (!initialized) {
      this.fieldMetadataSerializer = createFieldMetadataSerializer();
      this.docIdSetIteratorSerializer = createDISIWriter();
      this.valuesSerializer = createValuesWriter();
      this.documentBoundariesSerializer = createDocumentBoundariesWriter();
      this.termsEnumSerializer = createTermsEnumWriter();
      initialized = true;
    }
  }

  @Override
  public CompoundFieldMetadata addSortedField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException {
    initializeIfNeeded();
    SortedSetFieldMetadata fieldMetadata = new SortedSetFieldMetadata(field);

    SortedDocValues values = valuesProducer.getSorted(field);
    int numDocs = getNumDocs(values);
    fieldMetadata.setNumDocs(numDocs);

    // nothing to write
    if (numDocs == 0) {
      return null;
    }

    writeDISI(indexOutput, valuesProducer.getSorted(field), fieldMetadata, values.getValueCount());

    writeDocumentOrdinals(indexOutput, fieldMetadata, new SortedValuesIterator(valuesProducer.getSorted(field), fieldMetadata.getNumDocs()));

    termsEnumSerializer.write(indexOutput, values::termsEnum, fieldMetadata);

    long startFP = indexOutput.getFilePointer();
    fieldMetadataSerializer.write(indexOutput, fieldMetadata);
    return new CompoundFieldMetadata(field.number, DocValuesType.SORTED, startFP);
  }

  @Override
  public CompoundFieldMetadata addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException {
    initializeIfNeeded();
    SortedSetFieldMetadata fieldMetadata = new SortedSetFieldMetadata(field);

    SortedSetDocValues values = valuesProducer.getSortedSet(field);
    int numDocs = getNumDocs(values);
    fieldMetadata.setNumDocs(numDocs);

    // nothing to write
    if (numDocs == 0) {
      return null;
    }

    writeDISI(indexOutput, valuesProducer.getSortedSet(field), fieldMetadata, values.getValueCount());

    long numOrds = getNumOrds(valuesProducer.getSortedSet(field));
    writeDocumentOrdinals(indexOutput, fieldMetadata, new SortedSetValuesIterator(valuesProducer.getSortedSet(field), numOrds));// one value per document.

    termsEnumSerializer.write(indexOutput, values::termsEnum, fieldMetadata);

    if (fieldMetadata.getNumDocs() != numOrds) {
      // multi valued field
      writeDocumentBoundaries(indexOutput, fieldMetadata, valuesProducer.getSortedSet(field), fieldMetadata.getNumDocs());
    }

    long startFP = indexOutput.getFilePointer();
    fieldMetadataSerializer.write(indexOutput, fieldMetadata);
    return new CompoundFieldMetadata(field.number, DocValuesType.SORTED_SET, startFP);
  }

  private int getNumDocs(DocIdSetIterator values) throws IOException {
    int numDocs = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocs++;
    }
    return numDocs;
  }

  private void writeDISI(IndexOutput indexOutput, DocIdSetIterator docIdSetIterator, SortedSetFieldMetadata fieldMetadata, long valuesCount) throws IOException {
    fieldMetadata.setNumTerms(valuesCount);
    long documentIteratorStartFP = docIdSetIteratorSerializer.write(indexOutput, docIdSetIterator, fieldMetadata.getNumDocs(), maxDoc);
    fieldMetadata.setDocumentIteratorStartFP(documentIteratorStartFP);
  }

  private void writeDocumentOrdinals(IndexOutput blockOutput, SortedSetFieldMetadata fieldMetadata, LongIterator longIterator) throws IOException {
    long startFP = valuesSerializer.write(blockOutput, longIterator);
    fieldMetadata.setValuesStartFP(startFP);
  }

  private long getNumOrds(SortedSetDocValues values) throws IOException {
    long numOrds = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (long ord = values.nextOrd(); ord != NO_MORE_ORDS; ord = values.nextOrd()) {
        numOrds++;
      }
    }
    return numOrds;
  }

  private void writeDocumentBoundaries(IndexOutput blockOutput, SortedSetFieldMetadata fieldMetadata, SortedSetDocValues sortedSetDocValues, int numDocsWithField) throws IOException {
    long startFP = documentBoundariesSerializer.write(
        blockOutput,
        new DocumentBoundariesIterator(sortedSetDocValues, numDocsWithField + 1L)
    );
    fieldMetadata.setDocumentBoundariesMetaFP(startFP);
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  private static class SortedValuesIterator implements LongIterator {
    private final SortedDocValues docValues;
    private long size;
    int docId;

    private SortedValuesIterator(SortedDocValues docValues, long size) {
      this.docValues = docValues;
      this.size = size;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public long next() throws IOException {
      docId = docValues.nextDoc();
      if (docId == NO_MORE_DOCS) {
        return NO_MORE_ORDS;
      }
      return docValues.ordValue();
    }

    @Override
    public int numBitsPerValue() {
      return DirectWriter.unsignedBitsRequired(docValues.getValueCount() - 1L);
    }
  }

  private static class SortedSetValuesIterator implements LongIterator {
    final SortedSetDocValues docValues;
    private long size;
    int docId;
    long ordValue = NO_MORE_ORDS;

    private SortedSetValuesIterator(SortedSetDocValues docValues, long size) {
      this.docValues = docValues;
      this.size = size;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public long next() throws IOException {
      if (ordValueExhausted() && hasNextDoc()) {
        return ordValue = docValues.nextOrd();
      }
      if (ordValueExhausted() && !hasNextDoc()) {
        return ordValue = NO_MORE_ORDS;
      }

      ordValue = docValues.nextOrd();

      if (ordValueExhausted()) {
        if (!hasNextDoc()) {
          return ordValue = NO_MORE_ORDS;
        } else {
          return ordValue = docValues.nextOrd();
        }
      } else {
        return ordValue;
      }
    }

    @Override
    public int numBitsPerValue() {
      return DirectWriter.unsignedBitsRequired(docValues.getValueCount() - 1L);
    }

    private boolean hasNextDoc() throws IOException {
      docId = docValues.nextDoc();
      return docId != NO_MORE_DOCS;
    }

    boolean ordValueExhausted() {
      return ordValue == NO_MORE_ORDS;
    }
  }

  private static class DocumentBoundariesIterator extends SortedSetValuesIterator {
    long addr = -1;

    private DocumentBoundariesIterator(SortedSetDocValues docValues, long numDocs) {
      super(docValues, numDocs);
    }

    @Override
    public long next() throws IOException {
      // first
      if (addr == -1) {
        addr++;
        return addr;
      }

      docId = docValues.nextDoc();
      if (docId == NO_MORE_DOCS) {
        return NO_MORE_ORDS;
      }
      ordValue = docValues.nextOrd();
      while (!ordValueExhausted()) {
        ordValue = docValues.nextOrd();
        addr++;
      }
      return addr;
    }
  }
}