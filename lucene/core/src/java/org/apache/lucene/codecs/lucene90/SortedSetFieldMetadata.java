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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class SortedSetFieldMetadata {

  private final FieldInfo fieldInfo;
  private int numDocs;
  private long numTerms;

  private long documentIteratorStartFP;
  private long valuesStartFP;
  private long documentBoundariesStartFP;

  private long termsDictionaryStartFP;

  private final boolean isMutable;

  public SortedSetFieldMetadata(FieldInfo fieldInfo) {
    this(fieldInfo, -1, -1L, -1L, -1L, -1L, -1L, true);
  }

  public SortedSetFieldMetadata(FieldInfo fieldInfo, int numDocs, long numTerms,
                                long documentIteratorStartFP, long valuesStartFP, long documentBoundariesStartFP, long termsDictionaryStartFP,
                                boolean isMutable) {
    this.fieldInfo = fieldInfo;
    this.numDocs = numDocs;
    this.numTerms = numTerms;
    this.documentIteratorStartFP = documentIteratorStartFP;
    this.valuesStartFP = valuesStartFP;
    this.documentBoundariesStartFP = documentBoundariesStartFP;
    this.termsDictionaryStartFP = termsDictionaryStartFP;
    this.isMutable = isMutable;
  }

  public void setNumDocs(int numDocs) {
    ensureMutability();
    this.numDocs = numDocs;
  }

  private void ensureMutability() {
    if (!isMutable) {
      throw new IllegalStateException("the instance is read only mode");
    }
  }

  public int getNumDocs() {
    return numDocs;
  }

  public void setNumTerms(long numTerms) {
    ensureMutability();
    this.numTerms = numTerms;
  }

  public long getNumTerms() {
    return numTerms;
  }

  public void setDocumentIteratorStartFP(long documentIteratorStartFP) {
    ensureMutability();
    this.documentIteratorStartFP = documentIteratorStartFP;
  }

  public long getDocumentIteratorStartFP() {
    return documentIteratorStartFP;
  }

  public void setValuesStartFP(long valuesStartFP) {
    ensureMutability();
    this.valuesStartFP = valuesStartFP;
  }

  public void setDocumentBoundariesMetaFP(long documentBoundariesStartFP) {
    ensureMutability();
    this.documentBoundariesStartFP = documentBoundariesStartFP;
  }

  public long getValuesStartFP() {
    return valuesStartFP;
  }

  public long getDocumentBoundariesStartFP() {
    return documentBoundariesStartFP;
  }

  public void setTermsDictionaryStartFP(long termsDictionaryStartFP) {
    this.termsDictionaryStartFP = termsDictionaryStartFP;
  }

  public long getTermsDictionaryStartFP() {
    return termsDictionaryStartFP;
  }

  public FieldInfo getFieldInfo() {
    return fieldInfo;
  }

  static class Serializer {
    private static final Serializer INSTANCE = new Serializer();

    public static Serializer getInstance() {
      return INSTANCE;
    }

    private Serializer() {
      // use getInstance instead
    }

    public void write(DataOutput dataOutput, SortedSetFieldMetadata fieldMetadata) throws IOException {
      dataOutput.writeVInt(fieldMetadata.numDocs);
      dataOutput.writeVLong(fieldMetadata.numTerms);

      dataOutput.writeZLong(fieldMetadata.documentIteratorStartFP);
      dataOutput.writeVLong(fieldMetadata.valuesStartFP);
      dataOutput.writeZLong(fieldMetadata.documentBoundariesStartFP);

      dataOutput.writeVLong(fieldMetadata.termsDictionaryStartFP);
    }

    public SortedSetFieldMetadata read(DataInput dataInput, FieldInfo fieldInfo) throws IOException {
      int numDocs = dataInput.readVInt();
      long numTerms = dataInput.readVLong();
      long documentIteratorStartFP = dataInput.readZLong();
      long valuesStartFP = dataInput.readVLong();
      long documentBoundariesStartFP = dataInput.readZLong();
      long termsDictionaryStartFP = dataInput.readVLong();
      return new SortedSetFieldMetadata(fieldInfo, numDocs, numTerms, documentIteratorStartFP, valuesStartFP, documentBoundariesStartFP, termsDictionaryStartFP, false);
    }
  }
}