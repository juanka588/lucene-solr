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

import org.apache.lucene.codecs.lucene90.SortedSetFieldMetadata;
import org.apache.lucene.codecs.lucene90.TermsEnumSerializer;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOSupplier;

/**
 * TermsEnum writer / reader based on {@link Lucene80DocValuesProducer.TermsDict}
 * having low heap memory consumption.
 */
public class Lucene80TermsEnumSerializer implements TermsEnumSerializer {

  private static final Lucene80TermsEnumSerializer INSTANCE = new Lucene80TermsEnumSerializer();

  public static Lucene80TermsEnumSerializer getInstance() {
    return INSTANCE;
  }

  private Lucene80TermsEnumSerializer() {
    // use getInstance instead
  }

  @Override
  public void write(IndexOutput indexOutput, IOSupplier<BytesRefIterator> supplier, SortedSetFieldMetadata fieldMetadata) throws IOException {
    ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput buffersIndexOutput = new ByteBuffersIndexOutput(metaBuffer, "entry-buffer", "name");
    Lucene80DocValuesConsumer.addTermsDict(supplier, indexOutput, buffersIndexOutput, fieldMetadata.getNumTerms());
    long metaStartFP = indexOutput.getFilePointer();
    metaBuffer.copyTo(indexOutput);
    fieldMetadata.setTermsDictionaryStartFP(metaStartFP);
  }

  @Override
  public IOSupplier<TermsEnum> read(IndexInput blockInput, SortedSetFieldMetadata fieldMetadata) throws IOException {
    return new TermsEnumSupplier(blockInput, fieldMetadata);
  }

  private static class TermsEnumSupplier implements IOSupplier<TermsEnum> {
    private final IndexInput indexInput;
    private final Lucene80DocValuesProducer.TermsDictEntry entry;

    public TermsEnumSupplier(IndexInput indexInput, SortedSetFieldMetadata fieldMetadata) throws IOException {
      this.indexInput = indexInput.clone();
      this.indexInput.seek(fieldMetadata.getTermsDictionaryStartFP());

      Lucene80DocValuesProducer.TermsDictEntry toRead = new Lucene80DocValuesProducer.TermsDictEntry();
      Lucene80DocValuesProducer.readTermDict(this.indexInput, toRead);
      this.entry = toRead;
    }

    @Override
    public TermsEnum get() throws IOException {
      return new Lucene80DocValuesProducer.TermsDict(entry, indexInput);
    }
  }
}