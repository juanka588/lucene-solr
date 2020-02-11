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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;

/**
 * Binary Doc values Producer based on {@link Lucene80DocValuesProducer}
 */
public class Lucene80BinaryProducer extends Lucene80DocValuesProducer implements CompoundDocValuesFormat.Binary.Producer {

  public Lucene80BinaryProducer(IndexInput data, int maxDoc) {
    super(data, maxDoc);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field, CompoundFieldMetadata compoundFieldMetadata, IndexInput indexInput) throws IOException {
    BinaryEntry entry = getBinaryEntry(field, compoundFieldMetadata.getMetaStartFP(), indexInput);
    return getBinaryDocValues(entry);
  }

  private BinaryEntry getBinaryEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    BinaryEntry binaryEntry = binaries.get(field.name);
    if (binaryEntry != null) {
      return binaryEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    binaryEntry = readBinary(clone);

    binaries.put(field.name, binaryEntry);
    return binaryEntry;
  }

  @Override
  public void close() throws IOException {
    clearEntriesMap();
  }
}
