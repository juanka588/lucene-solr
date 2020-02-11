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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.CompoundDocValuesFormat;
import org.apache.lucene.codecs.lucene90.CompoundFieldMetadata;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;

/**
 * Binary doc values consumer based on {@link Lucene80DocValuesConsumer}
 */
public class Lucene80BinaryConsumer extends Lucene80DocValuesConsumer implements CompoundDocValuesFormat.Binary.Consumer {

  public Lucene80BinaryConsumer(IndexOutput data, int maxDoc) {
    super(data, maxDoc);
  }

  @Override
  public CompoundFieldMetadata addBinaryField(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException {
    ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput buffersIndexOutput = new ByteBuffersIndexOutput(metaBuffer, "entry-buffer", "name");
    doAddBinary(field, valuesProducer, indexOutput, buffersIndexOutput);
    long metaStartFP = indexOutput.getFilePointer();
    metaBuffer.copyTo(indexOutput);
    return new CompoundFieldMetadata(field.number, DocValuesType.BINARY, metaStartFP);
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
