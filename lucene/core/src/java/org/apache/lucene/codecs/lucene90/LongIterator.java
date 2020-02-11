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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LongValues;

/**
 * Represent an iterator of long elements
 */
public interface LongIterator {
  long size();

  long next() throws IOException;

  int numBitsPerValue();

  interface Serializer {
    /**
     * @return the file pointer to position the {@link IndexInput} for reading the elements
     */
    long write(IndexOutput dataOutput, LongIterator longIterator) throws IOException;

    IOSupplier<LongValues> read(IndexInput blockInput, long metadataFP) throws IOException;
  }
}