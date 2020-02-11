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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene80.TestLucene80DocValuesFormat;
import org.apache.lucene.util.TestUtil;

public class TestLucene90DocValuesFormat extends TestLucene80DocValuesFormat {
  private final Codec codec = TestUtil.alwaysDocValuesFormat(new CompoundDocValuesFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  public void testMergeStability() throws Exception {
    // ignore as we use V encoding file pointers may change in size depending of data insertion order.
  }

  @Override
  public void testRamBytesUsed() throws IOException {
    // ignored as now doc values dynamically use heap.
  }
}
