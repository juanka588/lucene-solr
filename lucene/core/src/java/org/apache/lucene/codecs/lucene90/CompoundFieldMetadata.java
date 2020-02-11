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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Field metadata needed for the compound doc values format in order to decode metadata for a given field.
 */
public class CompoundFieldMetadata {
  private final int fieldId;
  private final DocValuesType type;
  private final long metaStartFP;

  public CompoundFieldMetadata(int fieldId, DocValuesType type, long metaStartFP) {
    this.fieldId = fieldId;
    this.type = type;
    this.metaStartFP = metaStartFP;
  }

  public long getMetaStartFP() {
    return metaStartFP;
  }

  public DocValuesType getType() {
    return type;
  }

  public int getFieldId() {
    return fieldId;
  }

  public static class Serializer {
    public void write(DataOutput output, CompoundFieldMetadata fieldMetadata) throws IOException {
      output.writeVInt(fieldMetadata.fieldId);
      output.writeVLong(fieldMetadata.metaStartFP);
      output.writeByte(toByte(fieldMetadata.type));
    }

    public CompoundFieldMetadata read(DataInput dataInput) throws IOException {
      int fieldId = dataInput.readVInt();
      long startFP = dataInput.readVLong();
      DocValuesType type = fromByte(dataInput.readByte());
      return new CompoundFieldMetadata(fieldId, type, startFP);
    }
  }

  static byte toByte(DocValuesType docValuesType) {
    switch (docValuesType) {
      case NUMERIC:
        return 1;
      case SORTED_NUMERIC:
        return 2;
      case SORTED:
        return 3;
      case SORTED_SET:
        return 4;
      case BINARY:
        return 5;
      default:
        throw new IllegalStateException("invalid doc values option");
    }
  }

  static DocValuesType fromByte(byte encodedType) {
    switch (encodedType) {
      case 1:
        return DocValuesType.NUMERIC;
      case 2:
        return DocValuesType.SORTED_NUMERIC;
      case 3:
        return DocValuesType.SORTED;
      case 4:
        return DocValuesType.SORTED_SET;
      case 5:
        return DocValuesType.BINARY;
      default:
        throw new IllegalStateException("invalid doc values option");
    }
  }
}