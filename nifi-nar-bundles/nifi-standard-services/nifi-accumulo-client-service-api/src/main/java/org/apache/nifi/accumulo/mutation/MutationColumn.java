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
package org.apache.nifi.accumulo.mutation;

/**
 * Encapsulates the information for one column of a put operation.
 */
public class MutationColumn {

    private final String columnFamily;
    private final String columnQualifier;
    private final byte[] buffer;
    private final String visibility;

    public MutationColumn(final String columnFamily, final String columnQualifier, final byte[] buffer) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.buffer = buffer;
        this.visibility = null;
    }

    public MutationColumn(final String columnFamily, final String columnQualifier, final byte[] buffer, String visibility) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.buffer = buffer;
        this.visibility = visibility;
    }
    
    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public String getVisibility() {
        return visibility;
    }
    
}
