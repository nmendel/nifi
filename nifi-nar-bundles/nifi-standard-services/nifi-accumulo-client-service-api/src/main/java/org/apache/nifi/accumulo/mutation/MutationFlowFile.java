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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the FlowFile.
 */
public class MutationFlowFile {

    private final String tableName;
    private final String row;
    private final Collection<MutationColumn> columns;
    private final FlowFile flowFile;

    public MutationFlowFile(String tableName, String row, Collection<MutationColumn> columns, FlowFile flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.columns = columns;
        this.flowFile = flowFile;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public Collection<MutationColumn> getColumns() {
        return columns;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public boolean isValid() {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(row) || flowFile == null || columns == null || columns.isEmpty()) {
            return false;
        }

        for (MutationColumn column : columns) {
            if (StringUtils.isBlank(column.getColumnQualifier()) || StringUtils.isBlank(column.getColumnFamily()) || column.getBuffer() == null) {
                return false;
            }
        }

        return true;
    }

}
