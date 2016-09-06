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
package org.apache.nifi.accumulo;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.nifi.accumulo.mutation.MutationColumn;
import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.accumulo.scan.Column;
import org.apache.nifi.accumulo.scan.ResultHandler;
import org.apache.nifi.accumulo.validate.ConfigFilesValidator;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Collection;

@Tags({"accumulo", "client"})
@CapabilityDescription("A controller service for accessing an Accumulo client.")
public interface AccumuloClientService extends ControllerService {

    PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files," +
              " such as accumulo-site.xml and core-site.xml for kerberos, " +
              "including full paths to the files.")
            .addValidator(new ConfigFilesValidator())
            .build();

    PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor ZOOKEEPER_CLIENT_PORT = new PropertyDescriptor.Builder()
            .name("ZooKeeper Client Port")
            .description("The port on which ZooKeeper is accepting client connections. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    PropertyDescriptor ZOOKEEPER_ZNODE_PARENT = new PropertyDescriptor.Builder()
            .name("ZooKeeper ZNode Parent")
            .description("The ZooKeeper ZNode Parent value for Accumulo (example: /accumulo). Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor ACCUMULO_CLIENT_RETRIES = new PropertyDescriptor.Builder()
            .name("Accumulo Client Retries")
            .description("The number of times the Accumulo client will retry connecting. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    /**
     * Puts a batch of mutations to the given table.
     *
     * @param tableName the name of an Accumulo table
     * @param puts a list of put mutations for the given table
     * @throws IOException thrown when there are communication errors with Accumulo
     */
    void put(String tableName, Collection<MutationFlowFile> puts) throws IOException, TableNotFoundException, MutationsRejectedException;

    /**
     * Puts the given row to Accumulo with the provided columns.
     *
     * @param tableName the name of an Accumulo table
     * @param rowId the id of the row to put
     * @param columns the columns of the row to put
     * @throws IOException thrown when there are communication errors with Accumulo
     */
    void put(String tableName, String rowId, Collection<MutationColumn> columns) throws IOException, TableNotFoundException, MutationsRejectedException;

    /**
     * Scans the given table using the optional filter criteria and passing each result to the provided handler.
     *
     * @param tableName the name of an Accumulo table to scan
     * @param columns optional columns to return, if not specified all columns are returned
     * @param filterExpression optional filter expression, if not specified no filtering is performed
     * @param minTime the minimum timestamp of cells to return, passed to the Accumulo scanner timeRange
     * @param handler a handler to process rows of the result set
     * @throws IOException thrown when there are communication errors with Accumulo
     */
    void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime, ResultHandler handler) throws IOException;

}
