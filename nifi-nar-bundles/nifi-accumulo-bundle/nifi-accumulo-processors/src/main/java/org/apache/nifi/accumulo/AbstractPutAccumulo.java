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


import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for processors that put data to Accumulo.
 */
public abstract class AbstractPutAccumulo extends AbstractProcessor {

    protected static final PropertyDescriptor ACCUMULO_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Accumulo Client Service")
            .description("Specifies the Controller Service to use for accessing Accumulo.")
            .required(true)
            .identifiesControllerService(AccumuloClientService.class)
            .build();
    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("Specifies the Row ID to use when inserting data into Accumulo")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into Accumulo")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to use when inserting data into Accumulo")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Accumulo")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final Map<String,List<MutationFlowFile>> tablePuts = new HashMap<>();

        // Group FlowFiles by Accumulo Table
        for (final FlowFile flowFile : flowFiles) {
            final MutationFlowFile putFlowFile = createPut(session, context, flowFile);

            if (putFlowFile == null) {
                // sub-classes should log appropriate error messages before returning null
                session.transfer(flowFile, REL_FAILURE);
            } else if (!putFlowFile.isValid()) {
                if (StringUtils.isBlank(putFlowFile.getTableName())) {
                    getLogger().error("Missing table name for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (StringUtils.isBlank(putFlowFile.getRow())) {
                    getLogger().error("Missing row id for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (putFlowFile.getColumns() == null || putFlowFile.getColumns().isEmpty()) {
                    getLogger().error("No columns provided for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else {
                    // really shouldn't get here, but just in case
                    getLogger().error("Failed to produce a put for FlowFile {}; routing to failure", new Object[]{flowFile});
                }
                session.transfer(flowFile, REL_FAILURE);
            } else {
                List<MutationFlowFile> putFlowFiles = tablePuts.get(putFlowFile.getTableName());
                if (putFlowFiles == null) {
                    putFlowFiles = new ArrayList<>();
                    tablePuts.put(putFlowFile.getTableName(), putFlowFiles);
                }
                putFlowFiles.add(putFlowFile);
            }
        }

        getLogger().debug("Sending {} FlowFiles to Accumulo in {} put operations", new Object[]{flowFiles.size(), tablePuts.size()});

        final long start = System.nanoTime();
        final List<MutationFlowFile> successes = new ArrayList<>();
        final AccumuloClientService hBaseClientService = context.getProperty(ACCUMULO_CLIENT_SERVICE).asControllerService(AccumuloClientService.class);

        for (Map.Entry<String, List<MutationFlowFile>> entry : tablePuts.entrySet()) {
            try {
                hBaseClientService.put(entry.getKey(), entry.getValue());
                successes.addAll(entry.getValue());
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);

                for (MutationFlowFile putFlowFile : entry.getValue()) {
                    getLogger().error("Failed to send {} to Accumulo due to {}; routing to failure", new Object[]{putFlowFile.getFlowFile(), e});
                    final FlowFile failure = session.penalize(putFlowFile.getFlowFile());
                    session.transfer(failure, REL_FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().debug("Sent {} FlowFiles to Accumulo successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (MutationFlowFile putFlowFile : successes) {
            session.transfer(putFlowFile.getFlowFile(), REL_SUCCESS);
            final String details = "Put " + putFlowFile.getColumns().size() + " cells to Accumulo";
            session.getProvenanceReporter().send(putFlowFile.getFlowFile(), getTransitUri(putFlowFile), details, sendMillis);
        }

    }

    protected String getTransitUri(MutationFlowFile putFlowFile) {
        return "accumulo://" + putFlowFile.getTableName() + "/" + putFlowFile.getRow();
    }

    /**
     * Sub-classes provide the implementation to create a put from a FlowFile.
     *
     * @param session
     *              the current session
     * @param context
     *              the current context
     * @param flowFile
     *              the FlowFile to create a Put from
     *
     * @return a MutationFlowFile instance for the given FlowFile
     */
    protected abstract MutationFlowFile createPut(final ProcessSession session, final ProcessContext context, final FlowFile flowFile);

}
