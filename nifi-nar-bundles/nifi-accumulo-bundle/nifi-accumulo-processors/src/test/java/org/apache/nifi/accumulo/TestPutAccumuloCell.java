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

import org.apache.nifi.accumulo.mutation.MutationColumn;
import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPutAccumuloCell {

    @Test
    public void testSingleFlowFile() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final TestRunner runner = TestRunners.newTestRunner(PutAccumuloCell.class);
        runner.setProperty(PutAccumuloCell.TABLE_NAME, tableName);
        runner.setProperty(PutAccumuloCell.ROW_ID, row);
        runner.setProperty(PutAccumuloCell.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutAccumuloCell.COLUMN_QUALIFIER, columnQualifier);
        runner.setProperty(PutAccumuloCell.BATCH_SIZE, "1");

        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithEL() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutAccumuloCell.BATCH_SIZE, "1");

        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);

        final String content = "some content";
        final Map<String, String> attributes = getAtrributeMapWithEL(tableName, row, columnFamily, columnQualifier);
        runner.enqueue(content.getBytes("UTF-8"), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithELMissingAttributes() throws IOException, InitializationException {
        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutAccumuloCell.BATCH_SIZE, "1");

        final MockAccumuloClientService accumuloClient = new MockAccumuloClientService();
        runner.addControllerService("accumuloClient", accumuloClient);
        runner.enableControllerService(accumuloClient);
        runner.setProperty(PutAccumuloCell.ACCUMULO_CLIENT_SERVICE, "accumuloClient");

        getAccumuloClientService(runner);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"), new HashMap<String, String>());
        runner.run();

        runner.assertTransferCount(PutAccumuloCell.REL_SUCCESS, 0);
        runner.assertTransferCount(PutAccumuloCell.REL_FAILURE, 1);

        assertEquals(0, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFileWithELOneMissingAttributes() throws IOException, InitializationException {
        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutAccumuloCell.BATCH_SIZE, "10");

        final MockAccumuloClientService accumuloClient = new MockAccumuloClientService();
        runner.addControllerService("accumuloClient", accumuloClient);
        runner.enableControllerService(accumuloClient);
        runner.setProperty(PutAccumuloCell.ACCUMULO_CLIENT_SERVICE, "accumuloClient");

        getAccumuloClientService(runner);

        // this one will go to failure
        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"), new HashMap<String, String>());

        // this will go to success
        final String content2 = "some content2";
        final Map<String, String> attributes = getAtrributeMapWithEL("table", "row", "cf", "cq");
        runner.enqueue(content2.getBytes("UTF-8"), attributes);

        runner.run();
        runner.assertTransferCount(PutAccumuloCell.REL_SUCCESS, 1);
        runner.assertTransferCount(PutAccumuloCell.REL_FAILURE, 1);

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableDifferentRow() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row1 = "row1";
        final String row2 = "row2";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAtrributeMapWithEL(tableName, row1, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        final Map<String, String> attributes2 = getAtrributeMapWithEL(tableName, row2, columnFamily, columnQualifier);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content1);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(tableName);
        assertEquals(2, puts.size());
        verifyPut(row1, columnFamily, columnQualifier, content1, puts.get(0));
        verifyPut(row2, columnFamily, columnQualifier, content2, puts.get(1));

        assertEquals(2, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableDifferentRowFailure() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row1 = "row1";
        final String row2 = "row2";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        accumuloClient.setThrowException(true);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAtrributeMapWithEL(tableName, row1, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        final Map<String, String> attributes2 = getAtrributeMapWithEL(tableName, row2, columnFamily, columnQualifier);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 2);

        assertEquals(0, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableSameRow() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutAccumuloCell proc = new PutAccumuloCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAtrributeMapWithEL(tableName, row, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        runner.enqueue(content2.getBytes("UTF-8"), attributes1);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content1);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(tableName);
        assertEquals(2, puts.size());
        verifyPut(row, columnFamily, columnQualifier, content1, puts.get(0));
        verifyPut(row, columnFamily, columnQualifier, content2, puts.get(1));

        assertEquals(2, runner.getProvenanceEvents().size());
    }

    private Map<String, String> getAtrributeMapWithEL(String tableName, String row, String columnFamily, String columnQualifier) {
        final Map<String,String> attributes1 = new HashMap<>();
        attributes1.put("accumulo.tableName", tableName);
        attributes1.put("accumulo.row", row);
        attributes1.put("accumulo.columnFamily", columnFamily);
        attributes1.put("accumulo.columnQualifier", columnQualifier);
        return attributes1;
    }

    private TestRunner getTestRunnerWithEL(PutAccumuloCell proc) {
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutAccumuloCell.TABLE_NAME, "${accumulo.tableName}");
        runner.setProperty(PutAccumuloCell.ROW_ID, "${accumulo.row}");
        runner.setProperty(PutAccumuloCell.COLUMN_FAMILY, "${accumulo.columnFamily}");
        runner.setProperty(PutAccumuloCell.COLUMN_QUALIFIER, "${accumulo.columnQualifier}");
        return runner;
    }

    private MockAccumuloClientService getAccumuloClientService(TestRunner runner) throws InitializationException {
        final MockAccumuloClientService accumuloClient = new MockAccumuloClientService();
        runner.addControllerService("accumuloClient", accumuloClient);
        runner.enableControllerService(accumuloClient);
        runner.setProperty(PutAccumuloCell.ACCUMULO_CLIENT_SERVICE, "accumuloClient");
        return accumuloClient;
    }

    private void verifyPut(String row, String columnFamily, String columnQualifier, String content, MutationFlowFile put) {
        assertEquals(row, put.getRow());

        assertNotNull(put.getColumns());
        assertEquals(1, put.getColumns().size());

        final MutationColumn column = put.getColumns().iterator().next();
        assertEquals(columnFamily, column.getColumnFamily());
        assertEquals(columnQualifier, column.getColumnQualifier());
        assertEquals(content, new String(column.getBuffer(), StandardCharsets.UTF_8));
    }

}
