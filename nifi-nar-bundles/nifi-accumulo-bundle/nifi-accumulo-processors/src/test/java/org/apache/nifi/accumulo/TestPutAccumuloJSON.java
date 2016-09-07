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

import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
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

public class TestPutAccumuloJSON {

    public static final String DEFAULT_TABLE_NAME = "nifi";
    public static final String DEFAULT_ROW = "row1";
    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    @Test
    public void testCustomValidate() throws InitializationException {
        // missing row id and row id field name should be invalid
        TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getAccumuloClientService(runner);
        runner.assertNotValid();

        // setting both properties should still be invalid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, "rowId");
        runner.setProperty(PutAccumuloJSON.ROW_FIELD_NAME, "rowFieldName");
        runner.assertNotValid();

        // only a row id field name should make it valid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_FIELD_NAME, "rowFieldName");
        runner.assertValid();

        // only a row id should make it valid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, "rowId");
        runner.assertValid();
    }

    @Test
    public void testSingleJsonDocAndProvidedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", "value1");
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals("accumulo://" + DEFAULT_TABLE_NAME + "/" + DEFAULT_ROW, event.getTransitUri());
    }

    @Test
    public void testSingJsonDocAndExtractedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_FIELD_NAME, "rowField");

        final String content = "{ \"rowField\" : \"myRowId\", \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should be a put with row id of myRowId, and rowField shouldn't end up in the columns
        final Map<String,String> expectedColumns1 = new HashMap<>();
        expectedColumns1.put("field1", "value1");
        expectedColumns1.put("field2", "value2");
        AccumuloTestUtil.verifyPut("myRowId", DEFAULT_COLUMN_FAMILY, expectedColumns1, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        AccumuloTestUtil.verifyEvent(runner.getProvenanceEvents(), "accumulo://" + DEFAULT_TABLE_NAME + "/myRowId", ProvenanceEventType.SEND);
    }

    @Test
    public void testSingJsonDocAndExtractedRowIdMissingField() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_FIELD_NAME, "rowField");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, accumuloClient.getFlowFilePuts().size());
    }

    @Test
    public void testMultipleJsonDocsRouteToFailure() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);

        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        final String content2 = "{ \"field3\" : \"value3\", \"field4\" : \"value4\" }";
        final String content = "[ " + content1 + " , " + content2 + " ]";

        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, accumuloClient.getFlowFilePuts().size());
    }

    @Test
    public void testELWithProvidedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner("${accumulo.table}", "${accumulo.colFamily}", "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, "${accumulo.rowId}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("accumulo.table", "myTable");
        attributes.put("accumulo.colFamily", "myColFamily");
        attributes.put("accumulo.rowId", "myRowId");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get("myTable");
        assertEquals(1, puts.size());

        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", "value1");
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut("myRowId", "myColFamily", expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        AccumuloTestUtil.verifyEvent(runner.getProvenanceEvents(), "accumulo://myTable/myRowId", ProvenanceEventType.SEND);
    }

    @Test
    public void testELWithExtractedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner("${accumulo.table}", "${accumulo.colFamily}", "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_FIELD_NAME, "${accumulo.rowField}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("accumulo.table", "myTable");
        attributes.put("accumulo.colFamily", "myColFamily");
        attributes.put("accumulo.rowField", "field1");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get("myTable");
        assertEquals(1, puts.size());

        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut("value1", "myColFamily", expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        AccumuloTestUtil.verifyEvent(runner.getProvenanceEvents(), "accumulo://myTable/value1", ProvenanceEventType.SEND);
    }

    @Test
    public void testNullAndArrayElementsWithWarnStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_WARN.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNullAndArrayElementsWithIgnoreStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_IGNORE.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNullAndArrayElementsWithFailureStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_FAIL.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, accumuloClient.getFlowFilePuts().size());
    }

    @Test
    public void testNullAndArrayElementsWithTextStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_TEXT.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", "[{\"child_field1\":\"child_value1\"}]");
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNestedDocWithTextStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_TEXT.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : { \"child_field1\" : \"child_value1\" }, \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_SUCCESS);

        assertNotNull(accumuloClient.getFlowFilePuts());
        assertEquals(1, accumuloClient.getFlowFilePuts().size());

        final List<MutationFlowFile> puts = accumuloClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,String> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", "{\"child_field1\":\"child_value1\"}");
        expectedColumns.put("field2", "value2");
        AccumuloTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testAllElementsAreNullOrArrays() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockAccumuloClientService accumuloClient = getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutAccumuloJSON.COMPLEX_FIELD_STRATEGY, PutAccumuloJSON.COMPLEX_FIELD_WARN.getValue());

        // should route to failure since it would produce a put with no columns
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }],  \"field2\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutAccumuloCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, accumuloClient.getFlowFilePuts().size());
    }

    @Test
    public void testInvalidJson() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getAccumuloClientService(runner);
        runner.setProperty(PutAccumuloJSON.ROW_ID, DEFAULT_ROW);

        final String content = "NOT JSON";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutAccumuloCell.REL_FAILURE, 1);
    }

    private TestRunner getTestRunner(String table, String columnFamily, String batchSize) {
        final TestRunner runner = TestRunners.newTestRunner(PutAccumuloJSON.class);
        runner.setProperty(PutAccumuloJSON.TABLE_NAME, table);
        runner.setProperty(PutAccumuloJSON.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutAccumuloJSON.BATCH_SIZE, batchSize);
        return runner;
    }

    private MockAccumuloClientService getAccumuloClientService(final TestRunner runner) throws InitializationException {
        final MockAccumuloClientService accumuloClient = new MockAccumuloClientService();
        runner.addControllerService("accumuloClient", accumuloClient);
        runner.enableControllerService(accumuloClient);
        runner.setProperty(PutAccumuloCell.ACCUMULO_CLIENT_SERVICE, "accumuloClient");
        return accumuloClient;
    }

}
