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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.accumulo.GetAccumulo.ScanResult;
import org.apache.nifi.accumulo.scan.Column;
import org.apache.nifi.accumulo.util.StringSerDe;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGetAccumulo {

    private TestRunner runner;
    private MockGetAccumulo proc;
    private MockCacheClient cacheClient;
    private MockAccumuloClientService accumuloClient;

    @Before
    public void setup() throws InitializationException {
        proc = new MockGetAccumulo();
        runner = TestRunners.newTestRunner(proc);

        cacheClient = new MockCacheClient();
        runner.addControllerService("cacheClient", cacheClient);
        runner.enableControllerService(cacheClient);

        accumuloClient = new MockAccumuloClientService();
        runner.addControllerService("accumuloClient", accumuloClient);
        runner.enableControllerService(accumuloClient);

        runner.setProperty(GetAccumulo.TABLE_NAME, "nifi");
        runner.setProperty(GetAccumulo.DISTRIBUTED_CACHE_SERVICE, "cacheClient");
        runner.setProperty(GetAccumulo.ACCUMULO_CLIENT_SERVICE, "accumuloClient");
    }

    @After
    public void cleanup() {
        final File file = proc.getStateFile();
        if (file.exists()) {
            file.delete();
        }
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testColumnsValidation() {
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1:cq1");
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1");
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1:cq1,cf2:cq2,cf3:cq3");
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1,cf2:cq1,cf3");
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1 cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1:,cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(GetAccumulo.COLUMNS, "cf1:cq1,");
        runner.assertNotValid();
    }

    @Test
    public void testRowCounts() {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        accumuloClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 5);
    }

    @Test
    public void testPersistAndRecoverFromLocalState() throws InitializationException {
        final File stateFile = new File("target/test-recover-state.bin");
        if (!stateFile.delete() && stateFile.exists()) {
            Assert.fail("Could not delete state file " + stateFile);
        }
        proc.setStateFile(stateFile);

        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        accumuloClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 5);
        runner.clearTransferState();

        proc = new MockGetAccumulo(stateFile);

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 0);
    }

    @Test
    public void testBecomePrimaryWithNoLocalState() throws InitializationException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        accumuloClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 5);

        // delete the processor's local state to simulate becoming the primary node
        // for the first time, should use the state from distributed cache
        final File stateFile = proc.getStateFile();
        if (!stateFile.delete() && stateFile.exists()) {
            Assert.fail("Could not delete state file " + stateFile);
        }
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);
        accumuloClient.addResult("row4", cells, now + 1);

        runner.clearTransferState();
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 0);
    }

    @Test
    public void testBecomePrimaryWithNewerLocalState() throws InitializationException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        // trick for testing so that row4 gets written to local state but not to the real cache
        final MockCacheClient otherCacheClient = new MockCacheClient();
        runner.addControllerService("otherCacheClient", otherCacheClient);
        runner.enableControllerService(otherCacheClient);
        runner.setProperty(GetAccumulo.DISTRIBUTED_CACHE_SERVICE, "otherCacheClient");

        accumuloClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 5);

        // set back the original cache cacheClient which is missing row4
        runner.setProperty(GetAccumulo.DISTRIBUTED_CACHE_SERVICE, "cacheClient");

        // become the primary node, but we have existing local state with rows 0-4
        // so we shouldn't get any output because we should use the local state
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);
        accumuloClient.addResult("row4", cells, now + 1);

        runner.clearTransferState();
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 0);
    }

    @Test
    public void testOnRemovedClearsState() throws IOException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        // should have a local state file and a cache entry before removing
        runner.getStateManager().assertStateSet(Scope.CLUSTER);

        proc.onRemoved(runner.getProcessContext());

        // onRemoved should have cleared both
        Assert.assertFalse(proc.getStateFile().exists());
        Assert.assertFalse(cacheClient.containsKey(proc.getKey(), new StringSerDe()));
    }

    @Test
    public void testChangeTableNameClearsState() {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);

        // change the table name and run again, should get all the data coming out
        // again because previous state will be wiped
        runner.setProperty(GetAccumulo.TABLE_NAME, "otherTable");

        accumuloClient.addResult("row0", cells, now - 2);
        accumuloClient.addResult("row1", cells, now - 1);
        accumuloClient.addResult("row2", cells, now - 1);
        accumuloClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 4);
    }

    @Test
    public void testInitialTimeCurrentTime() {
        runner.setProperty(GetAccumulo.INITIAL_TIMERANGE, GetAccumulo.CURRENT_TIME);

        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        accumuloClient.addResult("row0", cells, now - 4000);
        accumuloClient.addResult("row1", cells, now - 3000);
        accumuloClient.addResult("row2", cells, now - 2000);
        accumuloClient.addResult("row3", cells, now - 1000);

        // should not get any output because the mock results have a time before current time
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetAccumulo.REL_SUCCESS, 0);
    }

    @Test
    public void testParseColumns() throws IOException {
        runner.setProperty(GetAccumulo.COLUMNS, "cf1,cf2:cq1,cf3");
        proc.parseColumns(runner.getProcessContext());

        final List<Column> expectedCols = new ArrayList<>();
        expectedCols.add(new Column("cf1".getBytes(Charset.forName("UTF-8")), null));
        expectedCols.add(new Column("cf2".getBytes(Charset.forName("UTF-8")), "cq1".getBytes(Charset.forName("UTF-8"))));
        expectedCols.add(new Column("cf3".getBytes(Charset.forName("UTF-8")), null));

        final List<Column> actualColumns = proc.getColumns();
        Assert.assertNotNull(actualColumns);
        Assert.assertEquals(expectedCols.size(), actualColumns.size());

        for (final Column expectedCol : expectedCols) {
            boolean found = false;
            for (final Column providedCol : actualColumns) {
                if (expectedCol.equals(providedCol)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Didn't find expected column", found);
        }
    }

    @Test
    public void testCustomValidate() throws CharacterCodingException {
        runner.setProperty(GetAccumulo.FILTER_EXPRESSION, "PrefixFilter ('Row') AND PageFilter (1) AND FirstKeyOnlyFilter ()");
        runner.assertValid();

        runner.setProperty(GetAccumulo.COLUMNS, "colA");
        runner.assertNotValid();
    }


    @Test
    public void testScanResultConvert() {
        final long timestamp = 14L;
        final Map<String, Set<String>> cellHashes = new LinkedHashMap<>();

        final Set<String> row1Cells = new HashSet<>();
        row1Cells.add("hello");
        row1Cells.add("there");
        cellHashes.put("abc", row1Cells);

        final Set<String> row2Cells = new HashSet<>();
        row2Cells.add("good-bye");
        row2Cells.add("there");
        cellHashes.put("xyz", row2Cells);

        final ScanResult scanResult = new GetAccumulo.ScanResult(timestamp, cellHashes);

        final Map<String, String> flatMap = scanResult.toFlatMap();
        assertEquals(7, flatMap.size());
        assertEquals("abc", flatMap.get("row.0"));

        final String row0Cell0 = flatMap.get("row.0.0");
        final String row0Cell1 = flatMap.get("row.0.1");
        assertTrue(row0Cell0.equals("hello") || row0Cell0.equals("there"));
        assertTrue(row0Cell1.equals("hello") || row0Cell1.equals("there"));
        assertNotSame(row0Cell0, row0Cell1);

        assertEquals("xyz", flatMap.get("row.1"));
        final String row1Cell0 = flatMap.get("row.1.0");
        final String row1Cell1 = flatMap.get("row.1.1");
        assertTrue(row1Cell0.equals("good-bye") || row1Cell0.equals("there"));
        assertTrue(row1Cell1.equals("good-bye") || row1Cell1.equals("there"));
        assertNotSame(row1Cell0, row1Cell1);

        final ScanResult reverted = ScanResult.fromFlatMap(flatMap);
        assertEquals(timestamp, reverted.getTimestamp());
        assertEquals(cellHashes, reverted.getMatchingCells());
    }


    // Mock processor to override the location of the state file
    private static class MockGetAccumulo extends GetAccumulo {

        private static final String DEFAULT_STATE_FILE_NAME = "target/TestGetAccumulo.bin";

        private File stateFile;

        public MockGetAccumulo() {
            this(new File(DEFAULT_STATE_FILE_NAME));
        }

        public MockGetAccumulo(final File stateFile) {
            this.stateFile = stateFile;
        }

        public void setStateFile(final File stateFile) {
            this.stateFile = stateFile;
        }

        @Override
        protected int getBatchSize() {
            return 2;
        }

        @Override
        protected File getStateDir() {
            return new File("target");
        }

        @Override
        protected File getStateFile() {
            return stateFile;
        }
    }

    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        private void verifyNotFail() throws IOException {
            if ( failOnCalls ) {
                throw new IOException("Could not call to remote cacheClient because Unit Test marked cacheClient unavailable");
            }
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            final Object retValue = values.putIfAbsent(key, value);
            return (retValue == null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                                          final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key, value);
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key);
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            values.remove(key);
            return true;
        }
    }

}
