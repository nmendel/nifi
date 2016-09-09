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
/*
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
*/
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.nifi.accumulo.mutation.MutationColumn;
import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.accumulo.scan.Column;
import org.apache.nifi.accumulo.scan.ResultCell;
import org.apache.nifi.accumulo.scan.ResultHandler;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import jline.internal.Log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAccumulo_CDH_5_1_4_ClientService {

    private KerberosProperties kerberosPropsWithFile;
    private KerberosProperties kerberosPropsWithoutFile;
    private Connector connector;

    @Before
    public void setup() throws AccumuloException, AccumuloSecurityException {
        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        NiFiProperties niFiPropertiesWithKerberos = Mockito.mock(NiFiProperties.class);
        when(niFiPropertiesWithKerberos.getKerberosConfigurationFile()).thenReturn(new File("src/test/resources/krb5.conf"));
        kerberosPropsWithFile = KerberosProperties.create(niFiPropertiesWithKerberos);

        NiFiProperties niFiPropertiesWithoutKerberos = Mockito.mock(NiFiProperties.class);
        when(niFiPropertiesWithKerberos.getKerberosConfigurationFile()).thenReturn(null);
        kerberosPropsWithoutFile = KerberosProperties.create(niFiPropertiesWithoutKerberos);
        
        
        final String instanceName = "";
        final String zookeeperConnString = "";
        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperConnString);

        final String username = "";
        final String password = "";

        connector = instance.getConnector(username, new PasswordToken(password));
    }

    @Test
    public void testCustomValidate() throws InitializationException, IOException, TableNotFoundException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        final String tableName = "nifi";
        
        // no conf file or zk properties so should be invalid
        MockAccumuloClientService service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.enableControllerService(service);

        runner.assertNotValid(service);
        runner.removeControllerService(service);

        // conf file with no zk properties should be valid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES, "src/test/resources/accumulo-site.xml");
        runner.enableControllerService(service);

        runner.assertValid(service);
        runner.removeControllerService(service);

        // only quorum and no conf file should be invalid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_QUORUM, "localhost");
        runner.enableControllerService(service);

        runner.assertNotValid(service);
        runner.removeControllerService(service);

        // quorum and port, no znode, no conf file, should be invalid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_QUORUM, "localhost");
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_CLIENT_PORT, "2181");
        runner.enableControllerService(service);

        runner.assertNotValid(service);
        runner.removeControllerService(service);

        // quorum, port, and znode, no conf file, should be valid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_QUORUM, "localhost");
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_CLIENT_PORT, "2181");
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_ZNODE_PARENT, "/accumulo");
        runner.enableControllerService(service);

        runner.assertValid(service);
        runner.removeControllerService(service);

        // quorum and port with conf file should be valid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES, "src/test/resources/accumulo-site.xml");
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_QUORUM, "localhost");
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.ZOOKEEPER_CLIENT_PORT, "2181");
        runner.enableControllerService(service);

        runner.assertValid(service);
        runner.removeControllerService(service);

        // Kerberos - principal with non-set keytab and only accumulo-site-security - valid because we need core-site-security to turn on security
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES, "src/test/resources/accumulo-site-security.xml");
        runner.setProperty(service, kerberosPropsWithFile.getKerberosPrincipal(), "test@REALM");
        runner.enableControllerService(service);
        runner.assertValid(service);

        // Kerberos - principal with non-set keytab and both config files
        runner.disableControllerService(service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES,
                "src/test/resources/accumulo-site-security.xml, src/test/resources/core-site-security.xml");
        runner.enableControllerService(service);
        runner.assertNotValid(service);

        // Kerberos - add valid options
        runner.disableControllerService(service);
        runner.setProperty(service, kerberosPropsWithFile.getKerberosKeytab(), "src/test/resources/fake.keytab");
        runner.setProperty(service, kerberosPropsWithFile.getKerberosPrincipal(), "test@REALM");
        runner.enableControllerService(service);
        runner.assertValid(service);

        // Kerberos - add invalid non-existent keytab file
        runner.disableControllerService(service);
        runner.setProperty(service, kerberosPropsWithFile.getKerberosKeytab(), "src/test/resources/missing.keytab");
        runner.enableControllerService(service);
        runner.assertNotValid(service);

        // Kerberos - add invalid principal
        runner.disableControllerService(service);
        runner.setProperty(service, kerberosPropsWithFile.getKerberosKeytab(), "src/test/resources/fake.keytab");
        runner.setProperty(service, kerberosPropsWithFile.getKerberosPrincipal(), "");
        runner.enableControllerService(service);
        runner.assertNotValid(service);

        // Kerberos - valid props but the KerberosProperties has a null Kerberos config file so be invalid
        service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithoutFile);
        runner.addControllerService("AccumuloClientService", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES,
                "src/test/resources/accumulo-site-security.xml, src/test/resources/core-site-security.xml");
        runner.setProperty(service, kerberosPropsWithoutFile.getKerberosKeytab(), "src/test/resources/fake.keytab");
        runner.setProperty(service, kerberosPropsWithoutFile.getKerberosPrincipal(), "test@REALM");
        runner.enableControllerService(service);
        runner.assertNotValid(service);
    }

    @Test
    public void testSinglePut() throws InitializationException, IOException, MutationsRejectedException, TableNotFoundException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final String content = "content1";

        final Collection<MutationColumn> columns = Collections.singletonList(new MutationColumn(columnFamily, columnQualifier,
                content.getBytes(StandardCharsets.UTF_8)));
        final MutationFlowFile putFlowFile = new MutationFlowFile(tableName, row, columns, null);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final AccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // try to put a single cell
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        accumuloClientService.put(tableName, Arrays.asList(putFlowFile));

        // verify only one call to put was made
        /* TODO
        ArgumentCaptor<List> capture = ArgumentCaptor.forClass(List.class);
        verify(table, times(1)).put(capture.capture());

        // verify only one put was in the list of puts
        final List<Mutation> puts = capture.getValue();
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, content, puts.get(0));
        */
    }

    @Test
    public void testMultiplePutsSameRow() throws IOException, InitializationException, MutationsRejectedException, TableNotFoundException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final String content1 = "content1";
        final String content2 = "content2";

        final Collection<MutationColumn> columns1 = Collections.singletonList(new MutationColumn(columnFamily, columnQualifier,
                content1.getBytes(StandardCharsets.UTF_8)));
        final MutationFlowFile putFlowFile1 = new MutationFlowFile(tableName, row, columns1, null);

        final Collection<MutationColumn> columns2 = Collections.singletonList(new MutationColumn(columnFamily, columnQualifier,
                content2.getBytes(StandardCharsets.UTF_8)));
        final MutationFlowFile putFlowFile2 = new MutationFlowFile(tableName, row, columns2, null);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final AccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // try to put a multiple cells for the same row
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        accumuloClientService.put(tableName, Arrays.asList(putFlowFile1, putFlowFile2));

        /* TODO
        // verify put was only called once
        ArgumentCaptor<List> capture = ArgumentCaptor.forClass(List.class);
        verify(table, times(1)).put(capture.capture());

        // verify there was only one put in the list of puts
        final List<Mutation> puts = capture.getValue();
        assertEquals(1, puts.size());

        // verify two cells were added to this one put operation
        final List<ColumnUpdate> familyCells = puts.get(0).getUpdates();
        assertEquals(2, familyCells.size());
        */
    }

    @Test
    public void testMultiplePutsDifferentRow() throws IOException, InitializationException, MutationsRejectedException, TableNotFoundException {
        final String tableName = "nifi";
        final String row1 = "row1";
        final String row2 = "row2";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final String content1 = "content1";
        final String content2 = "content2";

        final Collection<MutationColumn> columns1 = Collections.singletonList(new MutationColumn(columnFamily, columnQualifier,
                content1.getBytes(StandardCharsets.UTF_8)));
        final MutationFlowFile putFlowFile1 = new MutationFlowFile(tableName, row1, columns1, null);

        final Collection<MutationColumn> columns2 = Collections.singletonList(new MutationColumn(columnFamily, columnQualifier,
                content2.getBytes(StandardCharsets.UTF_8)));
        final MutationFlowFile putFlowFile2 = new MutationFlowFile(tableName, row2, columns2, null);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final AccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // try to put a multiple cells with different rows
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        accumuloClientService.put(tableName, Arrays.asList(putFlowFile1, putFlowFile2));

        // verify put was only called once
        /* TODO
        ArgumentCaptor<List> capture = ArgumentCaptor.forClass(List.class);
        verify(table, times(1)).put(capture.capture());

        // verify there were two puts in the list
        final List<Mutation> puts = capture.getValue();
        assertEquals(2, puts.size());
        */
    }

    @Test
    public void testScan() throws InitializationException, IOException, TableNotFoundException {
        final String tableName = "nifi";
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final MockAccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // stage some results in the mock service...
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        service.addResult("row0", cells, now - 2);
        service.addResult("row1", cells, now - 1);
        service.addResult("row2", cells, now - 1);
        service.addResult("row3", cells, now);

        // perform a scan and verify the four rows were returned
        final CollectingResultHandler handler = new CollectingResultHandler();
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        accumuloClientService.scan(tableName, new ArrayList<Column>(), null, now, handler);
        assertEquals(4, handler.results.size());

        // get row0 using the row id and verify it has 2 cells
        final ResultCell[] results = handler.results.get("row0");
        assertNotNull(results);
        assertEquals(2, results.length);

        verifyResultCell(results[0], "nifi", "greeting", "hello");
        verifyResultCell(results[1], "nifi", "name", "nifi");
    }

    @Test
    public void testScanWithValidFilter() throws InitializationException, IOException, TableNotFoundException {
        final String tableName = "nifi";
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final MockAccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // perform a scan and verify the four rows were returned
        final CollectingResultHandler handler = new CollectingResultHandler();
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        // make sure we parse the filter expression without throwing an exception
        final String filter = "PrefixFilter ('Row') AND PageFilter (1) AND FirstKeyOnlyFilter ()";
        accumuloClientService.scan(tableName, new ArrayList<Column>(), filter, System.currentTimeMillis(), handler);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testScanWithInvalidFilter() throws InitializationException, IOException, TableNotFoundException {
        final String tableName = "nifi";
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final MockAccumuloClientService service = configureAccumuloClientService(runner, tableName);
        runner.assertValid(service);

        // perform a scan and verify the four rows were returned
        final CollectingResultHandler handler = new CollectingResultHandler();
        final AccumuloClientService accumuloClientService = runner.getProcessContext().getProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE)
                .asControllerService(AccumuloClientService.class);

        // this should throw IllegalArgumentException
        final String filter = "this is not a filter";
        accumuloClientService.scan(tableName, new ArrayList<Column>(), filter, System.currentTimeMillis(), handler);
    }

    private MockAccumuloClientService configureAccumuloClientService(final TestRunner runner, final String tableName) throws InitializationException, TableNotFoundException {
        final MockAccumuloClientService service = new MockAccumuloClientService(connector, tableName, kerberosPropsWithFile);
        runner.addControllerService("accumuloClient", service);
        runner.setProperty(service, Accumulo_CDH_5_1_4_ClientService.HADOOP_CONF_FILES, "src/test/resources/accumulo-site.xml");
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.ACCUMULO_CLIENT_SERVICE, "accumuloClient");
        return service;
    }

    private void verifyResultCell(final ResultCell result, final String cf, final String cq, final String val) {
        final String colFamily = new String(result.getFamilyArray(), result.getFamilyOffset(), result.getFamilyLength());
        assertEquals(cf, colFamily);

        final String colQualifier = new String(result.getQualifierArray(), result.getQualifierOffset(), result.getQualifierLength());
        assertEquals(cq, colQualifier);

        final String value = new String(result.getValueArray(), result.getValueOffset(), result.getValueLength());
        assertEquals(val, value);
    }

    private void verifyPut(String row, String columnFamily, String columnQualifier, String content, Mutation put) {
        assertEquals(row, new String(put.getRow()));

        List<ColumnUpdate> updates = put.getUpdates();
        assertEquals(1, updates.size());

        ColumnUpdate entry = updates.get(0);
        assertEquals(columnFamily, new String(entry.getColumnFamily()));
        assert(entry.getValue().length > 0);

        assertEquals(columnQualifier, new String(entry.getColumnQualifier()));
        assertEquals(content, new String(entry.getValue()));
    }

    // Override methods to create a mock service that can return staged data
    private class MockAccumuloClientService extends Accumulo_CDH_5_1_4_ClientService {

        private String tableName;
        private BatchWriter batchWriter;
        private Scanner scanner;
        private BatchWriterConfig writerConfig;
        private List<Entry<Key, Value>> results = new ArrayList<>();
        private KerberosProperties kerberosProperties;

        public MockAccumuloClientService(final Connector connector, final String tableName, final KerberosProperties kerberosProperties) throws TableNotFoundException {
            this.tableName = tableName;
            this.batchWriter = connector.createBatchWriter(tableName, writerConfig);
            this.scanner = connector.createScanner(tableName, new Authorizations());
            this.kerberosProperties = kerberosProperties;
            
            BatchWriterConfig conf = new BatchWriterConfig();
        	conf.setMaxMemory(10000L);
        	conf.setMaxWriteThreads(2);
        	conf.setMaxLatency(10,  TimeUnit.SECONDS);
        	conf.setTimeout(10, TimeUnit.SECONDS);
        	this.writerConfig = conf;
        	
        }

        @Override
        protected KerberosProperties getKerberosProperties() {
            return kerberosProperties;
        }

        protected void setKerberosProperties(KerberosProperties properties) {
            this.kerberosProperties = properties;
        }

        public void addResult(final String rowKey, final Map<String, String> cells, final long timestamp) {
            final byte[] rowArray = rowKey.getBytes(StandardCharsets.UTF_8);

            final ResultCell[] cellArray = new ResultCell[cells.size()];
            int i = 0;
            for (final Map.Entry<String, String> cellEntry : cells.entrySet()) {
                final ResultCell cell = Mockito.mock(ResultCell.class);
                
                when(cell.getRowArray()).thenReturn(rowArray);
                when(cell.getRowOffset()).thenReturn(0);
                when(cell.getRowLength()).thenReturn((short) rowArray.length);

                final String cellValue = cellEntry.getValue();
                final byte[] valueArray = cellValue.getBytes(StandardCharsets.UTF_8);
                when(cell.getValueArray()).thenReturn(valueArray);
                when(cell.getValueOffset()).thenReturn(0);
                when(cell.getValueLength()).thenReturn(valueArray.length);

                final byte[] familyArray = "nifi".getBytes(StandardCharsets.UTF_8);
                when(cell.getFamilyArray()).thenReturn(familyArray);
                when(cell.getFamilyOffset()).thenReturn(0);
                when(cell.getFamilyLength()).thenReturn((byte) familyArray.length);

                final String qualifier = cellEntry.getKey();
                final byte[] qualifierArray = qualifier.getBytes(StandardCharsets.UTF_8);
                when(cell.getQualifierArray()).thenReturn(qualifierArray);
                when(cell.getQualifierOffset()).thenReturn(0);
                when(cell.getQualifierLength()).thenReturn(qualifierArray.length);

                when(cell.getTimestamp()).thenReturn(timestamp);

                cellArray[i++] = cell;
            }

            final Entry<Key, Value> result = Mockito.mock(Entry.class);
            when(result.getKey().getRow().getBytes()).thenReturn(rowArray);
            // TODO: when(result.getValue().get()).thenReturn(cellArray);
            results.add(result);
        }

        @Override
        protected Scanner getResults(String tableName, Collection<Column> columns, IteratorSetting filter, long minTime) throws IOException {
            final Scanner scanner = Mockito.mock(Scanner.class);
            Mockito.when(scanner.iterator()).thenReturn(results.iterator());
            return scanner;
        }

        @Override
        protected Connector createConnection(ConfigurationContext context) throws IOException {
        	Connector connector = Mockito.mock(Connector.class);
        	
        	try {
        		Mockito.when(connector.createBatchWriter(tableName, writerConfig)).thenReturn(batchWriter);
        		Mockito.when(connector.createScanner(tableName, new Authorizations())).thenReturn(scanner);
        	} catch(TableNotFoundException e) {
        		Log.error(String.format("Could not find table %s, aborting", tableName));
        		System.exit(1);
        	}
            return connector;
        }
    }

    // handler that saves results for verification
    private static final class CollectingResultHandler implements ResultHandler {

        Map<String,ResultCell[]> results = new LinkedHashMap<>();

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            final String rowStr = new String(row, StandardCharsets.UTF_8);
            results.put(rowStr, resultCells);
        }
    }

}
