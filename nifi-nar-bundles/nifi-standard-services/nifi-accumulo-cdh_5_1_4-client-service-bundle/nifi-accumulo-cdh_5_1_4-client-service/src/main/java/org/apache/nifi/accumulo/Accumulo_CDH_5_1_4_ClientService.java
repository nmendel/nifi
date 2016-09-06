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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.accumulo.mutation.MutationColumn;
import org.apache.nifi.accumulo.mutation.MutationFlowFile;
import org.apache.nifi.accumulo.scan.Column;
import org.apache.nifi.accumulo.scan.ResultHandler;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.KerberosTicketRenewer;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;

@Tags({ "accumulo", "client"})
@CapabilityDescription("Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing " +
        "a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files " +
        "are provided, they will be loaded first, and the values of the additional properties will override the values from " +
        "the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase " +
        "configuration.")
@DynamicProperty(name="The name of an HBase configuration property.", value="The value of the given HBase configuration property.",
        description="These properties will be set on the HBase configuration after loading any provided configuration files.")
public class Accumulo_CDH_5_1_4_ClientService extends AbstractControllerService implements AccumuloClientService {

	 static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Accumulo Instance Name")
            .description("The name of the Accumulo Instance to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ZOOKEEPER_CONNECT_STRING = new PropertyDescriptor.Builder()
            .name("ZooKeeper Connection String")
            .description("A comma-separated list of ZooKeeper hostname:port pairs")
            .required(true)
            .defaultValue("localhost:2181")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to use when connecting to Accumulo")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to use when connecting to Accumulo")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
    static final long TICKET_RENEWAL_PERIOD = 60000;

    private volatile Connector connector;
    private volatile BatchWriterConfig writerConfig;
    private volatile UserGroupInformation ugi;
    private volatile KerberosTicketRenewer renewer;

    private List<PropertyDescriptor> properties;
    private KerberosProperties kerberosProperties;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        this.kerberosProperties = getKerberosProperties();

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(INSTANCE_NAME);
        props.add(ZOOKEEPER_CONNECT_STRING);
        props.add(USERNAME);
        props.add(PASSWORD);
        this.properties = Collections.unmodifiableList(props);
    }

    protected KerberosProperties getKerberosProperties() {
        return KerberosProperties.create(NiFiProperties.getInstance());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' in the Accumulo configuration.")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        this.connector = createConnection(context);
        this.writerConfig = getWriterConfig();

        // connection check
        if (this.connector != null) {
        	final NamespaceOperations admin = this.connector.namespaceOperations();
            if (admin != null) {
                admin.list();
            }

            // if we got here then we have a successful connection, so if we have a ugi then start a renewer
            if (ugi != null) {
                final String id = getClass().getSimpleName();
                renewer = SecurityUtil.startTicketRenewalThread(id, ugi, TICKET_RENEWAL_PERIOD, getLogger());
            }
        }
    }

    protected Connector createConnection(final ConfigurationContext context) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        final String configFiles = context.getProperty(HADOOP_CONF_FILES).getValue();
        final Configuration config = getConfigurationFromFiles(configFiles);

        // add any dynamic properties to the Accumulo configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
            	config.set(descriptor.getName(), entry.getValue());
            }
        }
        
        final String instanceName = context.getProperty(INSTANCE_NAME).getValue();
        final String zookeeperConnString = context.getProperty(ZOOKEEPER_CONNECT_STRING).getValue();
        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperConnString);

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        if (SecurityUtil.isSecurityEnabled(config)) {
            final String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
            final String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).getValue();

            getLogger().info("Accumulo Security Enabled, logging in as principal {} with keytab {}", new Object[] {principal, keyTab});
            ugi = SecurityUtil.loginKerberos(config, principal, keyTab);
            getLogger().info("Successfully logged in as principal {} with keytab {}", new Object[] {principal, keyTab});

            return ugi.doAs(new PrivilegedExceptionAction<Connector>() {
                @Override
                public Connector run() throws Exception {
                    return instance.getConnector(username, new PasswordToken(password));
                }
            });

        } else {
            getLogger().info("Simple Authentication");
            return instance.getConnector(username, new PasswordToken(password));
        }

    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = new Configuration();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }
    
    // TODO: from file?
    protected BatchWriterConfig getWriterConfig() {
    	BatchWriterConfig conf = new BatchWriterConfig();
    	conf.setMaxMemory(1000000L);
    	conf.setMaxWriteThreads(10);
    	conf.setMaxLatency(10,  TimeUnit.SECONDS);
    	conf.setTimeout(15, TimeUnit.SECONDS);
    	
    	return conf;
    }

    @OnDisabled
    public void shutdown() {
        if (renewer != null) {
            renewer.stop();
        }
    }

    @Override
    public void put(final String tableName, final Collection<MutationFlowFile> mutations)
    		throws IOException, TableNotFoundException, MutationsRejectedException {
    	
    	BatchWriter writer = connector.createBatchWriter(tableName, writerConfig);
        
    	try {
            // Create one Mutation per row per visibility string....
            final Map<String, Mutation> rowMutations = new HashMap<>();
            for (final MutationFlowFile mutationFlowFile : mutations) {
            	Mutation mutation = rowMutations.get(mutationFlowFile.getRow());
            	if (mutation == null) {
            		mutation = new Mutation(mutationFlowFile.getRow().getBytes(StandardCharsets.UTF_8));
            		rowMutations.put(mutationFlowFile.getRow(), mutation);
                }
                   	
            	for (final MutationColumn column : mutationFlowFile.getColumns()) {
            		mutation.put(
            				column.getColumnFamily().getBytes(StandardCharsets.UTF_8),
            				column.getColumnQualifier().getBytes(StandardCharsets.UTF_8),
            				new ColumnVisibility(column.getVisibility()),
            				column.getBuffer());
            	}
            }

            writer.addMutations(new ArrayList<>(rowMutations.values()));
        } finally {
        	writer.close();
        }
    }

    @Override
    public void put(final String tableName, final String rowId, final Collection<MutationColumn> columns)
    		throws IOException, TableNotFoundException, MutationsRejectedException {

    	BatchWriter writer = connector.createBatchWriter(tableName, writerConfig);
		
    	try {
			Mutation mutation = new Mutation(rowId.getBytes(StandardCharsets.UTF_8));
			for (final MutationColumn column : columns) {
				mutation.put(column.getColumnFamily().getBytes(StandardCharsets.UTF_8),
						column.getColumnQualifier().getBytes(StandardCharsets.UTF_8),
						new ColumnVisibility(column.getVisibility()), column.getBuffer());
			}
			writer.addMutation(mutation);
		} finally {
			writer.close();
		}
    }

    @Override
    public void scan(final String tableName, final Collection<Column> columns, final String filterExpression, final long minTime, final ResultHandler handler)
            throws IOException {

    }
    	/*
        Filter filter = null;
        if (!StringUtils.isBlank(filterExpression)) {
            ParseFilter parseFilter = new ParseFilter();
            filter = parseFilter.parseFilterString(filterExpression);
        }
        
        try {
        	
			final ResultScanner scanner = getResults(tableName, columns, "", filter, minTime);

            for (final Result result : scanner) {
                final byte[] rowKey = result.getRow();
                final Cell[] cells = result.rawCells();

                if (cells == null) {
                    continue;
                }

                // convert HBase cells to NiFi cells
                final ResultCell[] resultCells = new ResultCell[cells.length];

                for (int i=0; i < cells.length; i++) {
                    final Cell cell = cells[i];

                    final ResultCell resultCell = new ResultCell();
                    resultCell.setRowArray(cell.getRowArray());
                    resultCell.setRowOffset(cell.getRowOffset());
                    resultCell.setRowLength(cell.getRowLength());

                    resultCell.setFamilyArray(cell.getFamilyArray());
                    resultCell.setFamilyOffset(cell.getFamilyOffset());
                    resultCell.setFamilyLength(cell.getFamilyLength());

                    resultCell.setQualifierArray(cell.getQualifierArray());
                    resultCell.setQualifierOffset(cell.getQualifierOffset());
                    resultCell.setQualifierLength(cell.getQualifierLength());

                    resultCell.setTimestamp(cell.getTimestamp());
                    resultCell.setTypeByte(cell.getTypeByte());
                    resultCell.setSequenceId(cell.getSequenceId());

                    resultCell.setValueArray(cell.getValueArray());
                    resultCell.setValueOffset(cell.getValueOffset());
                    resultCell.setValueLength(cell.getValueLength());

                    resultCell.setTagsArray(cell.getTagsArray());
                    resultCell.setTagsOffset(cell.getTagsOffset());
                    resultCell.setTagsLength(cell.getTagsLength());

                    resultCells[i] = resultCell;
                }

                // delegate to the handler
                handler.handle(rowKey, resultCells);
            }
        }
    }

    // protected and extracted into separate method for testing
    protected Map.Entry<Key, Value> getResults(final String tableName, final Collection<Column> columns, final Filter filter, final long minTime) throws IOException {
        // Create a new scan. We will set the min timerange as the latest timestamp that
        // we have seen so far. The minimum timestamp is inclusive, so we will get duplicates.
        // We will record any cells that have the latest timestamp, so that when we scan again,
        // we know to throw away those duplicates.
    	final Scanner scan = connector.createScanner(tableName, new Authorizations(""));

        scan.setTimeRange(minTime, Long.MAX_VALUE);

        if (filter != null) {
            scan.setFilter(filter);
        }

        if (columns != null) {
            for (Column col : columns) {
                if (col.getQualifier() == null) {
                    scan.addFamily(col.getFamily());
                } else {
                    scan.addColumn(col.getFamily(), col.getQualifier());
                }
            }
        }

        return table.getScanner(scan);
    }
    	 */
}
