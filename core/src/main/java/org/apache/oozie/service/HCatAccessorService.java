/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.MappingRule;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

public class HCatAccessorService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HCatAccessorService.";
    public static final String JMS_CONNECTIONS_PROPERTIES = CONF_PREFIX + "jmsconnections";
    public static final String HCAT_CONFIGURATION = CONF_PREFIX + "hcat.configuration";

    private static XLog LOG;
    private static String DELIMITER = "#";
    private Configuration conf;
    private JMSAccessorService jmsService;
    private List<MappingRule> mappingRules;
    private JMSConnectionInfo defaultJMSConnInfo;
    private Configuration hcatConf;
    /**
     * Map of publisher(host:port) to JMS connection info
     */
    private Map<String, JMSConnectionInfo> publisherJMSConnInfoMap;
    /**
     * List of non publishers(host:port)
     */
    private Set<String> nonJMSPublishers;
    /**
     * Mapping of table to the topic name for the table
     */
    private Map<String, String> registeredTopicsMap;

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        this.jmsService = services.get(JMSAccessorService.class);
        initializeMappingRules();
        this.nonJMSPublishers = new HashSet<String>();
        this.publisherJMSConnInfoMap = new HashMap<String, JMSConnectionInfo>();
        this.registeredTopicsMap = new HashMap<String, String>();
        try {
            loadHCatConf(services);
        } catch(IOException ioe) {
            throw new ServiceException(ErrorCode.E0100, HCatAccessorService.class.getName(), "An exception occured while attempting"
                    + "to load the HCat Configuration", ioe);
        }
    }

    private void loadHCatConf(Services services) throws IOException {
        String path = conf.get(HCAT_CONFIGURATION);
        if (path != null) {
            if (path.startsWith("hdfs")) {
                Path p = new Path(path);
                HadoopAccessorService has = services.get(HadoopAccessorService.class);
                try {
                    FileSystem fs = has.createFileSystem(
                            System.getProperty("user.name"), p.toUri(), has.createJobConf(p.toUri().getAuthority()));
                    if (fs.exists(p)) {
                        FSDataInputStream is = null;
                        try {
                            is = fs.open(p);
                            hcatConf = new XConfiguration(is);
                        } finally {
                            if (is != null) {
                                is.close();
                            }
                        }
                        LOG.info("Loaded HCat Configuration: " + path);
                    } else {
                        LOG.warn("HCat Configuration could not be found at [" + path + "]");
                    }
                } catch (HadoopAccessorException hae) {
                    throw new IOException(hae);
                }
            } else {
                File f = new File(path);
                if (f.exists()) {
                    InputStream is = null;
                    try {
                        is = new FileInputStream(f);
                        hcatConf = new XConfiguration(is);
                    } finally {
                        if (is != null) {
                            is.close();
                        }
                    }
                    LOG.info("Loaded HCat Configuration: " + path);
                } else {
                    LOG.warn("HCat Configuration could not be found at [" + path + "]");
                }
            }
        }
        else {
            LOG.info("HCat Configuration not specified");
        }
    }

    public Configuration getHCatConf() {
        return hcatConf;
    }

    private void initializeMappingRules() {
        String[] connections = ConfigurationService.getStrings(conf, JMS_CONNECTIONS_PROPERTIES);
        if (connections != null) {
            mappingRules = new ArrayList<MappingRule>(connections.length);
            for (String connection : connections) {
                String[] values = connection.split("=", 2);
                String key = values[0].trim();
                String value = values[1].trim();
                if (key.equals("default")) {
                    defaultJMSConnInfo = new JMSConnectionInfo(value);
                }
                else {
                    mappingRules.add(new MappingRule(key, value));
                }
            }
        }
        else {
            LOG.warn("No JMS connection defined");
        }
    }

    /**
     * Determine whether a given source URI publishes JMS messages
     *
     * @param sourceURI URI of the publisher
     * @return true if we have JMS connection information for the source URI, else false
     */
    public boolean isKnownPublisher(URI sourceURI) {
        if (nonJMSPublishers.contains(sourceURI.getAuthority())) {
            return true;
        }
        else {
            JMSConnectionInfo connInfo = publisherJMSConnInfoMap.get(sourceURI.getAuthority());
            return connInfo == null ? (getJMSConnectionInfo(sourceURI) != null) : true;
        }
    }

    /**
     * Given a publisher host:port return the connection details of JMS server that the publisher
     * publishes to
     *
     * @param publisherURI URI of the publisher
     * @return JMSConnectionInfo to connect to the JMS server that the publisher publishes to
     */
    public JMSConnectionInfo getJMSConnectionInfo(URI publisherURI) {
        String publisherAuthority = publisherURI.getAuthority();
        JMSConnectionInfo connInfo = null;
        if (publisherJMSConnInfoMap.containsKey(publisherAuthority)) {
            connInfo = publisherJMSConnInfoMap.get(publisherAuthority);
        }
        else {
            String schemeWithAuthority = publisherURI.getScheme() + "://" + publisherAuthority;
            for (MappingRule mr : mappingRules) {
                String jndiPropertiesString = mr.applyRule(schemeWithAuthority);
                if (jndiPropertiesString != null) {
                    connInfo = new JMSConnectionInfo(jndiPropertiesString);
                    publisherJMSConnInfoMap.put(publisherAuthority, connInfo);
                    LOG.info("Adding hcat server [{0}] to the list of JMS publishers", schemeWithAuthority);
                    break;
                }
            }
            if (connInfo == null && defaultJMSConnInfo != null) {
                connInfo = defaultJMSConnInfo;
                publisherJMSConnInfoMap.put(publisherAuthority, defaultJMSConnInfo);
                LOG.info("Adding hcat server [{0}] to the list of JMS publishers", schemeWithAuthority);
            }
            else {
                nonJMSPublishers.add(publisherAuthority);
                LOG.info("Adding hcat server [{0}] to the list of non JMS publishers", schemeWithAuthority);
            }

        }
        return connInfo;
    }

    /**
     * Check if we are already listening to the JMS topic for the table in the given hcatURI
     *
     * @param hcatURI hcatalog partition URI
     * @return true if registered to a JMS topic for the table in the given hcatURI
     */
    public boolean isRegisteredForNotification(HCatURI hcatURI) {
        return registeredTopicsMap.containsKey(getKeyForRegisteredTopicsMap(hcatURI));
    }

    /**
     * Register for notifications on a JMS topic for the specified hcatalog table.
     *
     * @param hcatURI hcatalog partition URI
     * @param topic JMS topic to register to
     * @param msgHandler Handler which will process the messages received on the topic
     */
    public void registerForNotification(HCatURI hcatURI, String topic, HCatMessageHandler msgHandler) {
        JMSConnectionInfo connInfo = getJMSConnectionInfo(hcatURI.getURI());
        jmsService.registerForNotification(connInfo, topic, msgHandler);
        registeredTopicsMap.put(
                getKeyForRegisteredTopicsMap(hcatURI), topic);
    }

    public void unregisterFromNotification(HCatURI hcatURI) {
        String topic = registeredTopicsMap.remove(getKeyForRegisteredTopicsMap(hcatURI));
        if (topic != null) {
            JMSConnectionInfo connInfo = getJMSConnectionInfo(hcatURI.getURI());
            jmsService.unregisterFromNotification(connInfo, topic);
        }
    }

    public void unregisterFromNotification(String server, String database, String table) {
        String key = server + DELIMITER + database + DELIMITER + table;
        String topic = registeredTopicsMap.remove(key);
        if (topic != null) {
            try {
                JMSConnectionInfo connInfo = getJMSConnectionInfo(new URI("hcat://" + server));
                jmsService.unregisterFromNotification(connInfo, topic);
            }
            catch (URISyntaxException e) {
                LOG.warn("Error unregistering from notification for topic [{0}]. Hcat table=[{1}]", topic, key, e);
            }
        }
    }

    private String getKeyForRegisteredTopicsMap(HCatURI hcatURI) {
        return hcatURI.getURI().getAuthority() + DELIMITER + hcatURI.getDb()
                + DELIMITER + hcatURI.getTable();
    }

    @Override
    public void destroy() {
        publisherJMSConnInfoMap.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return HCatAccessorService.class;
    }

}
