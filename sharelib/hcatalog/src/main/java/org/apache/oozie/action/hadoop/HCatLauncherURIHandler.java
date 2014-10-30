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

package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.api.ConnectionFailureException;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.oozie.util.HCatURI;

public class HCatLauncherURIHandler implements LauncherURIHandler {

    private static List<Class<?>> classesToShip = new ArrayList<Class<?>>();

    static {
        classesToShip.add(HCatURI.class);
    }

    @Override
    public boolean create(URI uri, Configuration conf) throws LauncherException {
        throw new UnsupportedOperationException("Creation of partition is not supported for " + uri);
    }

    @Override
    public boolean delete(URI uri, Configuration conf) throws LauncherException {
        HCatClient client = getHCatClient(uri, conf);
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            client.dropPartitions(hcatURI.getDb(), hcatURI.getTable(), hcatURI.getPartitionMap(), true);
            System.out.println("Dropped partitions for " + uri);
            return true;
        }
        catch (ConnectionFailureException e) {
            throw new LauncherException("Error trying to drop " + uri, e);
        }
        catch (HCatException e) {
            throw new LauncherException("Error trying to drop " + uri, e);
        }
        catch (URISyntaxException e) {
            throw new LauncherException("Error trying to drop " + uri, e);
        }
        finally {
            closeQuietly(client);
        }
    }

    private HCatClient getHCatClient(URI uri, Configuration conf) throws LauncherException {
        // Do not use the constructor public HiveConf(Configuration other, Class<?> cls)
        // It overwrites the values in conf with default values
        final HiveConf hiveConf = new HiveConf();
        for (Entry<String, String> entry : conf) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        String serverURI = getMetastoreConnectURI(uri);
        if (!serverURI.equals("")) {
            hiveConf.set("hive.metastore.local", "false");
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, serverURI);
        try {
            System.out.println("Creating HCatClient for user=" + UserGroupInformation.getCurrentUser() + " and server="
                    + serverURI);
            // Delegation token fetched from metastore has new Text() as service and
            // HiveMetastoreClient looks for the same if not overriden by hive.metastore.token.signature
            // We are good as long as HCatCredentialHelper does not change the service of the token.
            return HCatClient.create(hiveConf);
        }
        catch (HCatException e) {
            throw new LauncherException("Error trying to connect to " + serverURI, e);
        }
        catch (IOException e) {
            throw new LauncherException("Error trying to connect to " + serverURI, e);
        }
    }

    private String getMetastoreConnectURI(URI uri) {
        String metastoreURI;
        // For unit tests
        if (uri.getAuthority().equals("unittest-local")) {
            metastoreURI = "";
        }
        else {
            // Hardcoding hcat to thrift mapping till support for webhcat(templeton)
            // is added
            metastoreURI = "thrift://" + uri.getAuthority();
        }
        return metastoreURI;
    }

    private void closeQuietly(HCatClient client) {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception ignore) {
                System.err.println("Error closing hcat client");
                ignore.printStackTrace(System.err);
            }
        }
    }

    @Override
    public List<Class<?>> getClassesForLauncher() {
        return classesToShip;
    }

}
