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

package org.apache.oozie.test.hive;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;

import com.google.common.io.Files;

// TODO: This class and AbstractHiveService are copied from the org.apache.hive.jdbc package in Hive 13.1; we can remove them once
// Hive publishes its "hive-it-unit" artifact to maven.
// TODO: We also had to make some changes to the constructor to get this to work properly; hopefully by the time we pick up MiniHS2
// from Hive itself they'll have fixed it
public class MiniHS2 extends AbstractHiveService {
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private HiveServer2 hiveServer2 = null;
  private final File baseDir;
  private final Path baseDfsDir;
  private static final AtomicLong hs2Counter = new AtomicLong();
  private static final String HS2_BINARY_MODE = "binary";
  private static final String HS2_HTTP_MODE = "http";
  private MiniMrShim mr;
  private MiniDFSShim dfs;

  public MiniHS2(HiveConf hiveConf, FileSystem fs) throws IOException {
    // MiniHS2 normally only lets you do either "local" mode or normal mode.  We couldn't use "local" mode because it forks out
    // a process to run a shell script (that we don't have) to run Hadoop jobs.  And we didn't want to use normal mode because that
    // creates Mini MR and DFS clusters, which we already have setup for Oozie.  Our hacking here involved deleting the Hive Mini
    // MR/DFS cluster code and passing in our jobConf in the hiveConf so that HS2 would use our Mini MR/DFS cluster.
    super(hiveConf, "localhost", MetaStoreUtils.findFreePort(), MetaStoreUtils.findFreePort());
    baseDir =  Files.createTempDir();
    baseDfsDir =  new Path(new Path(fs.getUri()), "/base");
    String metaStoreURL =  "jdbc:derby:" + baseDir.getAbsolutePath() + File.separator + "test_metastore-" +
        hs2Counter.incrementAndGet() + ";create=true";

    fs.mkdirs(baseDfsDir);
    Path wareHouseDir = new Path(baseDfsDir, "warehouse");
    fs.mkdirs(wareHouseDir);
    setWareHouseDir(wareHouseDir.toString());
    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, metaStoreURL);
    // reassign a new port, just in case if one of the MR services grabbed the last one
    setBinaryPort(MetaStoreUtils.findFreePort());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HS2_BINARY_MODE);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, getHost());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, getBinaryPort());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, getHttpPort());
    HiveMetaStore.HMSHandler.resetDefaultDBFlag();

    Path scratchDir = new Path(baseDfsDir, "scratch");
    fs.mkdirs(scratchDir);
    System.setProperty(HiveConf.ConfVars.SCRATCHDIR.varname, scratchDir.toString());
    System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.varname,
        baseDir.getPath() + File.separator + "scratch");
  }

  public void start(Map<String, String> confOverlay) throws Exception {
    hiveServer2 = new HiveServer2();
    // Set confOverlay parameters
    for (Map.Entry<String, String> entry : confOverlay.entrySet()) {
      setConfProperty(entry.getKey(), entry.getValue());
    }
    hiveServer2.init(getHiveConf());
    hiveServer2.start();
    waitForStartup();
    setStarted(true);
  }

  public void stop() {
    verifyStarted();
    hiveServer2.stop();
    setStarted(false);
    try {
      if (mr != null) {
        mr.shutdown();
        mr = null;
      }
      if (dfs != null) {
        dfs.shutdown();
        dfs = null;
      }
    } catch (IOException e) {
      // Ignore errors cleaning up miniMR
    }
    FileUtils.deleteQuietly(baseDir);
  }

  public CLIServiceClient getServiceClient() {
    verifyStarted();
    return getServiceClientInternal();
  }

  public CLIServiceClient getServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        return new ThriftCLIServiceClient((ThriftHttpCLIService) service);
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }

  /**
   * return connection URL for this server instance
   * @return
   */
  public String getJdbcURL() {
    return getJdbcURL("default");
  }

  /**
   * return connection URL for this server instance
   * @param dbName - DB name to be included in the URL
   * @return
   */
  public String getJdbcURL(String dbName) {
    return getJdbcURL(dbName, "");
  }

  /**
   * return connection URL for this server instance
   * @param dbName - DB name to be included in the URL
   * @param urlExtension - Addional string to be appended to URL
   * @return
   */
  public String getJdbcURL(String dbName, String urlExtension) {
    assertNotNull("URL extension shouldn't be null", urlExtension);
    String transportMode = getConfProperty(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    if(transportMode != null && (transportMode.equalsIgnoreCase(HS2_HTTP_MODE))) {
      return "jdbc:hive2://" + getHost() + ":" + getHttpPort() + "/" + dbName;
    }
    else {
      return "jdbc:hive2://" + getHost() + ":" + getBinaryPort() + "/" + dbName + urlExtension;
    }
  }

  public static String getJdbcDriverName() {
    return driverName;
  }

  public MiniMrShim getMR() {
    return mr;
  }

  public MiniDFSShim getDFS() {
    return dfs;
  }

  private void waitForStartup() throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 1000000000L;
    CLIServiceClient hs2Client = getServiceClientInternal();
    SessionHandle sessionHandle = null;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer2: " + getJdbcURL());
      }
      try {
        sessionHandle = hs2Client.openSession("foo", "bar");
      } catch (Exception e) {
        // service not started yet
        continue;
      }
      hs2Client.closeSession(sessionHandle);
      break;
    } while (true);
  }

}