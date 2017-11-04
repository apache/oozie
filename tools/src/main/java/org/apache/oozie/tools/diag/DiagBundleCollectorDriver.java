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

package org.apache.oozie.tools.diag;

import com.google.common.io.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.TimeZone;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Output directory is specified by user")
public class DiagBundleCollectorDriver {
    static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss zzz",
            TimeZone.getTimeZone("GMT"), Locale.US);
    private String oozieURL;
    private Integer numWorkflows;
    private Integer numCoords;
    private Integer numBundles;
    private String[] jobIds;
    private Integer maxChildActions;
    private File outputDir;
    private Configuration hadoopConfig;

    public static void main(String[] args) throws Exception {
        final DiagBundleCollectorDriver oozieDiagBundleCollector = new DiagBundleCollectorDriver();
        System.exit(oozieDiagBundleCollector.run(args));
    }

    private int run(final String[] args) throws Exception {
        if (!parseCommandLineArgs(args) || !setHadoopConfig()) {
            return -1;
        }

        final DiagOozieClient client = new DiagOozieClient(oozieURL, null);
        checkOozieConnection(client);

        final File tempDir = Files.createTempDir();
        System.out.println("Using Temporary Directory: " + tempDir);

        try {
            collectDiagInformation(numWorkflows, numCoords, numBundles, jobIds, maxChildActions, client, tempDir);
            DiagBundleCompressor.compressDiagInformationToBundle(outputDir, tempDir);
        } finally {
            FileUtils.deleteDirectory(tempDir);
        }

        System.out.println();
        return 0;
    }

    private boolean parseCommandLineArgs(final String[] args) throws IOException {
        final ArgParser cliParser = new ArgParser();
        if (!cliParser.parseCommandLineArguments(args)) {
            return false;
        }

        oozieURL = cliParser.getOozieUrl();
        numWorkflows = cliParser.getNumWorkflows();
        numCoords = cliParser.getNumCoordinators();
        numBundles = cliParser.getNumBundles();
        jobIds = cliParser.getJobIds();
        maxChildActions = cliParser.getMaxChildActions();
        outputDir = cliParser.ensureOutputDir();

        return true;
    }

    private void collectDiagInformation(final Integer numWorkflows,
                                        final Integer numCoords,
                                        final Integer numBundles,
                                        final String[] jobIds,
                                        final Integer maxChildActions,
                                        final DiagOozieClient client,
                                        final File tempDir) {
        final ServerInfoCollector serverInfoCollector = new ServerInfoCollector(client);
        serverInfoCollector.storeShareLibInfo(tempDir);
        serverInfoCollector.storeServerConfiguration(tempDir);
        serverInfoCollector.storeOsEnv(tempDir);
        serverInfoCollector.storeJavaSystemProperties(tempDir);
        serverInfoCollector.storeCallableQueueDump(tempDir);
        serverInfoCollector.storeThreadDump(tempDir);

        final MetricsCollector metricsCollector = new MetricsCollector(client);
        metricsCollector.storeInstrumentationInfo(tempDir);
        metricsCollector.storeMetrics(tempDir);

        final AppInfoCollector workflowInfoCollector = new AppInfoCollector(hadoopConfig, client);
        workflowInfoCollector.storeLastWorkflows(tempDir, numWorkflows, maxChildActions);
        workflowInfoCollector.storeLastCoordinators(tempDir, numCoords, maxChildActions);
        workflowInfoCollector.storeLastBundles(tempDir, numBundles, maxChildActions);
        workflowInfoCollector.getSpecificJobs(tempDir, jobIds, maxChildActions);
    }

    private void checkOozieConnection(final OozieClient client) throws OozieClientException {
        System.out.print("Checking Connection...");
        client.validateWSVersion();
        System.out.println("Done");
    }

    private boolean setHadoopConfig() {
        final String oozieHome = System.getenv("OOZIE_HOME");
        if (oozieHome == null) {
            System.err.println("OOZIE_HOME environment variable is not set. Make sure you've set it to an absolute path.");
            return false;
        }

        System.setProperty(Services.OOZIE_HOME_DIR, oozieHome);
        try {
            final Services services = initOozieServices();
            final HadoopAccessorService hadoopAccessorService = services.get(HadoopAccessorService.class);
            hadoopConfig = hadoopAccessorService.createConfiguration("*");

        } catch (ServiceException e) {
            System.err.printf("Could not initialize Hadoop configuration: %s%n", e.getMessage());
            return false;
        }
        return true;
    }

    private Services initOozieServices() throws ServiceException {
        final Services services = new Services();
        services.getConf()
                .set(Services.CONF_SERVICE_CLASSES, "org.apache.oozie.service.LiteWorkflowAppService,"
                        + "org.apache.oozie.service.SchedulerService,"
                        + "org.apache.oozie.service.HadoopAccessorService,"
                        + "org.apache.oozie.service.ShareLibService");
        services.init();
        return services;
    }
}