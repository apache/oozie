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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherAM.OozieActionResult;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;

// Adapted from org.apache.hadoop.mapreduce.v2.app.JobEndNotifier
/**
 * This call sends back an HTTP GET callback to the configured URL.  It is meant for the {@link LauncherAM} to notify the
 * Oozie Server that it has finished.
 */
public class LauncherAMCallbackNotifier {
    private static final String OOZIE_LAUNCHER_CALLBACK = "oozie.launcher.callback.";
    private static final int OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL_MAX = 5000;

    public static final String OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS = OOZIE_LAUNCHER_CALLBACK + "retry.attempts";
    public static final String OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL = OOZIE_LAUNCHER_CALLBACK + "retry.interval";
    public static final String OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS = OOZIE_LAUNCHER_CALLBACK + "max.attempts";
    public static final String OOZIE_LAUNCHER_CALLBACK_TIMEOUT = OOZIE_LAUNCHER_CALLBACK + "timeout";
    public static final String OOZIE_LAUNCHER_CALLBACK_URL = OOZIE_LAUNCHER_CALLBACK + "url";
    public static final String OOZIE_LAUNCHER_CALLBACK_PROXY = OOZIE_LAUNCHER_CALLBACK + "proxy";
    public static final String OOZIE_LAUNCHER_CALLBACK_JOBSTATUS_TOKEN = "$jobStatus";

    protected String userUrl;
    protected String proxyConf;
    protected int numTries; //Number of tries to attempt notification
    protected int waitInterval; //Time (ms) to wait between retrying notification
    protected int timeout; // Timeout (ms) on the connection and notification
    protected URL urlToNotify; //URL to notify read from the config
    protected Proxy proxyToUse = Proxy.NO_PROXY; //Proxy to use for notification


    /**
     * Parse the URL that needs to be notified of the end of the job, along
     * with the number of retries in case of failure, the amount of time to
     * wait between retries and proxy settings
     * @param conf the configuration
     */
    public LauncherAMCallbackNotifier(Configuration conf) {
        numTries = Math.min(conf.getInt(OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, 0) + 1,
                conf.getInt(OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, 1));

        waitInterval = Math.min(conf.getInt(OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL_MAX),
                OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL_MAX);
        waitInterval = (waitInterval < 0) ? OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL_MAX : waitInterval;

        timeout = conf.getInt(OOZIE_LAUNCHER_CALLBACK_TIMEOUT, OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL_MAX);

        userUrl = conf.get(OOZIE_LAUNCHER_CALLBACK_URL);

        proxyConf = conf.get(OOZIE_LAUNCHER_CALLBACK_PROXY);

        //Configure the proxy to use if its set. It should be set like
        //proxyType@proxyHostname:port
        if(proxyConf != null && !proxyConf.equals("") &&
                proxyConf.lastIndexOf(":") != -1) {
            int typeIndex = proxyConf.indexOf("@");
            Proxy.Type proxyType = Proxy.Type.HTTP;
            if(typeIndex != -1 &&
                    proxyConf.substring(0, typeIndex).compareToIgnoreCase("socks") == 0) {
                proxyType = Proxy.Type.SOCKS;
            }
            String hostname = proxyConf.substring(typeIndex + 1,
                    proxyConf.lastIndexOf(":"));
            String portConf = proxyConf.substring(proxyConf.lastIndexOf(":") + 1);
            try {
                int port = Integer.parseInt(portConf);
                proxyToUse = new Proxy(proxyType,
                        new InetSocketAddress(hostname, port));
                System.out.println("Callback notification using proxy type \"" + proxyType +
                        "\" hostname \"" + hostname + "\" and port \"" + port + "\"");
            } catch(NumberFormatException nfe) {
                System.err.println("Callback notification couldn't parse configured proxy's port "
                        + portConf + ". Not going to use a proxy");
            }
        }

    }

    /**
     * Notify the URL just once. Use best effort.
     * @return true in case URL is notified successfully.
     */
    protected boolean notifyURLOnce() {
        boolean success = false;
        HttpURLConnection conn = null;
        try {
            System.out.println("Callback notification trying " + urlToNotify);
            conn = (HttpURLConnection) urlToNotify.openConnection(proxyToUse);
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);
            conn.setAllowUserInteraction(false);
            if(conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                System.err.println("Callback notification to " + urlToNotify +" failed with code: "
                        + conn.getResponseCode() + " and message \"" + conn.getResponseMessage()
                        +"\"");
            }
            else {
                success = true;
                System.out.println("Callback notification to " + urlToNotify + " succeeded");
            }
        } catch(IOException ioe) {
            System.err.println("Callback notification to " + urlToNotify + " failed");
            ioe.printStackTrace();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return success;
    }

    /**
     * Notify a server of the completion of a submitted job.
     * @param actionResult The Action Result (failed/succeeded/running)
     *
     * @throws InterruptedException in case of interruption
     */
    public void notifyURL(OozieActionResult actionResult) throws InterruptedException {
        // Do we need job-end notification?
        if (userUrl == null) {
            System.out.println("Callback notification URL not set, skipping.");
            return;
        }

        //Do string replacements for final status
        if (userUrl.contains(OOZIE_LAUNCHER_CALLBACK_JOBSTATUS_TOKEN)) {
            userUrl = userUrl.replace(OOZIE_LAUNCHER_CALLBACK_JOBSTATUS_TOKEN, actionResult.toString());
        }

        // Create the URL, ensure sanity
        try {
            urlToNotify = new URL(userUrl);
        } catch (MalformedURLException mue) {
            System.err.println("Callback notification couldn't parse " + userUrl);
            mue.printStackTrace();
            return;
        }

        // Send notification
        boolean success = false;
        while (numTries-- > 0 && !success) {
            System.out.println("Callback notification attempts left " + numTries);
            success = notifyURLOnce();
            if (!success) {
                Thread.sleep(waitInterval);
            }
        }
        if (!success) {
            System.err.println("Callback notification failed to notify : " + urlToNotify);
        } else {
            System.out.println("Callback notification succeeded");
        }
    }
}
