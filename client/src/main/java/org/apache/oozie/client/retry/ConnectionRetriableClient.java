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

package org.apache.oozie.client.retry;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;

/**
 * HTTP connection retryable class. It retries =oozie.connection.retry.count= times for ConnectException. For
 * SocketException all create connection are retried except =PUT= and =POST=.
 */
public abstract class ConnectionRetriableClient {
    private final int retryCount;

    public ConnectionRetriableClient(int retryCount) {
        this.retryCount = retryCount;
    }

    public Object execute(URL url, String method) throws IOException {
        int numTries = 0;
        boolean stopRetry = false;
        Exception cliException = null;

        while (numTries < retryCount && !stopRetry) {
            try {
                return doExecute(url, method);
            }
            catch (ConnectException e) {
                sleep(e, numTries++);
                cliException = e;
            }
            catch (SocketException e) {
                if (method.equals("POST") || method.equals("PUT")) {
                    stopRetry = true;
                }
                else {
                    sleep(e, numTries++);
                }
                cliException = e;
            }
            catch (Exception e) {
                stopRetry = true;
                cliException = e;
                numTries++;
                // No retry for other exceptions
            }
        }
        throw new IOException("Error while connecting Oozie server. No of retries = " + numTries + ". Exception = "
                + cliException.getMessage(), cliException);
    }

    private void sleep(Exception e, int numTries) {
        try {
            long wait = getWaitTimeExp(numTries);
            System.err.println("Connection exception has occurred [ " + e.getClass().getName() + " " + e.getMessage()
                    + " ]. Trying after " + wait / 1000 + " sec." + " Retry count = " + (numTries + 1));
            Thread.sleep(wait);
        }
        catch (InterruptedException e1) {
            // Ignore InterruptedException
        }
    }

    /*
     * Returns the next wait interval, in milliseconds, using an exponential backoff algorithm.
     */
    private long getWaitTimeExp(int retryCount) {
        long waitTime = ((long) Math.pow(2, retryCount) * 1000L);
        return waitTime;
    }

    public abstract Object doExecute(URL url, String method) throws Exception;

}