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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ZKUtils;

/**
 * This Service helps coordinate other Services to prevent duplicate processing of Jobs if there are multiple Oozie Servers.  This
 * implementation uses ZooKeeper and is designed to correctly deal with multiple Oozie Servers.
 * <p>
 * The distributed locks provided by {@link ZKLocksService} will prevent any concurrency issues from occurring if multiple Oozie
 * Servers try to process the same job at the same time.  However, this will make Oozie slower (more waiting on locks) and will
 * place additional stress on ZooKeeper and the Database.  By "assigning" different Oozie servers to process different jobs, we can
 * improve this situation.  This is particularly necessary for Services like the {@link RecoveryService}, which could duplicate jobs
 * otherwise.  We can assign jobs to servers by doing a mod of the jobs' id and the number of servers.
 */
public class ZKJobsConcurrencyService extends JobsConcurrencyService implements Service, Instrumentable {

    private ZKUtils zk;

    // This pattern gives us the id number without the extra stuff
    private static Pattern ID_PATTERN = Pattern.compile("(\\d{7})-.*");

    /**
     * Initialize the zookeeper jobs concurrency service
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) throws ServiceException {
        super.init(services);
        try {
            zk = ZKUtils.register(this);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E1700, ex.getMessage(), ex);
        }
    }

    /**
     * Destroy the zookeeper jobs concurrency service.
     */
    @Override
    public void destroy() {
        if (zk != null) {
            zk.unregister(this);
        }
        zk = null;
        super.destroy();
    }

    /**
     * Instruments the memory locks service.
     *
     * @param instr instance to instrument the zookeeper jobs concurrency service to.
     */
    @Override
    public void instrument(Instrumentation instr) {
        super.instrument(instr);
    }

    /**
     * Check to see if this server is the first server.  This implementation only returns true if this server is the first server in
     * ZooKeeper's list of Oozie servers (so only one Oozie Server will return true).
     *
     * @return true if this server is first; false if not
     */
    @Override
    public boolean isFirstServer() {
        List<ServiceInstance<Map>> oozies = zk.getAllMetaData();
        int myIndex = zk.getZKIdIndex(oozies);
        return (myIndex == 0);
    }

    /**
     * Check to see if jobId should be processed by this server.  This implementation only returns true if the index of this server
     * in ZooKeeper's list of servers is equal to the id of the job mod the number of servers.
     *
     * @param jobId The jobId to check
     * @return true if this server should process this jobId; false if not
     */
    @Override
    public boolean isJobIdForThisServer(String jobId) {
        List<ServiceInstance<Map>> oozies = zk.getAllMetaData();
        int numOozies = oozies.size();
        int myIndex = zk.getZKIdIndex(oozies);
        return checkJobIdForServer(jobId, numOozies, myIndex);
    }

    /**
     * Filter out any job ids that should not be processed by this server.  This implementation only preserves jobs such that the
     * index of this server in ZooKeeper's list of servers is equal to the id of the job mod the number of servers.
     *
     * @param ids The list of job ids to check
     * @return a filtered list of job ids that this server should process
     */
    @Override
    public List<String> getJobIdsForThisServer(List<String> ids) {
        List<String> filteredIds = new ArrayList<String>();
        List<ServiceInstance<Map>> oozies = zk.getAllMetaData();
        int numOozies = oozies.size();
        int myIndex = zk.getZKIdIndex(oozies);
        for(String id : ids) {
            if (checkJobIdForServer(id, numOozies, myIndex)) {
                filteredIds.add(id);
            }
        }
        return filteredIds;
    }

    /**
     * Check if the jobId should be processed by the server with index myIndex when there are numOozies servers.
     *
     * @param jobId The jobId to check
     * @param numOozies The number of Oozie servers
     * @param myIndex The index of the Oozie server
     * @return true if the jobId should be processed by the server, false if not
     */
    private boolean checkJobIdForServer(String jobId, int numOozies, int myIndex) {
        boolean belongs = true;
        Matcher m = ID_PATTERN.matcher(jobId);
        if (m.matches() && m.groupCount() == 1) {
            String idNumStr = m.group(1);
            int idNum = Integer.parseInt(idNumStr);
            belongs = (idNum % numOozies == myIndex);
        }
        return belongs;
    }

    /**
     * Return a map of server id to Oozie server URL.  This implementation always returns a map with where the key is the ZooKeeper
     * id and the value is the URL of each Oozie server that we can see in the service discovery in ZooKeeper.
     *
     * @return A map of Oozie server ids and URLs
     */
    @Override
    public Map<String, String> getServerUrls() {
        Map<String, String> urls = new HashMap<String, String>();
        List<ServiceInstance<Map>> oozies = zk.getAllMetaData();
        for (ServiceInstance<Map> oozie : oozies) {
            Map<String, String> metadata = oozie.getPayload();
            String id = metadata.get(ZKUtils.ZKMetadataKeys.OOZIE_ID);
            String url = metadata.get(ZKUtils.ZKMetadataKeys.OOZIE_URL);
            urls.put(id, url);
        }
        return urls;
    }
}
