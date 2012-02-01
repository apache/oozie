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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * The UUID service generates unique IDs.
 * <p/>
 * The configuration property {@link #CONF_GENERATOR} specifies the ID generation type, 'random' or 'counter'.
 * <p/>
 * For 'random' uses the JDK UUID.randomUUID() method.
 * <p/>
 * For 'counter' uses a counter postfixed wit the system start up time.
 */
public class UUIDService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "UUIDService.";

    public static final String CONF_GENERATOR = CONF_PREFIX + "generator";

    private String startTime;
    private AtomicLong counter;
    private String systemId;

    /**
     * Initialize the UUID service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the UUID service could not be initialized.
     */
    @Override
    public void init(Services services) throws ServiceException {
        String genType = services.getConf().get(CONF_GENERATOR, "counter").trim();
        if (genType.equals("counter")) {
            counter = new AtomicLong();
            startTime = new SimpleDateFormat("yyMMddHHmmssSSS").format(new Date());
        }
        else {
            if (!genType.equals("random")) {
                throw new ServiceException(ErrorCode.E0120, genType);
            }
        }
        systemId = services.getSystemId();
    }

    /**
     * Destroy the UUID service.
     */
    @Override
    public void destroy() {
        counter = null;
        startTime = null;
    }

    /**
     * Return the public interface for UUID service.
     *
     * @return {@link UUIDService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return UUIDService.class;
    }

    private String longPadding(long number) {
        StringBuilder sb = new StringBuilder();
        sb.append(number);
        if (sb.length() <= 7) {
            sb.insert(0, "0000000".substring(sb.length()));
        }
        return sb.toString();
    }

    /**
     * Create a unique ID.
     *
     * @param type: Type of Id. Generally 'C' for Coordinator and 'W' for Workflow.
     * @return unique ID.
     */
    public String generateId(ApplicationType type) {
        StringBuilder sb = new StringBuilder();

        if (counter != null) {
            sb.append(longPadding(counter.getAndIncrement())).append('-').append(startTime);
        }
        else {
            sb.append(UUID.randomUUID().toString());
            if (sb.length() > (37 - systemId.length())) {
                sb.setLength(37 - systemId.length());
            }
        }
        sb.append('-').append(systemId);
        sb.append('-').append(type.getType());
        // limitation due to current DB schema for action ID length (100)
        if (sb.length() > 40) {
            throw new RuntimeException(XLog.format("ID exceeds limit of 40 characters, [{0}]", sb));
        }
        return sb.toString();
    }

    /**
     * Create a child ID.
     * <p/>
     * If the same child name is given the returned child ID is the same.
     *
     * @param id unique ID.
     * @param childName child name.
     * @return a child ID.
     */
    public String generateChildId(String id, String childName) {
        id = ParamChecker.notEmpty(id, "id") + "@" + ParamChecker.notEmpty(childName, "childName");

        // limitation due to current DB schema for action ID length (100)
        if (id.length() > 95) {
            throw new RuntimeException(XLog.format("Child ID exceeds limit of 95 characters, [{0}]", id));
        }
        return id;
    }

    /**
     * Return the ID from a child ID.
     *
     * @param childId child ID.
     * @return ID of the child ID.
     */
    public String getId(String childId) {
        int index = ParamChecker.notEmpty(childId, "childId").indexOf("@");
        if (index == -1) {
            throw new IllegalArgumentException(XLog.format("invalid child id [{0}]", childId));
        }
        return childId.substring(0, index);
    }

    /**
     * Return the child name from a child ID.
     *
     * @param childId child ID.
     * @return child name.
     */
    public String getChildName(String childId) {
        int index = ParamChecker.notEmpty(childId, "childId").indexOf("@");
        if (index == -1) {
            throw new IllegalArgumentException(XLog.format("invalid child id [{0}]", childId));
        }
        return childId.substring(index + 1);
    }

    public enum ApplicationType {
        WORKFLOW('W'), COORDINATOR('C'), BUNDLE('B');
        private final char type;

        private ApplicationType(char type) {
            this.type = type;
        }

        public char getType() {
            return type;
        }
    }
}
