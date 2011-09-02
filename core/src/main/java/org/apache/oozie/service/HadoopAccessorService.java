/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.HadoopAccessor;
import org.apache.oozie.ErrorCode;

import java.util.HashMap;
import java.util.Map;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group.
 * <p/>
 * The default accessor used is the base accessor which just injects the UGI into the configuration instance
 * used to create/obtain JobClient and ileSystem instances.
 * <p/>
 * The HadoopAccess class to use can be configured in the <code>oozie-site.xml</code> using the
 * <code>oozie.service.HadoopAccessorService.accessor.class</code> property.
 */
public class HadoopAccessorService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";

    public static final String HADOOP_ACCESSOR_CLASS = CONF_PREFIX + "accessor.class";

    private Class<? extends HadoopAccessor> accessorClass;

    @SuppressWarnings("unchecked")
    public void init(Services services) throws ServiceException {
        accessorClass = (Class<? extends HadoopAccessor>) services.getConf().getClass(HADOOP_ACCESSOR_CLASS,
                                                                                      HadoopAccessor.class);
    }

    public void destroy() {
    }

    public Class<? extends Service> getInterface() {
        return HadoopAccessorService.class;
    }

    /**
     * Return a user/group configured HadoopAccessor instance.
     *
     * @param user user id.
     * @param group group id.
     * @return a user/group configured HadoopAccessor instance.
     */
    public HadoopAccessor get(String user, String group) {
        HadoopAccessor accessor = ReflectionUtils.newInstance(accessorClass, null);
        accessor.setUGI(user, group);
        return accessor;
    }

    /**
     * Return the HadoopAccessor class being used.
     *
     * @return HadoopAccessor class being used.
     */
    public Class<? extends HadoopAccessor> getAccessorClass() {
        return accessorClass;
    }

}