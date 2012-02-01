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

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.util.HashMap;
import java.util.Map;

public class ActionService implements Service {

    public static final String CONF_ACTION_EXECUTOR_CLASSES = CONF_PREFIX + "ActionService.executor.classes";

    public static final String CONF_ACTION_EXECUTOR_EXT_CLASSES = CONF_PREFIX + "ActionService.executor.ext.classes";

    private Services services;
    private Map<String, Class<? extends ActionExecutor>> executors;

    @SuppressWarnings("unchecked")
    public void init(Services services) throws ServiceException {
        this.services = services;
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = new HashMap<String, Class<? extends ActionExecutor>>();
        Class<? extends ActionExecutor>[] classes =
                (Class<? extends ActionExecutor>[]) services.getConf().getClasses(CONF_ACTION_EXECUTOR_CLASSES);
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) services.getConf().getClasses(CONF_ACTION_EXECUTOR_EXT_CLASSES);
        registerExecutors(classes);
    }

    private void registerExecutors(Class<? extends ActionExecutor>[] classes) throws ServiceException {
        if (classes != null) {
            for (Class<? extends ActionExecutor> executorClass : classes) {
                register(executorClass);
            }
        }
    }

    public void destroy() {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = null;
    }

    public Class<? extends Service> getInterface() {
        return ActionService.class;
    }

    public void register(Class<? extends ActionExecutor> klass) throws ServiceException {
        XLog log = XLog.getLog(getClass());
        ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(klass, services.getConf());
        if (executors.containsKey(executor.getType())) {
            throw new ServiceException(ErrorCode.E0150, XLog.format(
                    "Action executor for action type [{1}] already registered", executor.getType()));
        }
        ActionExecutor.enableInit();
        executor.initActionType();
        ActionExecutor.disableInit();
        executors.put(executor.getType(), klass);
        log.trace("Registered Action executor for action type [{0}] class [{1}]", executor.getType(), klass);
    }

    public ActionExecutor getExecutor(String actionType) {
        ParamChecker.notEmpty(actionType, "actionType");
        Class<? extends ActionExecutor> executorClass = executors.get(actionType);
        return (executorClass != null) ? (ActionExecutor) ReflectionUtils.newInstance(executorClass, null) : null;
    }

}
