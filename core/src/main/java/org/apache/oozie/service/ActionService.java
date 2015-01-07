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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.control.EndActionExecutor;
import org.apache.oozie.action.control.ForkActionExecutor;
import org.apache.oozie.action.control.JoinActionExecutor;
import org.apache.oozie.action.control.KillActionExecutor;
import org.apache.oozie.action.control.StartActionExecutor;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;

public class ActionService implements Service, Instrumentable {

    public static final String CONF_ACTION_EXECUTOR_CLASSES = CONF_PREFIX + "ActionService.executor.classes";

    public static final String CONF_ACTION_EXECUTOR_EXT_CLASSES = CONF_PREFIX + "ActionService.executor.ext.classes";

    private Services services;
    private Map<String, Class<? extends ActionExecutor>> executors;
    private static XLog LOG = XLog.getLog(ActionService.class);

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public void init(Services services) throws ServiceException {
        this.services = services;
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = new HashMap<String, Class<? extends ActionExecutor>>();

        Class<? extends ActionExecutor>[] classes = new Class[] { StartActionExecutor.class,
            EndActionExecutor.class, KillActionExecutor.class,  ForkActionExecutor.class, JoinActionExecutor.class };
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) ConfigurationService.getClasses
                (services.getConf(), CONF_ACTION_EXECUTOR_CLASSES);
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) ConfigurationService.getClasses
                (services.getConf(), CONF_ACTION_EXECUTOR_EXT_CLASSES);
        registerExecutors(classes);

        initExecutors();
    }

    private void registerExecutors(Class<? extends ActionExecutor>[] classes) {
        if (classes != null) {
            for (Class<? extends ActionExecutor> executorClass : classes) {
                @SuppressWarnings("deprecation")
                ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(executorClass, services.getConf());
                executors.put(executor.getType(), executorClass);
            }
        }
    }

    private void initExecutors() {
        for (Class<? extends ActionExecutor> executorClass : executors.values()) {
            initExecutor(executorClass);
        }
        LOG.info("Initialized action types: " + getActionTypes());
    }

    @Override
    public void destroy() {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = null;
    }

    @Override
    public Class<? extends Service> getInterface() {
        return ActionService.class;
    }

    @Override
    public void instrument(Instrumentation instr) {
        instr.addVariable("configuration", "action.types", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                Set<String> actionTypes = getActionTypes();
                if (actionTypes != null) {
                    return actionTypes.toString();
                }
                return "(unavailable)";
            }
        });
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    public void registerAndInitExecutor(Class<? extends ActionExecutor> klass) {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        registerExecutors(new Class[]{klass});
        initExecutors();
    }

    private void initExecutor(Class<? extends ActionExecutor> klass) {
        @SuppressWarnings("deprecation")
        ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(klass, services.getConf());
        LOG.debug("Initializing action type [{0}] class [{1}]", executor.getType(), klass);
        ActionExecutor.enableInit();
        executor.initActionType();
        ActionExecutor.disableInit();
        LOG.trace("Initialized Executor for action type [{0}] class [{1}]", executor.getType(), klass);
    }

    public ActionExecutor getExecutor(String actionType) {
        ParamChecker.notEmpty(actionType, "actionType");
        Class<? extends ActionExecutor> executorClass = executors.get(actionType);
        return (executorClass != null) ? (ActionExecutor) ReflectionUtils.newInstance(executorClass, null) : null;
    }

    Set<String> getActionTypes() {
        return executors.keySet();
    }
}
