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

import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.ErrorCode;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The ELService creates {@link ELEvaluator} instances preconfigured with constants and functions defined in the
 * configuration. <p/> The following configuration parameters control the EL service: <p/> {@link #CONF_CONSTANTS} list
 * of constant definitions to be available for EL evaluations. <p/> {@link #CONF_FUNCTIONS} list of function definitions
 * to be available for EL evalations. <p/> Definitions must be separated by a comma, definitions are trimmed. <p/> The
 * syntax for a constant definition is <code>PREFIX:NAME=CLASS_NAME#CONSTANT_NAME</code>. <p/> The syntax for a constant
 * definition is <code>PREFIX:NAME=CLASS_NAME#METHOD_NAME</code>.
 */
public class ELService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ELService.";

    public static final String CONF_CONSTANTS = CONF_PREFIX + "constants.";

    public static final String CONF_EXT_CONSTANTS = CONF_PREFIX + "ext.constants.";

    public static final String CONF_FUNCTIONS = CONF_PREFIX + "functions.";

    public static final String CONF_EXT_FUNCTIONS = CONF_PREFIX + "ext.functions.";

    public static final String CONF_GROUPS = CONF_PREFIX + "groups";

    private final XLog log = XLog.getLog(getClass());

    //<Group Name>, <List of constants>
    private HashMap<String, List<ELConstant>> constants;
    //<Group Name>, <List of functions>
    private HashMap<String, List<ELFunction>> functions;

    private static class ELConstant {
        private String name;
        private Object value;

        private ELConstant(String prefix, String name, Object value) {
            if (prefix.length() > 0) {
                name = prefix + ":" + name;
            }
            this.name = name;
            this.value = value;
        }
    }

    private static class ELFunction {
        private String prefix;
        private String name;
        private Method method;

        private ELFunction(String prefix, String name, Method method) {
            this.prefix = prefix;
            this.name = name;
            this.method = method;
        }
    }

    private List<ELService.ELConstant> extractConstants(Configuration conf, String key) throws ServiceException {
        List<ELService.ELConstant> list = new ArrayList<ELService.ELConstant>();
        if (conf.get(key, "").trim().length() > 0) {
            for (String function : ConfigurationService.getStrings(conf, key)) {
                String[] parts = parseDefinition(function);
                list.add(new ELConstant(parts[0], parts[1], findConstant(parts[2], parts[3])));
                log.trace("Registered prefix:constant[{0}:{1}] for class#field[{2}#{3}]", (Object[]) parts);
            }
        }
        return list;
    }

    private List<ELService.ELFunction> extractFunctions(Configuration conf, String key) throws ServiceException {
        List<ELService.ELFunction> list = new ArrayList<ELService.ELFunction>();
        if (conf.get(key, "").trim().length() > 0) {
            for (String function : ConfigurationService.getStrings(conf, key)) {
                String[] parts = parseDefinition(function);
                list.add(new ELFunction(parts[0], parts[1], findMethod(parts[2], parts[3])));
                log.trace("Registered prefix:constant[{0}:{1}] for class#field[{2}#{3}]", (Object[]) parts);
            }
        }
        return list;
    }

    /**
     * Initialize the EL service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the EL service could not be initialized.
     */
    @Override
    public synchronized void init(Services services) throws ServiceException {
        log.trace("Constants and functions registration");
        constants = new HashMap<String, List<ELConstant>>();
        functions = new HashMap<String, List<ELFunction>>();
        //Get the list of group names from configuration file
        // defined in the property tag: oozie.service.ELSerice.groups
        //String []groupList = services.getConf().get(CONF_GROUPS, "").trim().split(",");
        String[] groupList = ConfigurationService.getStrings(services.getConf(), CONF_GROUPS);
        //For each group, collect the required functions and constants
        // and store it into HashMap
        for (String group : groupList) {
            List<ELConstant> tmpConstants = new ArrayList<ELConstant>();
            tmpConstants.addAll(extractConstants(services.getConf(), CONF_CONSTANTS + group));
            tmpConstants.addAll(extractConstants(services.getConf(), CONF_EXT_CONSTANTS + group));
            constants.put(group, tmpConstants);
            List<ELFunction> tmpFunctions = new ArrayList<ELFunction>();
            tmpFunctions.addAll(extractFunctions(services.getConf(), CONF_FUNCTIONS + group));
            tmpFunctions.addAll(extractFunctions(services.getConf(), CONF_EXT_FUNCTIONS + group));
            functions.put(group, tmpFunctions);
        }
    }

    /**
     * Destroy the EL service.
     */
    @Override
    public void destroy() {
        constants = null;
        functions = null;
    }

    /**
     * Return the public interface for EL service.
     *
     * @return {@link ELService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return ELService.class;
    }

    /**
     * Return an {@link ELEvaluator} pre-configured with the constants and functions for the specific group of
     * EL-functions and variables defined in the configuration. If the group name doesn't exist,
     * IllegalArgumentException is thrown
     *
     * @param group: Name of the group of required EL Evaluator.
     * @return a preconfigured {@link ELEvaluator}.
     */
    public ELEvaluator createEvaluator(String group) {
        ELEvaluator.Context context = new ELEvaluator.Context();
        boolean groupDefined = false;
        if (constants.containsKey(group)) {
            for (ELConstant constant : constants.get(group)) {
                context.setVariable(constant.name, constant.value);
            }
            groupDefined = true;
        }
        if (functions.containsKey(group)) {
            for (ELFunction function : functions.get(group)) {
                context.addFunction(function.prefix, function.name, function.method);
            }
            groupDefined = true;
        }
        if (groupDefined == false) {
            throw new IllegalArgumentException("Group " + group + " is not defined");
        }
        return new ELEvaluator(context);
    }

    private static String[] parseDefinition(String str) throws ServiceException {
        try {
            str = str.trim();
            if (!str.contains(":")) {
                str = ":" + str;
            }
            String[] parts = str.split(":");
            String prefix = parts[0];
            parts = parts[1].split("=");
            String name = parts[0];
            parts = parts[1].split("#");
            String klass = parts[0];
            String method = parts[1];
            return new String[]{prefix, name, klass, method};
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0110, str, ex.getMessage(), ex);
        }
    }

    public static Method findMethod(String className, String methodName) throws ServiceException {
        Method method = null;
        try {
            Class klass = Thread.currentThread().getContextClassLoader().loadClass(className);
            for (Method m : klass.getMethods()) {
                if (m.getName().equals(methodName)) {
                    method = m;
                    break;
                }
            }
            if (method == null) {
                throw new ServiceException(ErrorCode.E0111, className, methodName);
            }
            if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) != (Modifier.PUBLIC | Modifier.STATIC)) {
                throw new ServiceException(ErrorCode.E0112, className, methodName);
            }
        }
        catch (ClassNotFoundException ex) {
            throw new ServiceException(ErrorCode.E0113, className);
        }
        return method;
    }

    public static Object findConstant(String className, String constantName) throws ServiceException {
        try {
            Class klass = Thread.currentThread().getContextClassLoader().loadClass(className);
            Field field = klass.getField(constantName);
            if ((field.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) != (Modifier.PUBLIC | Modifier.STATIC)) {
                throw new ServiceException(ErrorCode.E0114, className, constantName);
            }
            return field.get(null);
        }
        catch (IllegalAccessException ex) {
            throw new IllegalArgumentException(ex);
        }
        catch (NoSuchFieldException ex) {
            throw new ServiceException(ErrorCode.E0115, className, constantName);
        }
        catch (ClassNotFoundException ex) {
            throw new ServiceException(ErrorCode.E0113, className);
        }
    }

}
