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

package org.apache.oozie;

import org.apache.oozie.util.XLog;

/**
 * Fault Injection support class. <p/> Concrete classes should be available only during testing, not in production. <p/>
 * To activate fault injection the {@link #FAULT_INJECTION} system property must be set to true. <p/> When fault
 * injection is activated, the concrete class (specified by name) will be call for activation. <p/> Concrete classes
 * should be activated by presense of a second system property. <p/> This fault injection pattern provides 3 levels of
 * safeguard: a general 'fault injection' system property, the availabity of of the concrete 'fault injection' class in
 * the classpath, a specifi 'fault injection' system property. <p/> Refer to the <code>SkipCommitFaultInjection</code>
 * class in the test classes for an example.
 */
public abstract class FaultInjection {

    public static final String FAULT_INJECTION = "oozie.fault.injection";

    private static FaultInjection getFaultInjection(String className) {
        if (Boolean.parseBoolean(System.getProperty(FAULT_INJECTION, "false"))) {
            try {
                Class klass = Thread.currentThread().getContextClassLoader().loadClass(className);
                return (FaultInjection) klass.newInstance();
            }
            catch (ClassNotFoundException ex) {
                XLog.getLog(FaultInjection.class).warn("Trying to activate fault injection in production", ex);
            }
            catch (IllegalAccessException ex) {
                throw new RuntimeException(XLog.format("Could not initialize [{0}]", className, ex), ex);
            }
            catch (InstantiationException ex) {
                throw new RuntimeException(XLog.format("Could not initialize [{0}]", className, ex), ex);
            }
        }
        return null;
    }

    public static boolean activate(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.activate()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, ACTIVATING [{0}]", className);
                return true;
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, DID NOT ACTIVATE [{0}]", className);
            }
        }
        return false;
    }

    public static void deactivate(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.isActive()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, DEACTIVATING [{0}]", className);
                fi.deactivate();
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, CANNOT DEACTIVATE, NOT ACTIVE [{0}]",
                                                       className);
            }
        }
    }

    public static boolean isActive(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.isActive()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, ACTIVE [{0}]", className);
                return true;
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, NOT ACTIVE [{0}]", className);
            }
        }
        return false;
    }

    public abstract boolean activate();

    public abstract void deactivate();

    public abstract boolean isActive();

}
