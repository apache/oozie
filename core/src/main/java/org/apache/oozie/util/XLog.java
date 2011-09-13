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
package org.apache.oozie.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>XLog</code> class extends the functionality of the Apache common-logging <code>Log</code> interface. <p/>
 * It provides common prefix support, message templating with variable parameters and selective tee logging to multiple
 * logs. <p/> It provides also the LogFactory functionality.
 */
public class XLog implements Log {

    /**
     * <code>LogInfo</code> stores contextual information to create log prefixes. <p/> <code>LogInfo</code> uses a
     * <code>ThreadLocal</code> to propagate the context. <p/> <code>LogInfo</code> context parameters are configurable
     * singletons.
     */
    public static class Info {
        private static String template = "";
        private static List<String> parameterNames = new ArrayList<String>();

        private static ThreadLocal<Info> tlLogInfo = new ThreadLocal<Info>() {
            @Override
            protected Info initialValue() {
                return new Info();
            }

        };

        /**
         * Define a <code>LogInfo</code> context parameter. <p/> The parameter name and its contextual value will be
         * used to create all prefixes.
         *
         * @param name name of the context parameter.
         */
        public static void defineParameter(String name) {
            ParamChecker.notEmpty(name, "name");
            int count = parameterNames.size();
            if (count > 0) {
                template += " ";
            }
            template += name + "[{" + count + "}]";
            parameterNames.add(name);
        }

        /**
         * Remove all defined context parameters. <p/>
         */
        public static void reset() {
            template = "";
            parameterNames.clear();
        }

        /**
         * Return the <code>LogInfo</code> instance in context.
         *
         * @return The thread local instance of LogInfo
         */
        public static Info get() {
            return tlLogInfo.get();
        }

        /**
         * Remove the <code>LogInfo</code> instance in context.
         */
        public static void remove() {
            tlLogInfo.remove();
        }

        private Map<String, String> parameters = new HashMap<String, String>();

        /**
         * Constructs an empty LogInfo.
         */
        public Info() {
        }


        /**
         * Construct a new LogInfo object from an existing one.
         *
         * @param logInfo LogInfo object to copy parameters from.
         */
        public Info(Info logInfo) {
            setParameters(logInfo);
        }

        /**
         * Clear all parameters set in this logInfo instance.
         */
        public void clear() {
            parameters.clear();
        }

        /**
         * Set a parameter value in the <code>LogInfo</code> context.
         *
         * @param name parameter name.
         * @param value parameter value.
         */
        public void setParameter(String name, String value) {
            if (!parameterNames.contains(name)) {
                throw new IllegalArgumentException(format("Parameter[{0}] not defined", name));
            }
            parameters.put(name, value);
        }

        /**
         * Returns the specified parameter.
         *
         * @param name parameter name.
         * @return the parameter value.
         */
        public String getParameter(String name) {
            return parameters.get(name);
        }

        /**
         * Clear a parameter value from the <code>LogInfo</code> context.
         *
         * @param name parameter name.
         */
        public void clearParameter(String name) {
            if (!parameterNames.contains(name)) {
                throw new IllegalArgumentException(format("Parameter[{0}] not defined", name));
            }
            parameters.remove(name);
        }

        /**
         * Set all the parameter values from the given <code>LogInfo</code>.
         *
         * @param logInfo <code>LogInfo</code> to copy all parameter values from.
         */
        public void setParameters(Info logInfo) {
            parameters.clear();
            parameters.putAll(logInfo.parameters);
        }

        /**
         * Create the <code>LogInfo</code> prefix using the current parameter values.
         *
         * @return the <code>LogInfo</code> prefix.
         */
        public String createPrefix() {
            String[] params = new String[parameterNames.size()];
            for (int i = 0; i < params.length; i++) {
                params[i] = parameters.get(parameterNames.get(i));
                if (params[i] == null) {
                    params[i] = "-";
                }
            }
            return MessageFormat.format(template, (Object[]) params);
        }

    }

    /**
     * Return the named logger configured with the {@link org.apache.oozie.util.XLog.Info} prefix.
     *
     * @param name logger name.
     * @return the named logger configured with the {@link org.apache.oozie.util.XLog.Info} prefix.
     */
    public static XLog getLog(String name) {
        return getLog(name, true);
    }

    /**
     * Return the named logger configured with the {@link org.apache.oozie.util.XLog.Info} prefix.
     *
     * @param clazz from which the logger name will be derived.
     * @return the named logger configured with the {@link org.apache.oozie.util.XLog.Info} prefix.
     */
    public static XLog getLog(Class clazz) {
        return getLog(clazz, true);
    }

    /**
     * Return the named logger.
     *
     * @param name logger name.
     * @param prefix indicates if the {@link org.apache.oozie.util.XLog.Info} prefix has to be used or not.
     * @return the named logger.
     */
    public static XLog getLog(String name, boolean prefix) {
        return new XLog(LogFactory.getLog(name), (prefix) ? Info.get().createPrefix() : "");
    }

    /**
     * Return the named logger.
     *
     * @param clazz from which the logger name will be derived.
     * @param prefix indicates if the {@link org.apache.oozie.util.XLog.Info} prefix has to be used or not.
     * @return the named logger.
     */
    public static XLog getLog(Class clazz, boolean prefix) {
        return new XLog(LogFactory.getLog(clazz), (prefix) ? Info.get().createPrefix() : "");
    }

    /**
     * Reset the logger prefix
     *
     * @param log the named logger
     * @return the named logger with reset prefix
     */
    public static XLog resetPrefix(XLog log) {
        log.setMsgPrefix(Info.get().createPrefix());
        return log;
    }

    /**
     * Mask for logging to the standard log.
     */
    public static final int STD = 1;

    /**
     * Mask for tee logging to the OPS log.
     */
    public static final int OPS = 4;

    private static final int ALL = STD | OPS;

    private static final int[] LOGGER_MASKS = {STD, OPS};

    //package private for testing purposes.
    Log[] loggers;

    private String prefix = "";

    /**
     * Create a <code>XLog</code> with no prefix.
     *
     * @param log Log instance to use for logging.
     */
    public XLog(Log log) {
        this(log, "");
    }

    /**
     * Create a <code>XLog</code> with a common prefix. <p/> The prefix will be prepended to all log messages.
     *
     * @param log Log instance to use for logging.
     * @param prefix common prefix to use for all log messages.
     */
    public XLog(Log log, String prefix) {
        this.prefix = prefix;
        loggers = new Log[2];
        loggers[0] = log;
        loggers[1] = LogFactory.getLog("oozieops");
    }

    /**
     * Return the common prefix.
     *
     * @return the common prefix.
     */
    public String getMsgPrefix() {
        return prefix;
    }

    /**
     * Set the common prefix.
     *
     * @param prefix the common prefix to set.
     */
    public void setMsgPrefix(String prefix) {
        this.prefix = (prefix != null) ? prefix : "";
    }

    //All the methods from the commonsLogging Log interface will log to the default logger only.

    /**
     * Log a debug message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void debug(Object o) {
        log(Level.DEBUG, STD, "{0}", o);
    }

    /**
     * Log a debug message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void debug(Object o, Throwable throwable) {
        log(Level.DEBUG, STD, "{0}", o, throwable);
    }

    /**
     * Log a error message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void error(Object o) {
        log(Level.ERROR, STD, "{0}", o);
    }

    /**
     * Log a error message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void error(Object o, Throwable throwable) {
        log(Level.ERROR, STD, "{0}", o, throwable);
    }

    /**
     * Log a fatal message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void fatal(Object o) {
        log(Level.FATAL, STD, "{0}", o);
    }

    /**
     * Log a fatal message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void fatal(Object o, Throwable throwable) {
        log(Level.FATAL, STD, "{0}", o, throwable);
    }

    /**
     * Log a info message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void info(Object o) {
        log(Level.INFO, STD, "{0}", o);
    }

    /**
     * Log a info message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void info(Object o, Throwable throwable) {
        log(Level.INFO, STD, "{0}", o, throwable);
    }

    /**
     * Log a trace message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void trace(Object o) {
        log(Level.TRACE, STD, "{0}", o);
    }

    /**
     * Log a trace message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void trace(Object o, Throwable throwable) {
        log(Level.TRACE, STD, "{0}", o, throwable);
    }

    /**
     * Log a warn message to the common <code>Log</code>.
     *
     * @param o message.
     */
    @Override
    public void warn(Object o) {
        log(Level.WARN, STD, "{0}", o);
    }

    /**
     * Log a warn message and <code>Exception</code> to the common <code>Log</code>.
     *
     * @param o message.
     * @param throwable exception.
     */
    @Override
    public void warn(Object o, Throwable throwable) {
        log(Level.WARN, STD, "{0}", o, throwable);
    }

    /**
     * Return if debug logging is enabled.
     *
     * @return <code>true</code> if debug logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isDebugEnabled() {
        return isEnabled(Level.DEBUG, ALL);
    }

    /**
     * Return if error logging is enabled.
     *
     * @return <code>true</code> if error logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isErrorEnabled() {
        return isEnabled(Level.ERROR, ALL);
    }

    /**
     * Return if fatal logging is enabled.
     *
     * @return <code>true</code> if fatal logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isFatalEnabled() {
        return isEnabled(Level.FATAL, ALL);
    }

    /**
     * Return if info logging is enabled.
     *
     * @return <code>true</code> if info logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isInfoEnabled() {
        return isEnabled(Level.INFO, ALL);
    }

    /**
     * Return if trace logging is enabled.
     *
     * @return <code>true</code> if trace logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isTraceEnabled() {
        return isEnabled(Level.TRACE, ALL);
    }

    /**
     * Return if warn logging is enabled.
     *
     * @return <code>true</code> if warn logging is enable, <code>false</code> if not.
     */
    @Override
    public boolean isWarnEnabled() {
        return isEnabled(Level.WARN, ALL);
    }

    public enum Level {
        FATAL, ERROR, INFO, WARN, DEBUG, TRACE
    }

    private boolean isEnabled(Level level, int loggerMask) {
        for (int i = 0; i < loggers.length; i++) {
            if ((LOGGER_MASKS[i] & loggerMask) != 0) {
                boolean enabled = false;
                switch (level) {
                    case FATAL:
                        enabled = loggers[i].isFatalEnabled();
                        break;
                    case ERROR:
                        enabled = loggers[i].isErrorEnabled();
                        break;
                    case INFO:
                        enabled = loggers[i].isInfoEnabled();
                        break;
                    case WARN:
                        enabled = loggers[i].isWarnEnabled();
                        break;
                    case DEBUG:
                        enabled = loggers[i].isDebugEnabled();
                        break;
                    case TRACE:
                        enabled = loggers[i].isTraceEnabled();
                        break;
                }
                if (enabled) {
                    return true;
                }
            }
        }
        return false;
    }


    private void log(Level level, int loggerMask, String msgTemplate, Object... params) {
        loggerMask |= STD;
        if (isEnabled(level, loggerMask)) {
            String prefix = getMsgPrefix();
            prefix = (prefix != null && prefix.length() > 0) ? prefix + " " : "";

            String msg = prefix + format(msgTemplate, params);
            Throwable throwable = getCause(params);

            for (int i = 0; i < LOGGER_MASKS.length; i++) {
                if (isEnabled(level, loggerMask & LOGGER_MASKS[i])) {
                    Log log = loggers[i];
                    switch (level) {
                        case FATAL:
                            log.fatal(msg, throwable);
                            break;
                        case ERROR:
                            log.error(msg, throwable);
                            break;
                        case INFO:
                            log.info(msg, throwable);
                            break;
                        case WARN:
                            log.warn(msg, throwable);
                            break;
                        case DEBUG:
                            log.debug(msg, throwable);
                            break;
                        case TRACE:
                            log.trace(msg, throwable);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Log a fatal message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void fatal(String msgTemplate, Object... params) {
        log(Level.FATAL, STD, msgTemplate, params);
    }

    /**
     * Log a error message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void error(String msgTemplate, Object... params) {
        log(Level.ERROR, STD, msgTemplate, params);
    }

    /**
     * Log a info message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void info(String msgTemplate, Object... params) {
        log(Level.INFO, STD, msgTemplate, params);
    }

    /**
     * Log a warn message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void warn(String msgTemplate, Object... params) {
        log(Level.WARN, STD, msgTemplate, params);
    }

    /**
     * Log a debug message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void debug(String msgTemplate, Object... params) {
        log(Level.DEBUG, STD, msgTemplate, params);
    }

    /**
     * Log a trace message <code>Exception</code> to the common <code>Log</code>.
     *
     * @param msgTemplate message template.
     * @param params parameters for the message template. If the last parameter is an exception it is logged as such.
     */
    public void trace(String msgTemplate, Object... params) {
        log(Level.TRACE, STD, msgTemplate, params);
    }

    /**
     * Tee Log a fatal message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void fatal(int loggerMask, String msgTemplate, Object... params) {
        log(Level.FATAL, loggerMask, msgTemplate, params);
    }

    /**
     * Tee Log a error message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void error(int loggerMask, String msgTemplate, Object... params) {
        log(Level.ERROR, loggerMask, msgTemplate, params);
    }

    /**
     * Tee Log a info message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void info(int loggerMask, String msgTemplate, Object... params) {
        log(Level.INFO, loggerMask, msgTemplate, params);
    }

    /**
     * Tee Log a warn message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void warn(int loggerMask, String msgTemplate, Object... params) {
        log(Level.WARN, loggerMask, msgTemplate, params);
    }

    /**
     * Tee Log a debug message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void debug(int loggerMask, String msgTemplate, Object... params) {
        log(Level.DEBUG, loggerMask, msgTemplate, params);
    }

    /**
     * Tee Log a trace message <code>Exception</code> to the common log and specified <code>Log</code>s.
     *
     * @param loggerMask log mask, it is a bit mask, possible values are <code>APP</code> and <code>OPS</code>.
     * @param msgTemplate message template.
     * @param params parameters for the message template.
     */
    public void trace(int loggerMask, String msgTemplate, Object... params) {
        log(Level.TRACE, loggerMask, msgTemplate, params);
    }

    /**
     * Utility method that does uses the <code>StringFormat</code> to format the message template using the provided
     * parameters. <p/> In addition to the <code>StringFormat</code> syntax for message templates, it supports
     * <code>{E}</code> for ENTER. <p/> The last parameter is ignored for the formatting if it is an Exception.
     *
     * @param msgTemplate message template.
     * @param params paramaters to use in the template. If the last parameter is an Exception, it is ignored.
     * @return formatted message.
     */
    public static String format(String msgTemplate, Object... params) {
        ParamChecker.notEmpty(msgTemplate, "msgTemplate");
        msgTemplate = msgTemplate.replace("{E}", System.getProperty("line.separator"));
        if (params != null && params.length > 0) {
            msgTemplate = MessageFormat.format(msgTemplate, params);
        }
        return msgTemplate;
    }

    /**
     * Utility method that extracts the <code>Throwable</code>, if present, from the parameters.
     *
     * @param params parameters.
     * @return a <code>Throwable</code> instance if it is the last parameter, <code>null</code> otherwise.
     */
    public static Throwable getCause(Object... params) {
        Throwable throwable = null;
        if (params != null && params.length > 0 && params[params.length - 1] instanceof Throwable) {
            throwable = (Throwable) params[params.length - 1];
        }
        return throwable;
    }

}
