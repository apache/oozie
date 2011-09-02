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
package org.apache.oozie;

import org.apache.oozie.util.XLog;

public enum ErrorCode {
    E0001(XLog.OPS, "Could not create runtime directory, {0}"),
    E0002(XLog.STD, "System is in safe mode"),

    E0010(XLog.OPS, "Could not initialize log service, {0}"),

    E0020(XLog.OPS, "Configuration directory must be an absolute path [{0}]"),
    E0022(XLog.OPS, "Configuration file must be a file name [{0}]"),
    E0023(XLog.OPS, "Default configuration file not found in classpath [{0}]"),
    E0024(XLog.OPS, "Configuration file could not be read [{0}], {1}"),
    E0025(XLog.OPS, "Configuration service internal error, it should never happen, {0}"),

    E0100(XLog.OPS, "Could not initialize service [{0}], {1}"),
    E0110(XLog.OPS, "Could not parse or validate EL definition [{0}], {1}"),
    E0111(XLog.OPS, "class#method not found [{0}#{1}]"),
    E0112(XLog.OPS, "class#method does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0113(XLog.OPS, "class not found [{0}]"),
    E0114(XLog.OPS, "class#constant does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0115(XLog.OPS, "class#constant not found"),
    E0116(XLog.OPS, "class#constant does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0120(XLog.OPS, "UUID, invalid generator type [{0}]"),
    E0130(XLog.OPS, "Could not parse workflow schemas file/s, {0}"),
    E0131(XLog.OPS, "Could not read workflow schemas file/s, {0}"),
    E0140(XLog.OPS, "Could not access database, {0}"),
    E0141(XLog.OPS, "Could not create DataSource connection pool, {0}"),
    E0150(XLog.OPS, "Actionexecutor type already registered [{0}]"),
    E0160(XLog.OPS, "Could not read admin users file [{0}], {1}"),

    E0300(XLog.STD, "Invalid content-type [{0}]"),
    E0301(XLog.STD, "Invalid resource [{0}]"),
    E0302(XLog.STD, "Invalid parameter [{0}]"),
    E0303(XLog.STD, "Invalid parameter value, [{0}] = [{1}]"),
    E0304(XLog.STD, "Invalid parameter type, parameter [{0}] expected type [{1}]"),
    E0305(XLog.STD, "Missing parameter [{0}]"),

    E0400(XLog.STD, "User mismatch, request user [{0}] configuration user [{1}]"),
    E0401(XLog.STD, "Missing configuration property [{0}]"),
    E0402(XLog.STD, "Invalid callback ID [{0}]"),
    E0403(XLog.STD, "Invalid callback data, {0}"),

    E0420(XLog.STD, "Invalid jobs filter [{0}], {1}"),

    E0500(XLog.OPS, "Not authorized, {0}"),
    E0501(XLog.OPS, "Could not perform authorization operation, {0}"),
    E0502(XLog.OPS, "User [{0}] does not belong to group [{1}]"),
    E0503(XLog.OPS, "User [{0}] does not have admin privileges"),
    E0504(XLog.OPS, "Workflow app directory [{0}] does not exist"),
    E0505(XLog.OPS, "Workflow app definition [{0}] does not exist"),
    E0506(XLog.OPS, "Workflow app definition [{0}] is not a file"),
    E0507(XLog.OPS, "Could not access to [{0}], {1}"),
    E0508(XLog.OPS, "User [{0}] not authorized for job [{1}]"),


    E0600(XLog.OPS, "Could not get connection, {0}"),
    E0601(XLog.OPS, "Could not close connection, {0}"),
    E0602(XLog.OPS, "Could not commit connection, {0}"),
    E0603(XLog.OPS, "SQL error in operation [{0}], {1}"),
    E0604(XLog.STD, "Job does not exist [{0}]"),
    E0605(XLog.STD, "Action does not exist [{0}]"),
    E0606(XLog.STD, "Could not get lock [{0}]"),
    E0607(XLog.OPS, "Other error in operation [{0}], {1}"),

    E0700(XLog.STD, "XML error, {0}"),
    E0701(XLog.STD, "XML schema error, {0}"),
    E0702(XLog.STD, "IO error, {0}"),
    E0703(XLog.STD, "Invalid workflow element [{0}]"),
    E0704(XLog.STD, "Definition already complete, application [{0}]"),
    E0705(XLog.STD, "Nnode already defined, node [{0}]"),
    E0706(XLog.STD, "Node cannot transition to itself node [{0}]"),
    E0707(XLog.STD, "Loop detected at parsing, node [{0}]"),
    E0708(XLog.STD, "Invalid transition, node [{0}] transition [{1}]"),
    E0709(XLog.STD, "Loop detected at runtime, node [{0}]"),
    E0710(XLog.STD, "Could not read the workflow definition, {0}"),
    E0711(XLog.STD, "Invalid application URI [{0}], {1}"),
    E0712(XLog.STD, "Could not create lib paths list for application [{0}], {1}"),
    E0713(XLog.OPS, "SQL error, {0}"),
    E0714(XLog.OPS, "Workflow lib error, {0}"),
    E0715(XLog.OPS, "Invalid signal value for the start node, signal [{0}]"),
    E0716(XLog.OPS, "Workflow not running"),
    E0717(XLog.OPS, "Workflow not suspended"),
    E0718(XLog.OPS, "Workflow already completed"),
    E0719(XLog.OPS, "Job already started"),
    E0720(XLog.OPS, "Fork/join mismatch, node [{0}]"),
    E0721(XLog.OPS, "Invalid signal/transition, decision node [{0}] signal [{1}]"),
    E0722(XLog.OPS, "Action signals can only be OK or ERROR, action node [{0}]"),
    E0723(XLog.STD, "Unsupported action type, node [{0}] type [{1}]"),
    E0724(XLog.STD, "Invalid node name, {0}"),

    E0800(XLog.STD, "Action it is not running its in [{1}] state, action [{0}]"),
    E0801(XLog.STD, "Workflow already running, workflow [{0}]"),
    E0802(XLog.STD, "Invalid action type [{0}]"),
    E0803(XLog.STD, "IO error, {0}"),
    E0804(XLog.STD, "Disallowed default property [{0}]"),
    E0805(XLog.STD, "Workflow job not completed, status [{0}]"),
    E0806(XLog.STD, "Action did not complete in previous run, action [{0}]"),
    E0807(XLog.STD, "Some skip actions were not executed [{0}]"),

    ETEST(XLog.STD, "THIS SHOULD HAPPEN ONLY IN TESTING, invalid job id [{0}]"),
    ;

    private String template;
    private int logMask;

    /**
     * Create an error code.
     *
     * @param template template for the exception message.
     * @param logMask  log mask for the exception.
     */
    private ErrorCode(int logMask, String template) {
        this.logMask = logMask;
        this.template = template;
    }

    /**
     * Return the message (StringFormat) template for the error code.
     *
     * @return message template.
     */
    public String getTemplate() {
        return template;
    }

    /**
     * Return the log mask (to which log the exception should be logged) of the error.
     *
     * @return error log mask.
     */
    public int getLogMask() {
        return logMask;
    }

    /**
     * Return a templatized error message for the error code.
     *
     * @param args the parameters for the templatized message.
     * @return error message.
     */
    public String format(Object ... args) {
        return XLog.format("{0}: {1}", toString(), XLog.format(getTemplate(), args));
    }

}
