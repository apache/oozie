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

public enum ErrorCode {
    E0000(XLog.STD, "System property 'oozie.home.dir' not defined"),
    E0001(XLog.OPS, "Could not create runtime directory, {0}"),
    E0002(XLog.STD, "System is in safe mode"),
    E0003(XLog.OPS, "Oozie home directory must be an absolute path [{0}]"),
    E0004(XLog.OPS, "Oozie home directory does not exist [{0}]"),

    E0010(XLog.OPS, "Could not initialize log service, {0}"),
    E0011(XLog.OPS, "Log4j file must be a file name [{0}]"),
    E0012(XLog.OPS, "Log4j file must be a '.properties' file [{0}]"),
    E0013(XLog.OPS, "Log4j file [{0}] not found in configuration dir [{1}] neither in classpath"),

    E0020(XLog.OPS, "Environment variable {0} not defined"),
    E0022(XLog.OPS, "Configuration file must be a file name [{0}]"),
    E0023(XLog.OPS, "Default configuration file [{0}] not found in classpath"),
    E0024(XLog.OPS, "Oozie configuration directory does not exist [{0}]"),
    E0025(XLog.OPS, "Configuration service internal error, it should never happen, {0}"),
    E0026(XLog.OPS, "Missing required configuration property [{0}]"),

    E0100(XLog.OPS, "Could not initialize service [{0}], {1}"),
    E0101(XLog.OPS, "Service [{0}] does not implement declared interface [{1}]"),
    E0102(XLog.OPS, "Could not instanciate service class [{0}], {1}"),
    E0103(XLog.OPS, "Could not load service classes, {0}"),
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
    E0306(XLog.STD, "Invalid parameter"),
    E0307(XLog.STD, "Runtime error [{0}]"),
    E0308(XLog.STD, "Could not parse date range parameter [{0}]"),


    E0400(XLog.STD, "User mismatch, request user [{0}] configuration user [{1}]"),
    E0401(XLog.STD, "Missing configuration property [{0}]"),
    E0402(XLog.STD, "Invalid callback ID [{0}]"),
    E0403(XLog.STD, "Invalid callback data, {0}"),
    E0404(XLog.STD, "Only one of the properties are allowed [{0}]"),

    E0420(XLog.STD, "Invalid jobs filter [{0}], {1}"),
    E0421(XLog.STD, "Invalid job filter [{0}], {1}"),

    E0500(XLog.OPS, "Not authorized, {0}"),
    E0501(XLog.OPS, "Could not perform authorization operation, {0}"),
    E0502(XLog.OPS, "User [{0}] does not belong to group [{1}]"),
    E0503(XLog.OPS, "User [{0}] does not have admin privileges"),
    E0504(XLog.OPS, "App directory [{0}] does not exist"),
    E0505(XLog.OPS, "App definition [{0}] does not exist"),
    E0506(XLog.OPS, "App definition [{0}] is not a file"),
    E0507(XLog.OPS, "Could not access to [{0}], {1}"),
    E0508(XLog.OPS, "User [{0}] not authorized for WF job [{1}]"),
    E0509(XLog.OPS, "User [{0}] not authorized for Coord job [{1}]"),
    E0510(XLog.OPS, "Unable to get Credential [{0}]"),

    E0550(XLog.OPS, "Could not normalize host name [{0}], {1}"),
    E0551(XLog.OPS, "Missing [{0}] property"),

    E0600(XLog.OPS, "Could not get connection, {0}"),
    E0601(XLog.OPS, "Could not close connection, {0}"),
    E0602(XLog.OPS, "Could not commit connection, {0}"),
    E0603(XLog.OPS, "SQL error in operation [{0}], {1}"),
    E0604(XLog.STD, "Job does not exist [{0}]"),
    E0605(XLog.STD, "Action does not exist [{0}]"),
    E0606(XLog.STD, "Could not get lock [{0}], timed out [{1}]ms"),
    E0607(XLog.OPS, "Other error in operation [{0}], {1}"),
    E0608(XLog.OPS, "JDBC setup error [{0}], {1}"),
    E0609(XLog.OPS, "Missing [{0}] ORM file [{1}]"),
    E0610(XLog.OPS, "Missing JPAService, StoreService cannot run without a JPAService"),

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
    E0725(XLog.STD, "Workflow instance can not be killed, {0}"),
    E0726(XLog.STD, "Workflow action can not be killed, {0}"),
    E0727(XLog.STD, "Workflow Job can not be suspended as its not in running state, {0}, Status: {1}"),
    E0728(XLog.STD, "Coordinator Job can not be suspended as job finished or failed or killed, id : {0}, status : {1}"),
    E0729(XLog.OPS, "Kill node message [{0}]"),
    E0730(XLog.STD, "Fork/Join not in pair"),
    E0731(XLog.STD, "Fork node [{0}] cannot have less than two paths"),
    E0732(XLog.STD, "Fork [{0}]/Join [{1}] not in pair"),
    E0733(XLog.STD, "Fork [{0}] without a join"),
    E0734(XLog.STD, "Invalid transition from node [{0}] to node [{1}] while using fork/join"),
    E0735(XLog.STD, "There was an invalid \"error to\" transition to node [{1}] while using fork/join"),
    E0736(XLog.STD, "Workflow definition lenght [{0}] exceeded maximum allowed length [{1}]"),

    E0800(XLog.STD, "Action it is not running its in [{1}] state, action [{0}]"),
    E0801(XLog.STD, "Workflow already running, workflow [{0}]"),
    E0802(XLog.STD, "Invalid action type [{0}]"),
    E0803(XLog.STD, "IO error, {0}"),
    E0804(XLog.STD, "Disallowed default property [{0}]"),
    E0805(XLog.STD, "Workflow job not completed, status [{0}]"),
    E0806(XLog.STD, "Action did not complete in previous run, action [{0}]"),
    E0807(XLog.STD, "Some skip actions were not executed [{0}]"),
    E0808(XLog.STD, "Disallowed user property [{0}]"),
    E0809(XLog.STD, "Workflow action is not eligible to start [{0}]"),
    E0810(XLog.STD, "Job state is not [{0}]. Skipping ActionStart Execution"),
    E0811(XLog.STD, "Job state is not [{0}]. Skipping ActionEnd Execution"),
    E0812(XLog.STD, "Action pending=[{0}], status=[{1}]. Skipping ActionEnd Execution"),
    E0813(XLog.STD, "Workflow not RUNNING, current status [{0}]"),
    E0814(XLog.STD, "SignalCommand for action id=[{0}] is already processed, status=[{1}], , pending=[{2}]"),
    E0815(XLog.STD, "Action pending=[{0}], status=[{1}]. Skipping ActionCheck Execution"),
    E0816(XLog.STD, "Action pending=[{0}], status=[{1}]. Skipping ActionStart Execution"),
    E0817(XLog.STD, "The wf action [{0}] has been udated recently. Ignoring ActionCheck."),
    E0818(XLog.STD, "Action [{0}] status is running but WF Job [{1}] status is [{2}]. Expected status is RUNNING."),
    E0819(XLog.STD, "Unable to delete the temp dir of job WF Job [{0}]."),
    E0820(XLog.STD, "Action user retry max [{0}] is over system defined max [{1}], re-assign to use system max."),

    E0900(XLog.OPS, "Jobtracker [{0}] not allowed, not in Oozie's whitelist"),
    E0901(XLog.OPS, "Namenode [{0}] not allowed, not in Oozie's whitelist"),
    E0902(XLog.OPS, "Exception occured: [{0}]"),
    E0903(XLog.OPS, "Invalid JobConf, it has not been created by HadoopAccessorService"),

    E1001(XLog.STD, "Could not read the coordinator job definition, {0}"),
    E1002(XLog.STD, "Invalid coordinator application URI [{0}], {1}"),
    E1003(XLog.STD, "Invalid coordinator application attributes [{0}], {1}"),
    E1004(XLog.STD, "Expression language evaluation error [{0}], {1}"),
    E1005(XLog.STD, "Could not read the coordinator job configuration read from DB, {0}"),
    E1006(XLog.STD, "Invalid coordinator application [{0}], {1}"),
    E1007(XLog.STD, "Unable to add record to SLA table. [{0}], {1}"),
    E1008(XLog.STD, "Not implemented. [{0}]"),
    E1009(XLog.STD, "Unable to parse XML response. [{0}]"),
    E1010(XLog.STD, "Invalid data in coordinator xml. [{0}]"),
    E1011(XLog.STD, "Cannot update coordinator job [{0}], {1}"),
    E1012(XLog.STD, "Coord Job Materialization Error: {0}"),
    E1013(XLog.STD, "Coord Job Recovery Error: {0}"),
    E1014(XLog.STD, "Coord job change command not supported"),
    E1015(XLog.STD, "Invalid coordinator job change value {0}, {1}"),
    E1016(XLog.STD, "Cannot change a killed coordinator job"),
    E1017(XLog.STD, "Cannot change a workflow job"),
    E1018(XLog.STD, "Coord Job Rerun Error: {0}"),
    E1019(XLog.STD, "Could not submit coord job, [{0}]"),
    E1020(XLog.STD, "Could not kill coord job, this job either finished successfully or does not exist , [{0}]"),
    E1021(XLog.STD, "Coord Action Input Check Error: {0}"),

    E1100(XLog.STD, "Command precondition does not hold before execution, [{0}]"),

    E1101(XLog.STD, "SLA Nominal time is required."),
    E1102(XLog.STD, "SLA should-start can't be empty."),

    E1201(XLog.STD, "State [{0}] is invalid for job [{1}]."),

    E1301(XLog.STD, "Could not read the bundle job definition, [{0}]"),
    E1302(XLog.STD, "Invalid bundle application URI [{0}], {1}"),
    E1303(XLog.STD, "Invalid bundle application attributes [{0}], {1}"),
    E1304(XLog.STD, "Duplicate bundle application coordinator name [{0}]"),
    E1305(XLog.STD, "Empty bundle application coordinator name."),
    E1306(XLog.STD, "Could not read the bundle job configuration, [{0}]"),
    E1307(XLog.STD, "Could not read the bundle coord job configuration, [{0}]"),
    E1308(XLog.STD, "Bundle Action Status  [{0}] is not matching with coordinator previous status [{1}]."),
    E1309(XLog.STD, "Bundle Action for bundle ID  [{0}] and Coordinator [{1}] could not be update by BundleStatusUpdateXCommand"),
    E1310(XLog.STD, "Bundle Job submission Error: [{0}]"),
    E1311(XLog.STD, "Bundle Action for bundle ID  [{0}] could not be get."),
    E1312(XLog.STD, "Bundle Job can not be suspended as job finished or failed or killed, id : {0}, status : {1}"),
    E1313(XLog.STD, "Bundle Job can not be changed as job finished, {0}, Status: {1}"),
    E1314(XLog.STD, "Bundle Job can not be changed as job does not exist, {0}"),
    E1315(XLog.STD, "Bundle job can not be paused, {0}"),
    E1316(XLog.STD, "Bundle job can not be unpaused, {0}"),
    E1317(XLog.STD, "Invalid bundle job change value {0}, {1}"),
    E1318(XLog.STD, "No coord jobs for the bundle=[{0}], fail the bundle"),
    E1319(XLog.STD, "Invalid bundle coord job namespace, [{0}]"),

    E1400(XLog.STD, "doAs (proxyuser) failure"),

    ETEST(XLog.STD, "THIS SHOULD HAPPEN ONLY IN TESTING, invalid job id [{0}]"),;

    private String template;
    private int logMask;

    /**
     * Create an error code.
     *
     * @param template template for the exception message.
     * @param logMask log mask for the exception.
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
    public String format(Object... args) {
        return XLog.format("{0}: {1}", toString(), XLog.format(getTemplate(), args));
    }

}
