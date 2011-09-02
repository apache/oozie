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
package org.apache.oozie.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import javax.xml.XMLConstants;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Oozie command line utility.
 */
public class OozieCLI {
    public static final String ENV_OOZIE_URL = "OOZIE_URL";
    public static final String WS_HEADER_PREFIX = "header:";

    public static final String HELP_CMD = "help";
    public static final String VERSION_CMD = "version";
    public static final String JOB_CMD = "job";
    public static final String JOBS_CMD = "jobs";
    public static final String ADMIN_CMD = "admin";
    public static final String VALIDATE_CMD = "validate";

    public static final String OOZIE_OPTION = "oozie";
    public static final String CONFIG_OPTION = "config";
    public static final String SUBMIT_OPTION = "submit";
    public static final String OFFSET_OPTION = "offset";
    public static final String START_OPTION = "start";
    public static final String RUN_OPTION = "run";
    public static final String SUSPEND_OPTION = "suspend";
    public static final String RESUME_OPTION = "resume";
    public static final String KILL_OPTION = "kill";
    public static final String RERUN_OPTION = "rerun";
    public static final String INFO_OPTION = "info";
    public static final String LEN_OPTION = "len";
    public static final String FILTER_OPTION = "filter";
    public static final String SAFEMODE_OPTION = "safemode";
    public static final String VERSION_OPTION = "version";
    public static final String STATUS_OPTION = "status";
    public static final String LOCAL_TIME_OPTION = "localtime";

    private static final String[] OOZIE_HELP =
            {"the env variable '" + ENV_OOZIE_URL + "' is used as default value for the '-" + OOZIE_OPTION + "' option",
             "custom headers for Oozie web services can be specified using '-D" + WS_HEADER_PREFIX + "NAME=VALUE'"
            };

    private static final String RULER;
    private static final int LINE_WIDTH = 184;

    private boolean used;

    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < LINE_WIDTH; i++) {
            sb.append("-");
        }
        RULER = sb.toString();
    }

    /**
     * Entry point for the Oozie CLI when invoked from the command line.
     * <p/>
     * Upon completion this method exits the JVM with '0' (success) or '-1' (failure).
     *
     * @param args options and arguments for the Oozie CLI.
     */
    public static void main(String[] args) {
        System.exit(new OozieCLI().run(args));
    }

    /**
     * Create an Oozie CLI instance.
     */
    public OozieCLI() {
        used = false;
    }

    /**
     * Return Oozie CLI top help lines.
     *
     * @return help lines.
     */
    protected String[] getCLIHelp() {
        return OOZIE_HELP;
    }

    private static Options createAdminOptions() {
        Option oozie = new Option(OOZIE_OPTION, true,        "Oozie URL");
        Option safe_mode = new Option(SAFEMODE_OPTION, true, "switch safemode on/off (true|false)");
        Option status = new Option(STATUS_OPTION, false,     "show the current system status");
        Option version = new Option(VERSION_OPTION, false,     "show Oozie server build version");
        Options adminOptions = new Options();
        adminOptions.addOption(oozie);
        OptionGroup group = new OptionGroup();
        group.addOption(safe_mode);
        group.addOption(status);
        group.addOption(version);
        adminOptions.addOptionGroup(group);
        return adminOptions;
    }

    private static Options createJobOptions() {
        Option oozie = new Option(OOZIE_OPTION, true,           "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true,         "job configuration file '.xml' or '.properties'");
        Option submit = new Option(SUBMIT_OPTION, false,        "submit a job (requires -config)");
        Option run = new Option(RUN_OPTION, false,              "run a job    (requires -config)");
        Option rerun = new Option(RERUN_OPTION, true,           "rerun a job  (requires -config)");
        Option start = new Option(START_OPTION, true,           "start a job");
        Option suspend = new Option(SUSPEND_OPTION, true,       "suspend a job");
        Option resume = new Option(RESUME_OPTION, true,         "resume a job");
        Option kill = new Option(KILL_OPTION, true,             "kill a job");
        Option info = new Option(INFO_OPTION, true,             "info of a job");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (default GMT)");
        OptionGroup actions = new OptionGroup();
        actions.addOption(submit);
        actions.addOption(start);
        actions.addOption(run);
        actions.addOption(suspend);
        actions.addOption(resume);
        actions.addOption(kill);
        actions.addOption(info);
        actions.addOption(rerun);
        actions.setRequired(true);
        Options jobOptions = new Options();
        jobOptions.addOption(oozie);
        jobOptions.addOption(config);
        jobOptions.addOption(localtime);
        jobOptions.addOptionGroup(actions);
        return jobOptions;
    }

    private static Options createJobsOptions() {
        Option oozie = new Option(OOZIE_OPTION, true,           "Oozie URL");
        Option start = new Option(OFFSET_OPTION, true,          "jobs offset (default '1')");
        Option len = new Option(LEN_OPTION, true,               "number of jobs (default '100')");
        Option filter = new Option(FILTER_OPTION, true,         "user=<U>;name=<N>;group=<G>;status=<S>;...");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (default GMT)");
        start.setType(Integer.class);
        len.setType(Integer.class);
        Options jobsOptions = new Options();
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(localtime);
        jobsOptions.addOption(start);
        jobsOptions.addOption(len);
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(filter);
        return jobsOptions;
    }

    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(String[] args) {
        if (used) {
            throw new IllegalStateException("CLI instance already used");
        }
        used = true;
        CLIParser parser = new CLIParser(OOZIE_OPTION, getCLIHelp());
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show client version", new Options(), false);
        parser.addCommand(JOB_CMD, "", "job operations", createJobOptions(), false);
        parser.addCommand(JOBS_CMD, "", "jobs status", createJobsOptions(), false);
        parser.addCommand(ADMIN_CMD, "", "admin operations", createAdminOptions(), false);
        parser.addCommand(VALIDATE_CMD, "", "validate a workflow XML file", new Options(), true);

        try {
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            }
            else if (command.getName().equals(JOB_CMD)) {
                jobCommand(command.getCommandLine());
            }
            else if (command.getName().equals(JOBS_CMD)) {
                jobsCommand(command.getCommandLine());
            }
            else if (command.getName().equals(ADMIN_CMD)) {
                adminCommand(command.getCommandLine());
            }
            else if (command.getName().equals(VERSION_CMD)) {
                versionCommand();
            }
            else if (command.getName().equals(VALIDATE_CMD)) {
                validateCommand(command.getCommandLine());
            }
            return 0;
        }
        catch (OozieCLIException ex) {
            System.err.println("Error: " + ex.getMessage());
            return -1;
        }
        catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return -1;
        }
        catch (Exception ex) {
            System.err.println(ex.getMessage());
            return -1;
        }
    }

    private String getOozieUrl(CommandLine commandLine) {
        String url = commandLine.getOptionValue(OOZIE_OPTION);
        if (url == null) {
            url = System.getenv(ENV_OOZIE_URL);
            if (url == null) {
                throw new IllegalArgumentException("Oozie URL is no available as option or in the environment");
            }
        }
        return url;
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parse(InputStream is, Properties conf) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(is);
            return parseDocument(doc, conf);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parseDocument(Document doc, Properties conf) throws IOException {
        try {
            Element root = doc.getDocumentElement();
            if (!"configuration".equals(root.getTagName())) {
                throw new RuntimeException("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (!"property".equals(prop.getTagName())) {
                    throw new RuntimeException("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }

                if (attr != null && value != null) {
                    conf.setProperty(attr, value);
                }
            }
            return conf;
        }
        catch (DOMException e) {
            throw new IOException(e);
        }
    }

    private Properties getConfiguration(CommandLine commandLine) throws IOException {
        Properties conf = new Properties();
        conf.setProperty("user.name", System.getProperty("user.name"));
        String configFile = commandLine.getOptionValue(CONFIG_OPTION);
        if (configFile == null) {
            throw new IOException("configuration file not specified");
        }
        else {
            File file = new File(configFile);
            if (!file.exists()) {
                throw new IOException("configuration file [" + configFile + "] not found");
            }
            if (configFile.endsWith(".properties")) {
                conf.load(new FileReader(file));
            }
            else if (configFile.endsWith(".xml")) {
                parse(new FileInputStream(configFile), conf);
            }
            else {
                throw new IllegalArgumentException("configuration must be a '.properties' or a '.xml' file");
            }
        }
        return conf;
    }

    /**
     * Create a OozieClient.
     * <p/>
     * It injects any '-Dheader:' as header to the the {@link org.apache.oozie.client.OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured workflow client.
     * @throws OozieCLIException thrown if the Oozie client could not be configured.
     */
    protected OozieClient createOozieClient(CommandLine commandLine) throws OozieCLIException {
        OozieClient wc = new OozieClient(getOozieUrl(commandLine));
        for (Map.Entry entry : System.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(WS_HEADER_PREFIX)) {
                String header = key.substring(WS_HEADER_PREFIX.length());
                wc.setHeader(header, (String) entry.getValue());
            }
        }
        return wc;
    }

    private static String JOB_ID_PREFIX = "job: ";

    private void jobCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        OozieClient wc = createOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            if (options.contains(SUBMIT_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.submit(getConfiguration(commandLine)));
            }
            else if (options.contains(START_OPTION)) {
                wc.start(commandLine.getOptionValue(START_OPTION));
            }
            else if (options.contains(SUSPEND_OPTION)) {
                wc.suspend(commandLine.getOptionValue(SUSPEND_OPTION));
            }
            else if (options.contains(RESUME_OPTION)) {
                wc.resume(commandLine.getOptionValue(RESUME_OPTION));
            }
            else if (options.contains(KILL_OPTION)) {
                wc.kill(commandLine.getOptionValue(KILL_OPTION));
            }
            else if (options.contains(RUN_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.run(getConfiguration(commandLine)));
            }
            else if (options.contains(RERUN_OPTION)) {
                wc.reRun(commandLine.getOptionValue(RERUN_OPTION), getConfiguration(commandLine));
            }
            else if (options.contains(INFO_OPTION)) {
                printJob(wc.getJobInfo(commandLine.getOptionValue(INFO_OPTION)), options.contains(LOCAL_TIME_OPTION));
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private static final String JOBS_FORMATTER = "%-32s%-22s%-11s%-5s%-10s%-10s%-24s%-24s%-24s%-23s";

    private static final String JOB_FORMATTER = "%-13s :  %-72s";

    private static final String ACTION_FORMATTER = "%-24s%-12s%-11s%-13s%-22s%-16s%-14s%-24s%-23s";

    private void printJob(WorkflowJob job, boolean localtime) throws IOException {
        System.out.println("Job Id: " + job.getId());
        System.out.println(RULER);

        System.out.println(String.format(JOB_FORMATTER, "Workflow Name", job.getAppName()));
        System.out.println(String.format(JOB_FORMATTER, "App Path", job.getAppPath()));
        System.out.println(String.format(JOB_FORMATTER, "Status", job.getStatus()));
        System.out.println(String.format(JOB_FORMATTER, "Run", job.getRun()));
        System.out.println(String.format(JOB_FORMATTER, "User", job.getUser()));
        System.out.println(String.format(JOB_FORMATTER, "Group", job.getGroup()));
        System.out.println(String.format(JOB_FORMATTER, "Created", maskDate(job.getCreatedTime(), localtime)));
        System.out.println(String.format(JOB_FORMATTER, "Started", maskDate(job.getStartTime(), localtime)));
        System.out.println(String.format(JOB_FORMATTER, "Last Modified", maskDate(job.getLastModTime(), localtime)));
        System.out.println(String.format(JOB_FORMATTER, "Ended", maskDate(job.getEndTime(), localtime)));

        List<WorkflowAction> actions = job.getActions();
        if(actions!=null && actions.size()>0){
            System.out.println();
            System.out.println("Actions");
            System.out.println(RULER);
            System.out.println(String.format(ACTION_FORMATTER,
                    "Action Name", "Type", "Status", "Transition", 
                    "Ext. Id", "Ext. Status", "Error Code", 
                    "Started", "Ended"));
            System.out.println(RULER);
            
            for(WorkflowAction action:job.getActions()){
                System.out.println(String.format(ACTION_FORMATTER, action.getName(),
                        action.getType(), action.getStatus(), maskIfNull(action.getTransition()),
                        maskIfNull(action.getExternalId()), maskIfNull(action.getExternalStatus()),
                        maskIfNull(action.getErrorCode()), maskDate(action.getStartTime(), localtime),
                        maskDate(action.getEndTime(), localtime)));
                System.out.println(RULER);
            }
        }
        else {
        	System.out.println(RULER);
        }
        System.out.println();
    }

    private void jobsCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        OozieClient wc = createOozieClient(commandLine);

        String filter = commandLine.getOptionValue(FILTER_OPTION);
        String s = commandLine.getOptionValue(OFFSET_OPTION);
        int start = Integer.parseInt((s != null) ? s : "0");
        s = commandLine.getOptionValue(LEN_OPTION);
        int len = Integer.parseInt((s != null) ? s : "0");

        try {
            printJobs(wc.getJobsInfo(filter, start, len), commandLine.hasOption(LOCAL_TIME_OPTION));
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void adminCommand(CommandLine commandLine) throws OozieCLIException {
        OozieClient wc = createOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            boolean status = false;
            if (options.contains(VERSION_OPTION)) {
                System.out.println("Oozie server build version: " + wc.getServerBuildVersion());
            }
            else {
                if (options.contains(SAFEMODE_OPTION)) {
                    String safeModeOption = commandLine.getOptionValue(SAFEMODE_OPTION);
                    try {
                        status = safeModeOption.equalsIgnoreCase("ON");
                    }
                    catch (Exception e) {
                        throw new OozieCLIException("Invalid input provided for option: " + SAFEMODE_OPTION);
                    }
                    wc.setSafeMode(status);

                }
                else if (options.contains(STATUS_OPTION)) {
                    status = wc.isInSafeMode();
                }
                System.out.println("Safemode: " + (status ? "ON" : "OFF"));
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void versionCommand() throws OozieCLIException {
        System.out.println("Oozie client build version: " +
                           BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION));
    }

    private void printJobs(List<WorkflowJob> jobs, boolean localtime) throws IOException {
        if(jobs!=null && jobs.size() > 0) {
            System.out.println(String.format(JOBS_FORMATTER, "Job Id", "Workflow Name", "Status", "Run", "User",
                    "Group", "Created", "Started", "Last Modified", "Ended"));
            System.out.println(RULER);

            for (WorkflowJob job : jobs) {

                System.out.println(String.format(JOBS_FORMATTER,
                        job.getId(), job.getAppName(), job.getStatus().toString(),
                        job.getRun(), job.getUser(), job.getGroup(),
                        maskDate(job.getCreatedTime(), localtime),
                        maskDate(job.getStartTime(), localtime),
                        maskDate(job.getLastModTime(), localtime),
                        maskDate(job.getEndTime(), localtime)));
            }
            System.out.println(RULER);
        } else {
            System.out.println("No Jobs match your criteria!");
        }
    }
    
    private String maskIfNull(String value){
        if(value!=null && value.length()>0){
            return value;
        }
        return "-";
    }
    
    private String maskDate(Date date, boolean isLocalTimeZone){
        if(date==null){
            return "-";
        }
        
        SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm Z", 
                Locale.US);
        if(!isLocalTimeZone){
        	dateFormater.setTimeZone(TimeZone.getTimeZone("GMT"));
        }
        return dateFormater.format(date);
    }

    private void validateCommand(CommandLine commandLine) throws OozieCLIException {
        String[] args = commandLine.getArgs();
        if (args.length != 1) {
            throw new OozieCLIException("One file must be specified");
        }
        File file = new File(args[0]);
        if (file.exists()) {
            try {
                List<StreamSource> sources = new ArrayList<StreamSource>();
                sources.add(new StreamSource(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.1.xsd")));
                SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Schema schema = factory.newSchema(sources.toArray(new StreamSource[sources.size()]));
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new FileReader(file)));
                System.out.println("Valid worflow-app");
            }
            catch (Exception ex) {
                throw new OozieCLIException("Invalid workflow-app, " + ex.toString(), ex);
            }
        }
        else {
            throw new OozieCLIException("File does not exists");
        }
    }

}
