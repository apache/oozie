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

package org.apache.oozie.action.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ShellMain extends LauncherMain {
    public static final String CONF_OOZIE_SHELL_ARGS = "oozie.shell.args";
    public static final String CONF_OOZIE_SHELL_EXEC = "oozie.shell.exec";
    public static final String CONF_OOZIE_SHELL_ENVS = "oozie.shell.envs";
    public static final String CONF_OOZIE_SHELL_CAPTURE_OUTPUT = "oozie.shell.capture-output";
    public static final String OOZIE_ACTION_CONF_XML = "OOZIE_ACTION_CONF_XML";

    /**
     * @param args Invoked from LauncherMapper:map()
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        run(ShellMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {

        Configuration actionConf = loadActionConf();

        int exitCode = execute(actionConf);
        if (exitCode != 0) {
            // Shell command failed. therefore make the action failed
            throw new LauncherMainException(1);
        }

    }

    /**
     * Execute the shell command
     *
     * @param actionConf
     * @return command exit value
     * @throws IOException
     */
    private int execute(Configuration actionConf) throws Exception {
        String exec = getExec(actionConf);
        List<String> args = getShellArguments(actionConf);
        ArrayList<String> cmdArray = getCmdList(exec, args.toArray(new String[args.size()]));
        ProcessBuilder builder = new ProcessBuilder(cmdArray);
        Map<String, String> envp = getEnvMap(builder.environment(), actionConf);

        // Getting the Ccurrent working dir and setting it to processbuilder
        File currDir = new File("dummy").getAbsoluteFile().getParentFile();
        System.out.println("Current working dir " + currDir);
        builder.directory(currDir);

        printCommand(cmdArray, envp); // For debugging purpose

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Shell command line now >>");
        System.out.println();
        System.out.flush();

        boolean captureOutput = actionConf.getBoolean(CONF_OOZIE_SHELL_CAPTURE_OUTPUT, false);

        // Execute the Command
        Process p = builder.start();

        Thread[] thrArray = handleShellOutput(p, captureOutput);

        int exitValue = p.waitFor();
        // Wait for both the thread to exit
        if (thrArray != null) {
            for (Thread thr : thrArray) {
                thr.join();
            }
        }
        System.out.println("Exit code of the Shell command " + exitValue);
        System.out.println("<<< Invocation of Shell command completed <<<");
        System.out.println();
        return exitValue;
    }

    /**
     * Return the environment variable to pass to in shell command execution.
     *
     */
    private Map<String, String> getEnvMap(Map<String, String> envp, Configuration actionConf) {
        // Adding user-specified environments
        String[] envs = MapReduceMain.getStrings(actionConf, CONF_OOZIE_SHELL_ENVS);
        for (String env : envs) {
            String[] varValue = env.split("=",2); // Error case is handled in
                                                // ShellActionExecutor
            envp.put(varValue[0], varValue[1]);
        }
        // Adding action.xml to env
        envp.put(OOZIE_ACTION_CONF_XML, System.getProperty("oozie.action.conf.xml", ""));
        return envp;
    }

    /**
     * Get the shell commands with the arguments
     *
     * @param exec
     * @param args
     * @return command and list of args
     */
    private ArrayList<String> getCmdList(String exec, String[] args) {
        ArrayList<String> cmdArray = new ArrayList<String>();
        cmdArray.add(exec); // Main executable
        for (String arg : args) { // Adding rest of the arguments
            cmdArray.add(arg);
        }
        return cmdArray;
    }

    /**
     * Print the output written by the Shell execution in its stdout/stderr.
     * Also write the stdout output to a file for capturing.
     *
     * @param p process
     * @param captureOutput indicates if STDOUT should be captured or not.
     * @return Array of threads (one for stdout and another one for stderr
     *         processing
     * @throws IOException thrown if an IO error occurrs.
     */
    protected Thread[] handleShellOutput(Process p, boolean captureOutput)
            throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        OutputWriteThread thrStdout = new OutputWriteThread(input, true, captureOutput);
        thrStdout.setDaemon(true);
        thrStdout.start();

        OutputWriteThread thrStderr = new OutputWriteThread(error, false, false);
        thrStderr.setDaemon(true);
        thrStderr.start();

        return new Thread[]{ thrStdout, thrStderr };
    }

    /**
     * Thread to write output to LM stdout/stderr. Also write the content for
     * capture-output.
     */
    class OutputWriteThread extends Thread {
        BufferedReader reader = null;
        boolean isStdout = false;
        boolean needCaptured = false;

        public OutputWriteThread(BufferedReader reader, boolean isStdout, boolean needCaptured) {
            this.reader = reader;
            this.isStdout = isStdout;
            this.needCaptured = needCaptured;
        }

        @Override
        public void run() {
            String line;
            BufferedWriter os = null;

            try {
                if (needCaptured) {
                    File file = new File(System.getProperty("oozie.action.output.properties"));
                    os = new BufferedWriter(new FileWriter(file));
                }
                while ((line = reader.readLine()) != null) {
                    if (isStdout) { // For stdout
                        // 1. Writing to LM STDOUT
                        System.out.println("Stdoutput " + line);
                        // 2. Writing for capture output
                        if (os != null) {
                            os.write(line);
                            os.newLine();
                        }
                    }
                    else {
                        System.err.println(line); // 1. Writing to LM STDERR
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Stdout/Stderr read/write error :" + e);
            }finally {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                    //NOP ignoring error on close of STDOUT/STDERR
                }
                if (os != null) {
                    try {
                        os.close();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to close the file stream :" + e);
                    }
                }
            }
        }
    }

    /**
     * Print the command including the arguments as well as the environment
     * setup
     *
     * @param cmdArray :Command Array
     * @param envp :Environment array
     */
    protected void printCommand(ArrayList<String> cmdArray, Map<String, String> envp) {
        int i = 0;
        System.out.println("Full Command .. ");
        System.out.println("-------------------------");
        for (String arg : cmdArray) {
            System.out.println(i++ + ":" + arg + ":");
        }

        if (envp != null) {
            System.out.println("List of passing environment");
            System.out.println("-------------------------");
            for (Map.Entry<String, String> entry : envp.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue() + ":");
            }
        }

    }

    /**
     * Retrieve the list of arguments that were originally specified to
     * Workflow.xml.
     *
     * @param actionConf
     * @return argument list
     */
    protected List<String> getShellArguments(Configuration actionConf) {
        List<String> arguments = new ArrayList<String>();
        String[] scrArgs = MapReduceMain.getStrings(actionConf, CONF_OOZIE_SHELL_ARGS);
        for (String scrArg : scrArgs) {
            arguments.add(scrArg);
        }
        return arguments;
    }

    /**
     * Retrieve the executable name that was originally specified to
     * Workflow.xml.
     *
     * @param actionConf
     * @return executable
     */
    protected String getExec(Configuration actionConf) {
        String exec = actionConf.get(CONF_OOZIE_SHELL_EXEC);

        if (exec == null) {
            throw new RuntimeException("Action Configuration does not have " + CONF_OOZIE_SHELL_EXEC + " property");
        }
        return exec;
    }

    /**
     * Read action configuration passes through action xml file.
     *
     * @return action  Configuration
     * @throws IOException
     */
    protected Configuration loadActionConf() throws IOException {
        System.out.println();
        System.out.println("Oozie Shell action configuration");
        System.out.println("=================================================================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));
        logMasking("Shell configuration:", new HashSet<String>(), actionConf);
        return actionConf;
    }
}
