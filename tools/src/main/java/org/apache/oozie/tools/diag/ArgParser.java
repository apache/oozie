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

package org.apache.oozie.tools.diag;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.cli.OozieCLI;

import java.io.File;
import java.io.IOException;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Output directory is specified by user")
class ArgParser {

    private static final String OOZIE_OPTION = "oozie";
    private static final String NUM_WORKFLOWS_OPTION = "numworkflows";
    private static final String NUM_COORDS_OPTION = "numcoordinators";
    private static final String NUM_BUNDLES_OPTION = "numbundles";
    private static final String JOBS_OPTION = "jobs";
    private static final String MAX_CHILD_ACTIONS = "maxchildactions";
    private static final String OUTPUT_DIR_OPTION = "output";
    private final Options options = new Options();
    private CommandLine commandLine;

    public void setCommandLine(CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    private void addNewOption(String optionName, String argName, String details, boolean required) {
        addNewOption(optionName, argName, details, null, required, null, false);
    }

    private void addNewOption(String optionName, String argName, String details, Class type, boolean required) {
        addNewOption(optionName, argName, details, type, required, null, false);
    }

    private void addNewOption(String optionName, String argName, String details, Class type, boolean required,
                              Character valueSeparator, boolean isUnlimited) {
        final Option option = new Option(optionName, true,
                details);
        option.setRequired(required);
        option.setArgName(argName);
        if (type != null) {
            option.setType(type);
        }
        if (valueSeparator != null) {
            option.setValueSeparator(valueSeparator);
        }
        if (isUnlimited) {
            option.setArgs(Option.UNLIMITED_VALUES);
        }
        options.addOption(option);
    }

    Options setupOptions() {
        addNewOption(OOZIE_OPTION, "url", String.format("Required: Oozie URL (or specify with %s env var)",
                OozieCLI.ENV_OOZIE_URL),  true);

        addNewOption(NUM_WORKFLOWS_OPTION, "n",
                "Detailed information on the last n workflows will be collected (default: 0)", Integer.class, false);

        addNewOption(NUM_COORDS_OPTION, "n",
                "Detailed information on the last n Coordinators will be collected (default: 0)", Integer.class,
                false);

        addNewOption(NUM_BUNDLES_OPTION, "n",
                "Detailed information on the last n Bundles will be collected (default: 0)", Integer.class, false);

        addNewOption(JOBS_OPTION, "id ...",
                "Detailed information on the given job IDs will be collected (default: none)", null, false,
                ',', true);

        addNewOption(MAX_CHILD_ACTIONS, "n",
                "Maximum number of Workflow or Coordinator actions that will be collected (default: 10)", Integer.class,
                false);

        addNewOption(OUTPUT_DIR_OPTION, "dir", "Required: Directory to output the zip file",true);
        return options;
    }

    File ensureOutputDir() throws IOException {
        final String output = commandLine.getOptionValue(OUTPUT_DIR_OPTION);
        Preconditions.checkNotNull(output);

        final File outputDir = new File(output);
        if (!outputDir.isDirectory() && !outputDir.mkdirs()) {
            throw new IOException("Could not create output directory: " + outputDir.getAbsolutePath());
        }
        return outputDir;
    }

    Integer getMaxChildActions() {
        final Integer maxChildActions = Integer.valueOf(commandLine.getOptionValue(MAX_CHILD_ACTIONS, "10"));
        Preconditions.checkArgument(maxChildActions >= 0,
                MAX_CHILD_ACTIONS + " cannot be negative");
        return maxChildActions;
    }

    String[] getJobIds() {
        return commandLine.getOptionValues(JOBS_OPTION);
    }

    Integer getNumBundles() {
        final Integer numBundles = Integer.valueOf(commandLine.getOptionValue(NUM_BUNDLES_OPTION, "0"));
        Preconditions.checkArgument(numBundles >= 0,
                NUM_BUNDLES_OPTION + " cannot be negative");
        return numBundles;
    }

    Integer getNumCoordinators() {
        final Integer numCoords = Integer.valueOf(commandLine.getOptionValue(NUM_COORDS_OPTION, "0"));
        Preconditions.checkArgument(numCoords >= 0,
                NUM_COORDS_OPTION + " cannot be negative");
        return numCoords;
    }

    Integer getNumWorkflows() {
        final Integer numWorkflows = Integer.valueOf(commandLine.getOptionValue(NUM_WORKFLOWS_OPTION, "0"));
        Preconditions.checkArgument(numWorkflows >= 0,
                NUM_WORKFLOWS_OPTION + " cannot be negative");
        return numWorkflows;
    }

    String getOozieUrl() {
        String url = commandLine.getOptionValue(OOZIE_OPTION);
        if (url == null) {
            url = System.getenv(OozieCLI.ENV_OOZIE_URL);
            if (url == null) {
                throw new IllegalArgumentException(
                        "Oozie URL is not available neither in command option or in the environment");
            }
        }
        return url;
    }

    boolean parseCommandLineArguments(String[] args) {
        final CommandLineParser parser = new GnuParser();
        final Options options = setupOptions();

        try {
            commandLine = parser.parse(options, args);
        } catch (final ParseException pe) {
            System.err.print("Error: " + pe.getMessage());
            System.err.println();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("DiagBundleCollectorDriver",
                    "A tool that collects a diagnostic bundle of information from Oozie",
                    options, "", true);
            return false;
        }
        return true;
    }
}
