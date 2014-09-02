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

package org.apache.oozie.cli;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.UnrecognizedOptionException;

import java.util.Arrays;
import java.util.Map;
import java.util.LinkedHashMap;
import java.text.MessageFormat;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

/**
 * Command line parser based on Apache common-cli 1.x that supports subcommands.
 */
public class CLIParser {
    private static final String LEFT_PADDING = "      ";

    private String cliName;
    private String[] cliHelp;
    private Map<String, Options> commands = new LinkedHashMap<String, Options>();
    private Map<String, Boolean> commandWithArgs = new LinkedHashMap<String, Boolean>();
    private Map<String, String> commandsHelp = new LinkedHashMap<String, String>();

    /**
     * Create a parser.
     *
     * @param cliName name of the parser, for help purposes.
     * @param cliHelp help for the CLI.
     */
    public CLIParser(String cliName, String[] cliHelp) {
        this.cliName = cliName;
        this.cliHelp = cliHelp;
    }

    /**
     * Add a command to the parser.
     *
     * @param command comand name.
     * @param argsHelp command arguments help.
     * @param commandHelp command description.
     * @param commandOptions command options.
     * @param hasArguments
     */
    public void addCommand(String command, String argsHelp, String commandHelp, Options commandOptions,
                           boolean hasArguments) {
        String helpMsg = argsHelp + ((hasArguments) ? "<ARGS> " : "") + ": " + commandHelp;
        commandsHelp.put(command, helpMsg);
        commands.put(command, commandOptions);
        commandWithArgs.put(command, hasArguments);
    }

    /**
     * Bean that represents a parsed command.
     */
    public class Command {
        private String name;
        private CommandLine commandLine;

        private Command(String name, CommandLine commandLine) {
            this.name = name;
            this.commandLine = commandLine;
        }

        /**
         * Return the command name.
         *
         * @return the command name.
         */
        public String getName() {
            return name;
        }

        /**
         * Return the command line.
         *
         * @return the command line.
         */
        public CommandLine getCommandLine() {
            return commandLine;
        }
    }

    /**
     * Parse a array of arguments into a command.
     *
     * @param args array of arguments.
     * @return the parsed Command.
     * @throws ParseException thrown if the arguments could not be parsed.
     */
    public Command parse(String[] args) throws ParseException {
        if (args.length == 0) {
            throw new ParseException("missing sub-command");
        }
        else {
            if (commands.containsKey(args[0])) {
                GnuParser parser ;
                String[] minusCommand = new String[args.length - 1];
                System.arraycopy(args, 1, minusCommand, 0, minusCommand.length);

                if (args[0].equals(OozieCLI.JOB_CMD)) {
                    validdateArgs(args, minusCommand);
                    parser = new OozieGnuParser(true);
                }
                else {
                    parser = new OozieGnuParser(false);
                }

                return new Command(args[0], parser.parse(commands.get(args[0]), minusCommand,
                                                         commandWithArgs.get(args[0])));
            }
            else {
                throw new ParseException(MessageFormat.format("invalid sub-command [{0}]", args[0]));
            }
        }
    }

    public void validdateArgs(final String[] args, String[] minusCommand) throws ParseException {
        try {
            GnuParser parser = new OozieGnuParser(false);
            parser.parse(commands.get(args[0]), minusCommand, commandWithArgs.get(args[0]));
        }
        catch (MissingOptionException e) {
            if (Arrays.toString(args).contains("-dryrun")) {
                // ignore this, else throw exception
                //Dryrun is also part of update sub-command. CLI parses dryrun as sub-command and throws
                //Missing Option Exception, if -dryrun is used as command. It's ok to skip exception only for dryrun.
            }
            else {
                throw e;
            }
        }
    }

    public String shortHelp() {
        return "use 'help [sub-command]' for help details";
    }

    /**
     * Print the help for the parser to standard output.
     * 
     * @param commandLine the command line
     */
    public void showHelp(CommandLine commandLine) {
        PrintWriter pw = new PrintWriter(System.out);
        pw.println("usage: ");
        for (String s : cliHelp) {
            pw.println(LEFT_PADDING + s);
        }
        pw.println();
        HelpFormatter formatter = new HelpFormatter();
        Set<String> commandsToPrint = commands.keySet();
        String[] args = commandLine.getArgs();
        if (args.length > 0 && commandsToPrint.contains(args[0])) {
            commandsToPrint = new HashSet<String>();
            commandsToPrint.add(args[0]);
        }
        for (String comm : commandsToPrint) {
            Options opts = commands.get(comm);
            String s = LEFT_PADDING + cliName + " " + comm + " ";
            if (opts.getOptions().size() > 0) {
                pw.println(s + "<OPTIONS> " + commandsHelp.get(comm));
                formatter.printOptions(pw, 100, opts, s.length(), 3);
            }
            else {
                pw.println(s + commandsHelp.get(comm));
            }
            pw.println();
        }
        pw.flush();
    }

    static class OozieGnuParser extends GnuParser {
        private boolean ignoreMissingOption;

        public OozieGnuParser(final boolean ignoreMissingOption) {
            this.ignoreMissingOption = ignoreMissingOption;
        }

        @Override
        protected void checkRequiredOptions() throws MissingOptionException {
            if (ignoreMissingOption) {
                return;
            }
            else {
                super.checkRequiredOptions();
            }
        }
    }

}


