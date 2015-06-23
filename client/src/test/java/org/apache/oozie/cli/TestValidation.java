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

import junit.framework.TestCase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URI;
import java.io.File;

public class TestValidation extends TestCase {

    private String getPath(String resource) throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
        URI uri = url.toURI();
        File file = new File(uri.getPath());
        return file.getAbsolutePath();
    }

    public void testValid() throws Exception {
        String[] args = new String[]{"validate", getPath("valid.xml")};
        assertTrue(captureOutput(args).contains("Valid workflow-app"));
    }

    public void testInvalid() throws Exception {
        String[] args = new String[]{"validate", getPath("invalid.xml")};
        assertTrue(captureOutput(args).contains("Invalid app definition"));
    }

    private String captureOutput(String[] args) throws ParseException {
        OozieCLI cli = new OozieCLI();
        CLIParser parser = cli.getCLIParser();
        CLIParser.Command command = parser.parse(args);
        PrintStream original = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        String outStr = null;
        System.out.flush();
        try {
            System.setOut(ps);
            cli.validateCommandV41(command.getCommandLine());
            System.out.flush();
            outStr = baos.toString();
        } catch (OozieCLIException e) {
            outStr = e.getMessage();
        } finally {
            System.setOut(original);
            if (outStr != null) {
                System.out.print(outStr);
            }
            System.out.flush();
        }
        return outStr;
    }
}
