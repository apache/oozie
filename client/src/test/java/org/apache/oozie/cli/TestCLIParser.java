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

import junit.framework.TestCase;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TestCLIParser extends TestCase {

    public void testEmptyParser() throws Exception {
        try {
            CLIParser parser = new CLIParser("oozie", new String[] {});
            CLIParser.Command c = parser.parse(new String[]{"a"});
            fail();
        }
        catch (ParseException ex) {
            //nop
        }
    }

    public void testCommandParser() throws Exception {
        try {
            CLIParser parser = new CLIParser("oozie", new String[] {});
            parser.addCommand("a", "<A>", "AAAAA", new Options(), false);
            CLIParser.Command c = parser.parse(new String[]{"a", "b"});
            assertEquals("a", c.getName());
            assertEquals("b", c.getCommandLine().getArgs()[0]);
        }
        catch (ParseException ex) {
            fail();
        }
    }

}
