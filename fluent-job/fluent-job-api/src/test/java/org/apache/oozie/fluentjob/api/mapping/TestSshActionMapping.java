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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.action.SshAction;
import org.apache.oozie.fluentjob.api.action.SshActionBuilder;
import org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestSshActionMapping {

    public static final String DEFAULT = "default";

    @Test
    public void testMappingSshAction() {
        final List<String> args = Arrays.asList("arg1", "arg2");

        final SshActionBuilder builder = SshActionBuilder.create();

        builder.withHost(DEFAULT)
                .withCommand(DEFAULT)
                .withCaptureOutput(true);

        for (final String arg : args) {
            builder.withArg(arg);
        }

        final SshAction action = builder.build();

        final ACTION ssh = DozerBeanMapperSingleton.instance().map(action, ACTION.class);

        assertEquals(DEFAULT, ssh.getHost());
        assertEquals(DEFAULT, ssh.getCommand());
        assertEquals(args, ssh.getArgs());
        assertNotNull(ssh.getCaptureOutput());
    }
}
