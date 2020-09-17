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

package org.apache.oozie.fluentjob.api.action;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestChgrpBuilder extends TestChBaseBuilder<Chgrp, ChgrpBuilder> {
    @Override
    protected ChgrpBuilder getBuilderInstance() {
        return new ChgrpBuilder();
    }

    @Test
    public void testWithGroup() {
        final String group = "root";

        final ChgrpBuilder builder = new ChgrpBuilder();
        builder.withGroup(group);

        final Chgrp chgrp = builder.build();
        assertEquals(group, chgrp.getGroup());
    }

    @Test
    public void testWithPermissionsCalledTwiceThrows() {
        final String group1 = "group1";
        final String group2 = "group1";

        final ChgrpBuilder builder = new ChgrpBuilder();
        builder.withGroup(group1);

        expectedException.expect(IllegalStateException.class);
        builder.withGroup(group2);
    }
}
