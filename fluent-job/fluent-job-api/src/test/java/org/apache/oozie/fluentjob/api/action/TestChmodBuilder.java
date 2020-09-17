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

public class TestChmodBuilder extends TestChBaseBuilder<Chmod, ChmodBuilder> {
    @Override
    protected ChmodBuilder getBuilderInstance() {
        return new ChmodBuilder();
    }

    @Test
    public void testWithPermissions() {
        final String permission = "-rwxrw-rw-";

        final ChmodBuilder builder = new ChmodBuilder();
        builder.withPermissions(permission);

        final Chmod chmod = builder.build();
        assertEquals(permission, chmod.getPermissions());
    }

    @Test
    public void testWithPermissionsCalledTwiceThrows() {
        final String permission1 = "-rwxrw-rw-";
        final String permission2 = "-rwxrw-r-";

        final ChmodBuilder builder = new ChmodBuilder();
        builder.withPermissions(permission1);

        expectedException.expect(IllegalStateException.class);
        builder.withPermissions(permission2);
    }
}
