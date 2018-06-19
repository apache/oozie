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

import org.apache.oozie.fluentjob.api.generated.workflow.DELETE;
import org.apache.oozie.fluentjob.api.generated.workflow.MKDIR;
import org.apache.oozie.fluentjob.api.generated.workflow.PREPARE;
import org.apache.oozie.fluentjob.api.action.Prepare;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPrepareMapping {

    @Test
    public void testMappingPrepare() {
        final String deletePath1 = "path/to/delete/location1";
        final String deletePath2 = "path/to/delete/location2";

        final String mkdirPath1 = "path/to/mkdir/location1";
        final String mkdirPath2 = "path/to/mkdir/location2";

        final Prepare prepare = new PrepareBuilder()
                .withDelete(deletePath1, false)
                .withDelete(deletePath2, false)
                .withMkdir(mkdirPath1)
                .withMkdir(mkdirPath2)
                .build();

        final PREPARE prepareJAXB = DozerBeanMapperSingleton.instance().map(prepare, PREPARE.class);

        final List<DELETE> deletesJAXB = prepareJAXB.getDelete();
        final DELETE delete1JAXB = deletesJAXB.get(0);
        final DELETE delete2JAXB = deletesJAXB.get(1);

        final List<MKDIR> mkdirsJAXB = prepareJAXB.getMkdir();
        final MKDIR mkdir1JAXB = mkdirsJAXB.get(0);
        final MKDIR mkdir2JAXB = mkdirsJAXB.get(1);

        assertEquals(deletePath1, delete1JAXB.getPath());
        assertEquals(deletePath2, delete2JAXB.getPath());

        assertEquals(mkdirPath1, mkdir1JAXB.getPath());
        assertEquals(mkdirPath2, mkdir2JAXB.getPath());
    }
}
