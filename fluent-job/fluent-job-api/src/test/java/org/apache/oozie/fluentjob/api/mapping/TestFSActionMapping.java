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

import com.google.common.collect.ImmutableList;
import org.apache.oozie.fluentjob.api.generated.workflow.FS;
import org.apache.oozie.fluentjob.api.action.Chgrp;
import org.apache.oozie.fluentjob.api.action.ChgrpBuilder;
import org.apache.oozie.fluentjob.api.action.Chmod;
import org.apache.oozie.fluentjob.api.action.ChmodBuilder;
import org.apache.oozie.fluentjob.api.action.Delete;
import org.apache.oozie.fluentjob.api.action.FSAction;
import org.apache.oozie.fluentjob.api.action.FSActionBuilder;
import org.apache.oozie.fluentjob.api.action.Mkdir;
import org.apache.oozie.fluentjob.api.action.Move;
import org.apache.oozie.fluentjob.api.action.Touchz;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFSActionMapping {
    @Test
    public void testMappingFSAction() {
        final String nameNode = "${nameNode}";
        final ImmutableList<String> jobXmls = new ImmutableList.Builder<String>().add("jobXml1").add("jobXml2").build();

        final Delete delete = new Delete("path", true);
        final Mkdir mkdir = new Mkdir("path");
        final Move move = new Move("from", "to");
        final Chmod chmod = new ChmodBuilder().withPermissions("511").build();
        final Touchz touchz = new Touchz("path");
        final Chgrp chgrp = new ChgrpBuilder().withGroup("user:group:").build();

        final FSActionBuilder builder = FSActionBuilder.create()
                .withNameNode(nameNode);

        for (final String jobXml : jobXmls) {
            builder.withJobXml(jobXml);
        }

        builder.withDelete(delete)
                .withMkdir(mkdir)
                .withMove(move)
                .withChmod(chmod)
                .withTouchz(touchz)
                .withChgrp(chgrp);

        final FSAction action = builder.build();

        final FS fsAction = DozerBeanMapperSingleton.instance().map(action, FS.class);

        assertEquals(nameNode, fsAction.getNameNode());
        assertEquals(jobXmls, fsAction.getJobXml());

        final List<Object> expectedList = Arrays.asList(delete, mkdir, move, chmod, touchz, chgrp);
        assertEquals(expectedList.size(), fsAction.getDeleteOrMkdirOrMove().size());
    }
}
