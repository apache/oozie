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

package org.apache.oozie;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.oozie.client.BundleJob.Timeunit;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleJobBean extends XTestCase {

    private static final Date ct = new Date(1L);
    private static final Date et = new Date(2L);
    private static final Date st = new Date(3L);
    private static final Date kt = new Date(4L);
    private static final Date lmt = new Date(5L);
    private static final Date pt = new Date(7L);
    private static final Date spt = new Date(8L);

    private void set(BundleJobBean bean) {
        bean.setAppName("an");
        bean.setAppPath("ap");
        bean.setConf("c");
        bean.setConsoleUrl("cu");
        bean.setCreatedTime(ct);
        bean.setEndTime(et);
        bean.setEndTimestamp(DateUtils.convertDateToTimestamp(et));
        bean.setExternalId("ei");
        bean.setGroup("group");
        bean.setId("id");
        bean.setJobXml("jx");
        bean.setKickoffTime(kt);
        bean.setKickoffTimestamp(DateUtils.convertDateToTimestamp(kt));
        bean.setLastModifiedTime(lmt);
        bean.setLastModifiedTimestamp(DateUtils.convertDateToTimestamp(lmt));
        bean.setOrigJobXml("ojx");
        bean.setPauseTime(pt);
        bean.setPending();
        bean.setStartTime(st);
        bean.setStartTimestamp(DateUtils.convertDateToTimestamp(st));
        bean.setStatus(Status.KILLED);
        bean.setSuspendedTime(spt);
        bean.setSuspendedTimestamp(DateUtils.convertDateToTimestamp(spt));
        bean.setTimeOut(11);
        bean.setTimeUnit(Timeunit.MINUTE);
        bean.setUser("u");
    }

    /**
     * Test {@link BundleJobBean} get- and set- methods.
     */
    public void testSetGet() {
        final BundleJobBean bean = new BundleJobBean();
        set(bean);
        _testGet(bean, true);
    }

    @SuppressWarnings("deprecation")
    private void _testGet(BundleJobBean bean, boolean checkAllFields) {
        assertEquals("an", bean.getAppName());
        assertEquals("ap", bean.getAppPath());
        assertEquals("c", bean.getConf());
        if (checkAllFields) {
            assertEquals("cu", bean.getConsoleUrl());
            assertEquals(ct, bean.getCreatedTime());
        }
        assertEquals(et, bean.getEndTime());
        assertEquals(DateUtils.convertDateToTimestamp(et), bean.getEndTimestamp());
        assertEquals("ei", bean.getExternalId());
        assertEquals("group", bean.getGroup());
        assertEquals("id", bean.getId());
        if (checkAllFields) {
            assertEquals("jx", bean.getJobXml());
        }
        assertEquals(kt, bean.getKickoffTime());
        assertEquals(DateUtils.convertDateToTimestamp(kt), bean.getKickoffTimestamp());
        if (checkAllFields) {
            assertEquals(lmt, bean.getLastModifiedTime());
            assertEquals(DateUtils.convertDateToTimestamp(lmt), bean.getLastModifiedTimestamp());
        }
        if (checkAllFields) {
            assertEquals("ojx", bean.getOrigJobXml());
            assertEquals(pt, bean.getPauseTime());
            assertEquals(true, bean.isPending());
        }
        assertEquals(st, bean.getStartTime());
        assertEquals(Status.KILLED, bean.getStatus());
        if (checkAllFields) {
            assertEquals(DateUtils.convertDateToTimestamp(spt), bean.getSuspendedTimestamp());
        }
        assertEquals(11, bean.getTimeout());
        assertEquals(Timeunit.MINUTE, bean.getTimeUnit());
        assertEquals("u", bean.getUser());
    }

    /**
     * Test {@link BundleJobBean} serialization and deserialization.
     */
    public void testSerialization() throws IOException {
        final BundleJobBean bean = new BundleJobBean();

        set(bean);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bean.write(dos);
        dos.close();

        final BundleJobBean bean2 = new BundleJobBean();
        bean2.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

        _testGet(bean2, false);
    }
}
