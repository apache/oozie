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
package org.apache.oozie.util;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

public class TestDateUtils {

    @After
    public void resetToUTC() {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT);
        DateUtils.setConf(conf);
    }

    @Test
    public void testOozieInUTC() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT);
        DateUtils.setConf(conf);
        String s1 = "2012-08-08T12:42Z";
        Date d = DateUtils.parseDateOozieTZ(s1);
        Assert.assertNotNull(d);
        String s2 = DateUtils.formatDateOozieTZ(d);
        Assert.assertEquals(s1, s2);
    }

    @Test
    public void testOozieInOtherTZ() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "GMT-0300");
        DateUtils.setConf(conf);
        String s1 = "2012-08-08T12:42-0300";
        Date d = DateUtils.parseDateOozieTZ(s1);
        Assert.assertNotNull(d);
        String s2 = DateUtils.formatDateOozieTZ(d);
        Assert.assertEquals(s1, s2);
    }

    @Test(expected = ParseException.class)
    public void testOozieInOtherTZIncorrectOffset() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "GMT-0300");
        DateUtils.setConf(conf);
        String s1 = "2012-08-08T12:42-0400";
        DateUtils.parseDateOozieTZ(s1);
    }

    @Test(expected = ParseException.class)
    public void testOozieInOtherTZInvalidOffset() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "GMT-0300");
        DateUtils.setConf(conf);
        String s1 = "2012-08-08T12:42-0300x";
        DateUtils.parseDateOozieTZ(s1);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidOozieTimeZone() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "US/Los Angeles");
        DateUtils.setConf(conf);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidOozieTimeZoneGMTPrefix() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "xGMT-0300");
        DateUtils.setConf(conf);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidOozieTimeZoneGMTPostfix() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, "GMT-0300x");
        DateUtils.setConf(conf);
    }

}
