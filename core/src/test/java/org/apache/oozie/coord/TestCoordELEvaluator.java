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
package org.apache.oozie.coord;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;

import java.io.StringReader;

import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class TestCoordELEvaluator extends XTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        if (Services.get() != null) {
            Services.get().destroy();
        }
        super.tearDown();
    }

    public void testCreateFreqELValuator() throws Exception {
        // System.out.println("CP :" + System.getProperty("java.class.path"));
        // Configuration conf = new
        // XConfiguration(IOUtils.getResourceAsReader("org/apache/oozie/coord/conf.xml",
        // -1));
        Configuration conf = new XConfiguration(new StringReader(
                getConfString()));
        ELEvaluator eval = CoordELEvaluator.createELEvaluatorForGroup(conf,
                                                                      "coord-job-submit-freq");
        String expr = "<coordinator-app name=\"mycoordinator-app\" start=\"${start}\" end=\"${end}\""
                + " frequency=\"${coord:hours(12)}\"><data-in name=\"A\" dataset=\"a\"></data-in>";
        String reply = expr.replace("${start}", conf.get("start")).replace(
                "${end}", conf.get("end")).replace("${coord:hours(12)}", "720");
        assertEquals(reply, CoordELFunctions.evalAndWrap(eval, expr));

        expr = "<coordinator-app name=\"mycoordinator-app\" start=\"${start}\" end=\"${end}\""
                + " frequency=\"${coord:days(7)}\"><data-in name=\"A\" dataset=\"a\"></data-in>";
        reply = expr.replace("${start}", conf.get("start")).replace("${end}",
                                                                    conf.get("end")).replace("${coord:days(7)}", "7");
        assertEquals(reply, CoordELFunctions.evalAndWrap(eval, expr));

        expr = "<coordinator-app name=\"mycoordinator-app\" start=\"${start}\" end=\"${end}\""
                + " frequency=\"${coord:months(1)}\"><data-in name=\"A\" dataset=\"a\"></data-in>";
        reply = expr.replace("${start}", conf.get("start")).replace("${end}",
                                                                    conf.get("end")).replace("${coord:months(1)}", "1");
        // System.out.println("****testCreateELValuator :"+
        // CoordELFunctions.evaluateFunction(eval, expr));
        assertEquals(reply, CoordELFunctions.evalAndWrap(eval, expr));

        expr = "frequency=${coord:days(2)}";
        expr = "frequency=60";
        CoordELFunctions.evalAndWrap(eval, expr);
        expr = "frequency=${coord:daysInMonth(2)}";
        try {
            CoordELFunctions.evalAndWrap(eval, expr);
            fail();
        }
        catch (Exception ex) {
        }

        expr = "frequency=${coord:hoursInDay(2)}";
        try {
            CoordELFunctions.evalAndWrap(eval, expr);
            fail();
        }
        catch (Exception ex) {
        }

        expr = "frequency=${coord:tzOffset()}";
        try {
            CoordELFunctions.evalAndWrap(eval, expr);
            fail();
        }
        catch (Exception ex) {
        }

        expr = "<frequency=120";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testCreateURIELEvaluator() throws Exception {
        ELEvaluator eval = CoordELEvaluator
                .createURIELEvaluator("2009-08-09T23:59Z");
        String expr = "hdfs://p1/p2/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
        // System.out.println("OUTPUT "+ eval.evaluate(expr, String.class));
        assertEquals("hdfs://p1/p2/2009/08/09/23/59/", CoordELFunctions
                .evalAndWrap(eval, expr));

        expr = "hdfs://p1/p2/${YEAR}/${MONTH}/${DAY}/${MINUTE}/";
        assertEquals("hdfs://p1/p2/2009/08/09/59/", CoordELFunctions
                .evalAndWrap(eval, expr));

    }

    public void testCreateDataEvaluator() throws Exception {
        String jobXml = "<coordinator-app name=\"mycoordinator-app\" start=\"2009-02-01T01:00GMT\" end=\"2009-02-03T23:59GMT\" frequency=\"720\"";
        jobXml += " action-nominal-time='2009-09-01T00:00Z' action-actual-time='2010-10-01T00:00Z'>";
        jobXml += "<input-events><data-in name=\"A\" dataset=\"a\"><uris>file:///tmp/coord/US/2009/1/30|file:///tmp/coord/US/2009/1/31</uris>";
        jobXml += "<dataset name=\"a\" frequency=\"1440\" initial-instance=\"2009-01-01T00:00Z\">";
        jobXml += "<uri-template>file:///tmp/coord/US/${YEAR}/${MONTH}/${DAY}</uri-template></dataset></data-in></input-events>";
        jobXml += "<action><workflow><url>http://foobar.com:8080/oozie</url><app-path>hdfs://foobarfoobar.com:9000/usr/tucu/mywf</app-path>";
        jobXml += "<configuration><property><name>inputA</name><value>${coord:dataIn('A')}</value></property>";
        jobXml += "<property><name>ACTIONID</name><value>${coord:actionId()}</value></property>";
        jobXml += "<property><name>NAME</name><value>${coord:name()}</value></property>";
        jobXml += "<property><name>NOMINALTIME</name><value>${coord:nominalTime()}</value></property>";
        jobXml += "<property><name>ACTUALTIME</name><value>${coord:actualTime()}</value></property>";
        jobXml += "</configuration></workflow></action></coordinator-app>";

        String reply = "<action><workflow><url>http://foobar.com:8080/oozie</url><app-path>hdfs://foobarfoobar.com:9000/usr/tucu/mywf</app-path>";
        reply += "<configuration><property><name>inputA</name><value>file:///tmp/coord/US/2009/1/30|file:///tmp/coord/US/2009/1/31</value></property>";
        reply += "<property><name>ACTIONID</name><value>00000-oozie-C@1</value></property>";
        reply += "<property><name>NAME</name><value>mycoordinator-app</value></property>";
        reply += "<property><name>NOMINALTIME</name><value>2009-09-01T00:00Z</value></property>";
        reply += "<property><name>ACTUALTIME</name><value>2010-10-01T00:00Z</value></property>";
        reply += "</configuration></workflow></action>";
        Element eJob = XmlUtils.parseXml(jobXml);
        Configuration conf = new XConfiguration(new StringReader(getConfString()));
        ELEvaluator eval = CoordELEvaluator.createDataEvaluator(eJob, conf, "00000-oozie-C@1");
        Element action = eJob.getChild("action", eJob.getNamespace());
        String str = XmlUtils.prettyPrint(action).toString();
        assertEquals(XmlUtils.prettyPrint(XmlUtils.parseXml(reply)).toString(), CoordELFunctions.evalAndWrap(eval, str));
    }

    public void testCreateInstancesELEvaluator() throws Exception {
        String dataEvntXML = "<data-in name=\"A\" dataset=\"a\"><uris>file:///tmp/coord/US/2009/1/30|file:///tmp/coord/US/2009/1/31</uris>";
        dataEvntXML += "<dataset name=\"a\" frequency=\"1440\" initial-instance=\"2009-01-01T00:00Z\" freq_timeunit=\"MINUTE\" timezone=\"UTC\" end_of_duration=\"NONE\">";
        dataEvntXML += "<uri-template>file:///tmp/coord/US/${YEAR}/${MONTH}/${DAY}</uri-template></dataset></data-in>";
        Element event = XmlUtils.parseXml(dataEvntXML);
        SyncCoordAction appInst = new SyncCoordAction();
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-09-08T01:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        appInst.setTimeUnit(TimeUnit.MINUTE);
        // Configuration conf = new
        // XConfiguration(IOUtils.getResourceAsReader("org/apache/oozie/coord/conf.xml",
        // -1));
        Configuration conf = new XConfiguration(new StringReader(
                getConfString()));
        ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event,
                                                                       appInst, conf);
        String expr = "${coord:current(0)}";
        // System.out.println("OUTPUT :" + eval.evaluate(expr, String.class));
        assertEquals("2009-09-08T00:00Z", eval.evaluate(expr, String.class));
    }

    public void testCreateLazyEvaluator() throws Exception {
        // Configuration conf = new
        // XConfiguration(IOUtils.getResourceAsReader("org/apache/oozie/coord/conf.xml",
        // -1));
    	String testCaseDir = getTestCaseDir();
    	Configuration conf = new XConfiguration(new StringReader(getConfString()));

        Date actualTime = DateUtils.parseDateUTC("2009-09-01T01:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2009-09-01T00:00Z");
        String dataEvntXML = "<data-in name=\"A\" dataset=\"a\"><uris>file:///"+testCaseDir+"/US/2009/1/30|file:///tmp/coord/US/2009/1/31</uris>";
        dataEvntXML += "<dataset name=\"a\" frequency=\"1440\" initial-instance=\"2009-01-01T00:00Z\"  freq_timeunit=\"MINUTE\" timezone=\"UTC\" end_of_duration=\"NONE\">";
        dataEvntXML += "<uri-template>file:///"+testCaseDir+"/${YEAR}/${MONTH}/${DAY}</uri-template></dataset></data-in>";
        Element dEvent = XmlUtils.parseXml(dataEvntXML);
        ELEvaluator eval = CoordELEvaluator.createLazyEvaluator(actualTime, nominalTime, dEvent, conf);
        createDir(testCaseDir+"/2009/01/02");
        String expr = "${coord:latest(0)} ${coord:latest(-1)}";
        // Dependent on the directory structure
        // TODO: Create the directory
        assertEquals("2009-01-02T00:00Z ${coord:latest(-1)}", eval.evaluate(expr, String.class));

        // future
        createDir(testCaseDir+"/2009/09/04");
        createDir(testCaseDir+"/2009/09/05");
        expr = "${coord:future(1, 30)}";
        assertEquals("2009-09-05T00:00Z", eval.evaluate(expr, String.class));

        // System.out.println("OUTPUT :" + eval.evaluate(expr, String.class));
    }

    public void testCleanup() throws Exception {
        Services.get().destroy();
    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getConfString() {
        StringBuilder conf = new StringBuilder();
        conf.append("<configuration> <property><name>baseFsURI</name> <value>file:///tmp/coord/</value> </property>");
        conf.append("<property><name>language</name> <value>en</value> </property>");
        conf
                .append("<property> <name>country</name>  <value>US</value>  </property> "
                        + "<property> <name>market</name> <value>teens</value> </property> "
                        + "<property>  <name>app_path</name> <value>file:///tmp/coord/workflows</value> </property> "
                        + "<property> <name>start</name> <value>2009-02-01T01:00Z</value> </property>"
                        + "<property> <name>end</name> <value>2009-02-03T23:59Z</value> </property> "
                        + "<property> <name>timezone</name> <value>UTC</value> </property> "
                        + "<property> <name>user.name</name> <value>test_user</value> </property> "
                        + "<property> <name>timeout</name>  <value>180</value>  </property> "
                        + "<property> <name>concurrency_level</name> <value>1</value> </property> "
                        + "<property> <name>execution_order</name> <value>LIFO</value> </property>"
                        + "<property> <name>include_ds_files</name>  <value>file:///homes/" + getTestUser() + "/workspace/oozie-main/core/src/main/java/org/apache/oozie/coord/datasets.xml</value>"
                        + " </property></configuration>");
        return conf.toString();
    }
}
