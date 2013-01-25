/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import javax.imageio.ImageIO;
import junit.framework.Assert;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.test.XTestCase;

public class TestGraphGenerator extends XTestCase {

    public void testConstructor() {
        try {
            new GraphGenerator(null, null);
        }
        catch (IllegalArgumentException iae) {
            Assert.assertTrue("Construction with illegal args failed as expected: " + iae.getMessage(), true);
        }
        try {
            new GraphGenerator("<workflow></workflow>", null);
        }
        catch (IllegalArgumentException iae) {
            Assert.assertTrue("Construction with illegal args failed as expected: " + iae.getMessage(), true);
        }
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", new JsonWorkflowJob()));
        Assert.assertNotNull(new GraphGenerator(null, new JsonWorkflowJob()));
        JsonWorkflowJob jsonWFJob = new JsonWorkflowJob();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob));
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob, false));
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob, true));
    }

    public void testWrite() {
        JsonWorkflowJob jsonWFJob = new JsonWorkflowJob();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");
        String png1 = "src/test/resources/tmp1.png";
        String png2 = "src/test/resources/tmp2.png";

        try {
            GraphGenerator g = new GraphGenerator(readFile("src/test/resources/graphWF.xml"), jsonWFJob);
            g.write(new FileOutputStream(new File(png1)));
        }
        catch (Exception e) {
            Assert.fail("Write PNG failed for graphWF.xml: " + e.getMessage());
        }

        File f1 = new File(png1);
        try {
            // Check if a valid file was written
            Assert.assertNotNull(ImageIO.read(f1));
        }
        catch (IOException io) {
            Assert.fail("Not a valid PNG: " + io.getMessage());
        }

        try {
            GraphGenerator g = new GraphGenerator(readFile("src/test/resources/graphWF.xml"), jsonWFJob, true);
            g.write(new FileOutputStream(new File(png2)));
        }
        catch (Exception e) {
            Assert.fail("Write PNG failed for graphWF.xml: " + e.getMessage());
        }

        File f2 = new File(png2);
        try {
            // Check if a valid file was written
            Assert.assertNotNull(ImageIO.read(f2));
        }
        catch (IOException io) {
            Assert.fail("Not a valid PNG: " + io.getMessage());
        }

        Assert.assertTrue(f1.length() < f2.length());
        f1.delete();
        f2.delete();

        try {
            GraphGenerator g = new GraphGenerator(readFile("src/test/resources/invalidGraphWF.xml"), jsonWFJob, true);
            g.write(new FileOutputStream(new File("src/test/resources/invalid.png")));
        }
        catch (Exception e) {
            Assert.fail("Write PNG failed for invalidGraphWF.xml: " + e.getMessage());
        }
        new File("src/test/resources/invalid.png").delete();
    }

    private static String readFile(String path) throws IOException {
        File f = new File(path);
        System.out.println("Reading input file " + f.getAbsolutePath());
        FileInputStream stream = new FileInputStream(f);
        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            /* Instead of using default, pass in a decoder. */
            return Charset.defaultCharset().decode(bb).toString();
        }
        finally {
            stream.close();
        }
    }
}