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


import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.oozie.test.XTestCase;

public class TestMultiFileReader extends XTestCase {

    public void testOneFile() throws Exception {
        File dir = new File(getTestCaseDir());
        File f1 = new File(dir, "file1.txt");
        FileWriter fw = new FileWriter(f1);
        String str1 = "This is the first line of the only file\nThis is the second line\n";
        fw.write(str1);
        fw.close();
        ArrayList<File> files = new ArrayList<File>();
        files.add(f1);

        MultiFileReader reader = new MultiFileReader(files);
        char[] buf = new char[str1.length()];
        Arrays.fill(buf, '+');
        int numRead = reader.read(buf, 0, 10);
        assertEquals(10, numRead);
        assertEquals(str1.substring(0, 10), new String(buf, 0, 10));
        for (int i = 10; i < buf.length; i++) {
            assertEquals('+', buf[i]);
        }
        numRead = reader.read(buf, 10, str1.length() - 10);
        assertEquals(str1.length() - 10, numRead);
        assertEquals(str1, new String(buf));
        numRead = reader.read();
        assertEquals(-1, numRead);
    }

    public void testMultipleFiles() throws Exception {
        File dir = new File(getTestCaseDir());
        File f1 = new File(dir, "file1.txt");
        FileWriter fw = new FileWriter(f1);
        String str1 = "This is the first line of the first file\nThis is the second line\n";
        fw.write(str1);
        fw.close();
        File f2 = new File(dir, "file2.gz");
        String str2 = "This is a gz file with just one line\n";
        TestLogStreamer.writeToGZFile(f2, new StringBuilder(str2));
        File f3 = new File(dir, "file3.txt");
        fw = new FileWriter(f3);
        String str3 = "And this is the last file\nwith\n3 lines\n";
        fw.write(str3);
        fw.close();
        ArrayList<File> files = new ArrayList<File>();
        files.add(f1);
        files.add(f2);
        files.add(f3);

        MultiFileReader reader = new MultiFileReader(files);
        char[] buf = new char[str1.length() + str2.length() + str3.length()];
        Arrays.fill(buf, '+');
        // Try reading longer than the first file; MultiFileReader doesn't read past it and will simply return less chars
        // Reading again should get the next chars from the second file
        int numRead = reader.read(buf, 0, str1.length() + 10);
        assertEquals(str1.length(), numRead);
        assertEquals(str1, new String(buf, 0, str1.length()));
        for (int i = str1.length(); i < buf.length; i++) {
            assertEquals('+', buf[i]);
        }
        numRead = reader.read(buf, str1.length(), str2.length());
        assertEquals(str2.length(), numRead);
        assertEquals(str1 + str2, new String(buf, 0, str1.length() + str2.length()));
        for (int i = str1.length() + str2.length(); i < buf.length; i++) {
            assertEquals('+', buf[i]);
        }
        numRead = reader.read(buf, str1.length() + str2.length(), str3.length());
        assertEquals(str3.length(), numRead);
        assertEquals(str1 + str2 + str3, new String(buf));
        numRead = reader.read();
        assertEquals(-1, numRead);
    }
}
