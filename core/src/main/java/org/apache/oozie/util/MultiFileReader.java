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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of a {@link Reader} which can be used to read in multiple files sequentially.  That is, when the first file ends
 * it will silently move to the next file and so on.  If the file has a ".gz" extension, this Reader will properly handle it; all
 * other types of files will simply be read using a {@link FileReader}.
 */
public class MultiFileReader extends Reader {

    private ArrayList<File> files;
    private int index;
    private Reader reader;
    private boolean closed;

    /**
     * Constructs the MultiFileReader with the given files.  The files will be read in the order given in the ArrayList.
     *
     * @param files The files to read
     * @throws IOException If there was a problem opening the first file
     */
    public MultiFileReader(ArrayList<File> files) throws IOException {
        this.files = files;
        closed = false;
        index = 0;
        reader = null;
        openNextReader();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int numRead = -1;
        while(!closed && numRead == -1) {
            numRead = reader.read(cbuf, off, len);
            if (numRead == -1) {
                reader.close();
                openNextReader();
            }
        }
        return numRead;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        closed = true;
    }

    private void openNextReader() throws IOException {
        if (index < files.size()) {
            // gzip files
            if (files.get(index).getName().endsWith(".gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(files.get(index)));
                reader = new InputStreamReader(gzipInputStream);
            }
            // regular files
            else {
                reader = new FileReader(files.get(index));
            }
            index++;
        }
        else {
            closed = true;
        }
    }
}
