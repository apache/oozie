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

package org.apache.oozie.tools.diag;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.oozie.util.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Output directory is specified by user")
public class DiagBundleEntryWriter implements Closeable {
    private final Writer writer;

    DiagBundleEntryWriter(final File parentDir, final String fileName) throws FileNotFoundException {
        this(new FileOutputStream(new File(parentDir, fileName)));
    }

    private DiagBundleEntryWriter(final OutputStream ous) {
        try {
            this.writer = new OutputStreamWriter(ous, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    DiagBundleEntryWriter writeDateValue(final String key, final Date date) throws IOException {
        if (date != null) {
            writeStringValue(key, DiagBundleCollectorDriver.DATE_FORMAT.format(date));
        } else {
            writeStringValue(key, "null");
        }
        return this;
    }

    DiagBundleEntryWriter writeLongValue(final String key, long value) throws IOException {
        writeStringValue(key, Long.toString(value));
        return this;
    }

    DiagBundleEntryWriter writeIntValue(final String key, int value) throws IOException {
        writeStringValue(key, Integer.toString(value));
        return this;
    }

    DiagBundleEntryWriter writeStringValue(final String key, final String value) throws IOException {
        writer.write(key);
        if (value != null && !value.isEmpty()) {
            writer.write(String.format("\"%s\"", value));
        } else if (value == null){
            writeNull();
        } else {
            writer.write("\"\"");
        }
        writer.write("\n");
        return this;
    }

    DiagBundleEntryWriter writeString(final String s) throws IOException {
        if (s == null) {
            writeNull();
        }
        else {
            writer.write(s);
        }
        return this;
    }

    DiagBundleEntryWriter writeNewLine() throws IOException {
        writer.write("\n");
        return this;
    }

    DiagBundleEntryWriter writeNull() throws IOException {
        writer.write("null");
        return this;
    }

    void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() {
        try {
            writer.flush();
        } catch (IOException e) {
            System.err.printf("Could not persist data. Exception: %s%n", e.getMessage());
        } finally {
            IOUtils.closeSafely(writer);
        }
    }
}