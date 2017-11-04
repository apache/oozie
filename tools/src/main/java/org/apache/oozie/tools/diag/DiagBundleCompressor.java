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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipOutputStream;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Output directory is specified by user")
class DiagBundleCompressor {
    private static void zip(final File inputDir, final File outputFile) throws IOException {
        System.out.print("Creating Zip File: " + outputFile.getAbsolutePath() + "...");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(outputFile))) {
            IOUtils.zipDir(inputDir, "/", zos);
        }
        System.out.println("Done");
    }

    static void compressDiagInformationToBundle(final File outputDir, final File tempDir) throws IOException {
        final File zipFile = new File(outputDir, "oozie-diag-bundle-" + System.currentTimeMillis() + ".zip");
        zip(tempDir, zipFile);
    }
}
