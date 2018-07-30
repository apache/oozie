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

package org.apache.oozie.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public final class OozieSharelibFileOperations {

    /**
     * Suppress default constructor for noninstantiability
     */
    private OozieSharelibFileOperations() {
        throw new UnsupportedOperationException();
    }

    /**
     * generate a number of files equals with fileNr, and save the fileList parameter
     * @param fileNr number of files to be generated
     * @return a list of the generated files
     * @throws Exception
     */
    public static List<File> generateAndWriteFiles(File libDirectory, int fileNr) throws IOException {
        List<File> fileList = new ArrayList<>();
        for (int i=0; i<fileNr; i++) {
            String fileName = generateFileName(i);
            String fileContent = generateFileContent(i);
            fileList.add(writeFile(libDirectory, fileName, fileContent));
        }
        return fileList;
    }

    /**
     * Create a file in a specified folder, with a specific name and content
     * @param folder source folder
     * @param filename name of the generated file
     * @param content content of the generated file
     * @return the created file
     * @throws Exception
     */
    public static File writeFile(File folder, String filename, String content) throws IOException {
        File file = new File(folder.getAbsolutePath() + File.separator + filename);
        Writer writer = new FileWriter(file);
        writer.write(content);
        writer.flush();
        writer.close();
        return file;
    }

    public static String generateFileName(int i) {
        return "file_" + i;
    }

    public static String generateFileContent(int i) {
        return "test File " + i;
    }
}
