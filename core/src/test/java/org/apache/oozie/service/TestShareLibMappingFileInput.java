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

package org.apache.oozie.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.FSUtils;

import java.io.IOException;

public class TestShareLibMappingFileInput {
    final FileSystem fs;
    final String sharelibNameWithMappingFilePrefix;
    final String sharelibName;
    final String sharelibPath;
    final String baseName;

    public TestShareLibMappingFileInput(final FileSystem fs,
                                        final String sharelibMappingActionName,
                                        final String sharelibPath) {
        this.fs = fs;
        this.sharelibName = sharelibMappingActionName;
        this.sharelibNameWithMappingFilePrefix = ShareLibService.SHARE_LIB_CONF_PREFIX + "."
                + sharelibMappingActionName;
        this.sharelibPath = sharelibPath;
        this.baseName = sharelibPath.substring(sharelibPath.lastIndexOf(Path.SEPARATOR) + 1);
    }

    public void materialize() throws IOException {
        TestShareLibService.createFileWithDirectoryPath(fs, sharelibPath);
    }

    String getFullShareLibPathDir() {
        String fullShareLibPathDir = getLocalizedShareLibPath(fs, sharelibPath);
        return fullShareLibPathDir.substring(0, fullShareLibPathDir.lastIndexOf(Path.SEPARATOR));
    }

    static String getLocalizedShareLibPath(FileSystem fs, String path) {
        return (fs instanceof LocalFileSystem)
                ? FSUtils.FILE_SCHEME_PREFIX + "//" + path
                : path;
    }
}

class TestShareLibMappingSymlinkInput extends TestShareLibMappingFileInput {
    final String symlinkPath;

    public TestShareLibMappingSymlinkInput(final FileSystem fs,
                                           final String sharelibActionName,
                                           final String sharelibPath,
                                           final String symlinkPath) {
        super(fs, sharelibActionName, sharelibPath);
        this.symlinkPath = symlinkPath;
    }

    @Override
    public void materialize() throws IOException {
            super.materialize();
            FSUtils.createSymlink(fs, new Path(sharelibPath), new Path(symlinkPath), true);
    }

    @Override
    String getFullShareLibPathDir() {
        int lastIndexOfPathSeparator = sharelibPath.lastIndexOf(Path.SEPARATOR);
        String symlinkTargetFileName = (lastIndexOfPathSeparator != -1)
                ? sharelibPath.substring(lastIndexOfPathSeparator + 1)
                : sharelibPath;
        String fullShareLibPathDir = getLocalizedShareLibPath(fs, symlinkPath);
        return fullShareLibPathDir + "#" + symlinkTargetFileName;
    }
}
