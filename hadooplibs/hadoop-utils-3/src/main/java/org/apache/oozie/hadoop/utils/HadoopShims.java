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

package org.apache.oozie.hadoop.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HadoopShims {
    FileSystem fs;

    public HadoopShims(FileSystem fs) {
        this.fs = fs;
    }

    public static boolean isSymlinkSupported() {
        return true;
    }

    public Path getSymLinkTarget(Path p) throws IOException {
        try {
            //getSymlink doesn't work with fragment name, need to remove fragment before calling getSymlink
            Path tempPath = new URI(p.toString()).getFragment() == null ? p : new Path(new URI(p.toString()).getPath());
            return fs.getFileLinkStatus(tempPath).getSymlink();
        }
        catch (java.net.URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public boolean isSymlink(Path p) throws IOException {
        try {
            //isSymlink doesn't work with fragment name, need to remove fragment before checking for symlink
            Path tempPath = new URI(p.toString()).getFragment() == null ? p : new Path(new URI(p.toString()).getPath());
            return fs.getFileLinkStatus(tempPath).isSymlink();
        }
        catch (java.net.URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
        fs.createSymlink(target, link, createParent);
    }

    public boolean isYARN() {
        return true;
    }

}
