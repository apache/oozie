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

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

class HadoopUriFinder {

    static String getJarVersion(final File jarFile) throws IOException {
        try (final JarFile openedJarFile = new JarFile(jarFile)) {
            final Manifest manifest = openedJarFile.getManifest();
            return manifest.getMainAttributes().getValue("Specification-Version");
        }
    }

    static URI getFixedUri(final URI fileUri) throws URISyntaxException, IOException {
        final FileSystem fs = FileSystem.get(new Configuration(true));
        return getFixedUri(fs, fileUri);
    }

    /**
     * Spark compares URIs based on scheme, host and port. Here we convert URIs
     * into the default format so that Spark won't think those belong to
     * different file system. This will avoid an extra copy of files which
     * already exists on same hdfs.
     *
     * @param fs
     * @param fileUri
     * @return fixed uri
     * @throws URISyntaxException
     */
    static URI getFixedUri(final FileSystem fs, final URI fileUri) throws URISyntaxException {
        if (fs.getUri().getScheme().equals(fileUri.getScheme())
                && (fs.getUri().getHost().equals(fileUri.getHost()) || fileUri.getHost() == null)
                && (fs.getUri().getPort() == -1 || fileUri.getPort() == -1
                        || fs.getUri().getPort() == fileUri.getPort())) {
            return new URI(fs.getUri().getScheme(), fileUri.getUserInfo(), fs.getUri().getHost(), fs.getUri().getPort(),
                    fileUri.getPath(), fileUri.getQuery(), fileUri.getFragment());
        }
        return fileUri;
    }
}