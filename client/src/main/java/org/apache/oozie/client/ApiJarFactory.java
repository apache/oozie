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

package org.apache.oozie.client;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Given a folder where the {@code .class} file containing a {@link WorkflowFactory} subclass persist, a name, and the
 * exact {@code Class} reference, creates a {@code .jar} file containing {@code Main-Class} manifest attribute
 * pointing to {@code apiFactoryClass}.
 * <p>
 * Used mainly by unit tests, this class helps to dynamically assemble Fluent Job API {@code .jar} files.
 */
class ApiJarFactory {
    private final File classFolder;
    private final File jarFolder;
    private final String apiJarName;
    private final Class<? extends WorkflowFactory> apiFactoryClass;

    ApiJarFactory(final File classFolder,
                         File jarFolder, final Class<? extends WorkflowFactory> apiFactoryClass, final String apiJarName) {
        Preconditions.checkNotNull(classFolder, "classFolder should be set");
        Preconditions.checkNotNull(jarFolder, "jarFolder should be set");
        Preconditions.checkNotNull(apiJarName, "apiJarName should be set");
        Preconditions.checkNotNull(apiFactoryClass, "apiFactoryClass should be set");
        Preconditions.checkState(WorkflowFactory.class.isAssignableFrom(apiFactoryClass),
                String.format("%s should be a %s", apiFactoryClass.getName(), WorkflowFactory.class.getName()));

        this.classFolder = classFolder;
        this.jarFolder = jarFolder;
        this.apiJarName = apiJarName;
        this.apiFactoryClass = apiFactoryClass;
    }

    @SuppressFBWarnings(value = {"PATH_TRAVERSAL_OUT", "WEAK_FILENAMEUTILS"},
            justification = "FilenameUtils is used to filter user output. JDK8+ is used.")
    JarFile create() throws IOException {
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, apiFactoryClass.getName());

        final String apiJarPath = jarFolder + File.separator + FilenameUtils.getName(apiJarName);
        try (final JarOutputStream target = new JarOutputStream(
                new FileOutputStream(FilenameUtils.getFullPath(apiJarPath) + FilenameUtils.getName(apiJarPath)), manifest)) {
            addWorkflowJarEntry(classFolder, target);
        }

        return new JarFile(apiJarPath);
    }

    private void addWorkflowJarEntry(final File source, final JarOutputStream target) throws IOException {
        if (source.isDirectory()) {
            String name = source.getPath().replace("\\", "/");
            if (!name.isEmpty()) {
                if (!name.endsWith("/")) {
                    name += "/";
                }
                final JarEntry entry = new JarEntry(name);
                entry.setTime(source.lastModified());
                target.putNextEntry(entry);
                target.closeEntry();
            }

            final File[] nestedFiles = source.listFiles();
            if (nestedFiles == null) {
                return;
            }

            for (final File nestedFile : nestedFiles) {
                addWorkflowJarEntry(nestedFile, target);
            }

            return;
        }

        try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(source))) {

            final JarEntry entry = new JarEntry(source.getPath().replace("\\", "/"));
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);

            IOUtils.copy(in, target);
            target.closeEntry();
        }
    }
}
