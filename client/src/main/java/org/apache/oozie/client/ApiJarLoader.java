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
import com.google.common.base.Strings;
import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;
import org.apache.oozie.fluentjob.api.workflow.Workflow;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

/**
 * Given a Fluent Job API {@code .jar} file compiled by the user this class loads the {@code Class} instance defined by
 * {@code Main-Class} manifest attribute, calls its {@code Workflow create()} method, and returns its output to the caller.
 */
public class ApiJarLoader {
    private final File apiJarFile;

    public ApiJarLoader(final File apiJarFile) {
        Preconditions.checkArgument(apiJarFile.isFile(), "Fluent Job API JAR [%s] should be a file", apiJarFile.toString());
        Preconditions.checkArgument(apiJarFile.getName().endsWith(".jar"), "Fluent Job API JAR [%s] should be a JAR file",
                apiJarFile.toString());

        this.apiJarFile = apiJarFile;
    }

    public Workflow loadAndGenerate() throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InstantiationException, InvocationTargetException {
        final String mainClassName = getMainClassName();
        Preconditions.checkState(!Strings.isNullOrEmpty(mainClassName),
                "Fluent Job API JAR should have a Main-Class defined in MANIFEST.MF");

        final URLClassLoader workflowFactoryClassLoader = URLClassLoader.newInstance(new URL[]{apiJarFile.toURI().toURL()});
        final Class mainClass = workflowFactoryClassLoader.loadClass(mainClassName);

        Preconditions.checkNotNull(mainClass, "Fluent Job API JAR file should have a main class");
        Preconditions.checkState(WorkflowFactory.class.isAssignableFrom(mainClass),
                "Fluent Job API JAR main class should be an " + WorkflowFactory.class.getName());

        @SuppressWarnings("unchecked")
        final Method mainMethod = mainClass.getMethod("create");
        Preconditions.checkState(Workflow.class.isAssignableFrom(mainMethod.getReturnType()),
                "Fluent Job API JAR file's main class's create() method should return a " + Workflow.class.getName());

        return (Workflow) mainMethod.invoke(mainClass.newInstance());
    }

    private String getMainClassName() throws IOException {
        try (final JarFile apiJar = new JarFile(apiJarFile)) {
            Preconditions.checkNotNull(apiJar.getManifest(), "Fluent Job API JAR doesn't have MANIFEST.MF");

            return apiJar.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
        }
    }
}
