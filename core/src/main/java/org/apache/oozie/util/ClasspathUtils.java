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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;


public class ClasspathUtils {
    private static boolean usingMiniYarnCluster = false;
    private static final List<String> CLASSPATH_ENTRIES = Arrays.asList(
            ApplicationConstants.Environment.PWD.$(),
            ApplicationConstants.Environment.PWD.$() + Path.SEPARATOR + "*"
    );

    @VisibleForTesting
    public static void setUsingMiniYarnCluster(boolean useMiniYarnCluster) {
        usingMiniYarnCluster = useMiniYarnCluster;
    }

    // Adapted from MRApps#setClasspath.  Adds Yarn, HDFS, Common, and distributed cache jars.
    public static void setupClasspath(Map<String, String> env, Configuration conf) throws IOException {
        // Propagate the system classpath when using the mini cluster
        if (usingMiniYarnCluster) {
            MRApps.addToEnvironment(
                    env,
                    ApplicationConstants.Environment.CLASSPATH.name(),
                    System.getProperty("java.class.path"), conf);
        }

        for (String entry : CLASSPATH_ENTRIES) {
            MRApps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), entry, conf);
        }

        // a * in the classpath will only find a .jar, so we need to filter out
        // all .jars and add everything else
        addToClasspathIfNotJar(org.apache.hadoop.mapreduce.filecache.DistributedCache.getFileClassPaths(conf),
                org.apache.hadoop.mapreduce.filecache.DistributedCache.getCacheFiles(conf),
                conf,
                env, ApplicationConstants.Environment.PWD.$());
        addToClasspathIfNotJar(org.apache.hadoop.mapreduce.filecache.DistributedCache.getArchiveClassPaths(conf),
                org.apache.hadoop.mapreduce.filecache.DistributedCache.getCacheArchives(conf),
                conf,
                env, ApplicationConstants.Environment.PWD.$());


        boolean crossPlatform = conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
                MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM);

        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                crossPlatform
                        ? YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH
                        : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            MRApps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), conf);
        }
    }

    // Adapted from MRApps#setClasspath
    public static void addMapReduceToClasspath(Map<String, String> env, Configuration conf) {
        boolean crossPlatform = conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
                MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM);

        for (String c : conf.getStrings(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                crossPlatform ?
                        StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH)
                        : StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
            MRApps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), conf);
        }
    }

    // Borrowed from MRApps#addToClasspathIfNotJar
    private static void addToClasspathIfNotJar(Path[] paths,
                                               URI[] withLinks, Configuration conf,
                                               Map<String, String> environment,
                                               String classpathEnvVar) throws IOException {
        if (paths != null) {
            HashMap<Path, String> linkLookup = new HashMap<Path, String>();
            if (withLinks != null) {
                for (URI u: withLinks) {
                    Path p = new Path(u);
                    FileSystem remoteFS = p.getFileSystem(conf);
                    p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
                            remoteFS.getWorkingDirectory()));
                    String name = (null == u.getFragment())
                            ? p.getName() : u.getFragment();
                    if (!name.toLowerCase(Locale.ENGLISH).endsWith(".jar")) {
                        linkLookup.put(p, name);
                    }
                }
            }

            for (Path p : paths) {
                FileSystem remoteFS = p.getFileSystem(conf);
                p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
                        remoteFS.getWorkingDirectory()));
                String name = linkLookup.get(p);
                if (name == null) {
                    name = p.getName();
                }
                if(!name.toLowerCase(Locale.ENGLISH).endsWith(".jar")) {
                    MRApps.addToEnvironment(
                            environment,
                            classpathEnvVar,
                            ApplicationConstants.Environment.PWD.$() + Path.SEPARATOR + name, conf);
                }
            }
        }
    }

    public static Configuration addToClasspathFromLocalShareLib(Configuration conf, Path libPath) {
        if (conf == null) {
            conf = new Configuration(false);
        }
        final String pathStr = normalizedLocalFsPath(libPath);

        String appClassPath = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);

        if (org.apache.commons.lang.StringUtils.isEmpty(appClassPath)) {
            addPathToYarnClasspathInConfig(conf, pathStr, StringUtils.join(File.pathSeparator,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH));
        } else {
            addPathToYarnClasspathInConfig(conf, pathStr, appClassPath);
        }
        return conf;
    }

    private static void addPathToYarnClasspathInConfig(Configuration conf, String pathStr, String appClassPath) {
        conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, appClassPath + File.pathSeparator + pathStr);
    }

    private static String normalizedLocalFsPath(Path libPath) {
        return org.apache.commons.lang.StringUtils.replace(libPath.toString(), FSUtils.FILE_SCHEME_PREFIX, "");
    }
}
