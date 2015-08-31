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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.SparkSubmit;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SparkMain extends LauncherMain {
    private static final String MASTER_OPTION = "--master";
    private static final String MODE_OPTION = "--deploy-mode";
    private static final String JOB_NAME_OPTION = "--name";
    private static final String CLASS_NAME_OPTION = "--class";
    private static final String VERBOSE_OPTION = "--verbose";
    private static final String EXECUTOR_CLASSPATH = "spark.executor.extraClassPath=";
    private static final String DRIVER_CLASSPATH = "spark.driver.extraClassPath=";
    private static final String DIST_FILES = "spark.yarn.dist.files=";
    private static final String JARS_OPTION = "--jars";
    private static final String DELIM = "\\s+";

    private String sparkJars = null;
    private String sparkClasspath = null;

    public static void main(String[] args) throws Exception {
        run(SparkMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        Configuration actionConf = loadActionConf();
        setYarnTag(actionConf);
        LauncherMainHadoopUtils.killChildYarnJobs(actionConf);

        List<String> sparkArgs = new ArrayList<String>();

        sparkArgs.add(MASTER_OPTION);
        String master = actionConf.get(SparkActionExecutor.SPARK_MASTER);
        sparkArgs.add(master);

        String sparkDeployMode = actionConf.get(SparkActionExecutor.SPARK_MODE);
        if (sparkDeployMode != null) {
            sparkArgs.add(MODE_OPTION);
            sparkArgs.add(sparkDeployMode);
        }
        boolean yarnClusterMode = master.equals("yarn-cluster")
                || (master.equals("yarn") && sparkDeployMode != null && sparkDeployMode.equals("cluster"));
        boolean yarnClientMode = master.equals("yarn-client")
                || (master.equals("yarn") && sparkDeployMode != null && sparkDeployMode.equals("client"));

        sparkArgs.add(JOB_NAME_OPTION);
        sparkArgs.add(actionConf.get(SparkActionExecutor.SPARK_JOB_NAME));

        String className = actionConf.get(SparkActionExecutor.SPARK_CLASS);
        if (className != null) {
            sparkArgs.add(CLASS_NAME_OPTION);
            sparkArgs.add(className);
        }

        String jarPath = actionConf.get(SparkActionExecutor.SPARK_JAR);

        // In local mode, everything runs here in the Launcher Job.
        // In yarn-client mode, the driver runs here in the Launcher Job and the executor in Yarn.
        // In yarn-cluster mode, the driver and executor run in Yarn.
        // Due to this, configuring Spark's classpath is not straightforward (see below)

        // Parse the spark opts.  We need to make sure to pass the necessary jars (Hadoop, Spark, user) to Spark.  The way we do
        // this depends on what mode is being used:
        // local/yarn-cluster/yarn-client: Passed as comma-separated list via --jars argument
        // yarn-cluster/yarn-client: Passed as ':'-separated list via spark.executor.extraClassPath and spark.driver.extraClassPath
        // yarn-client: Passed as comma-separted list via spark.yarn.dist.files
        //
        // --jars will cause the jars to be uploaded to HDFS and localized.  To prevent the Sharelib and user jars from being
        // unnecessarily reuploaded to HDFS, we use the HDFS paths for these.  The hadoop jars are needed as well, but we'll have
        // to use local paths for these because they're not in the Sharelib.
        //
        // spark.executor.extraClassPath and spark.driver.extraClassPath are blindly used as classpaths, so we need to put only
        // localized jars.  --jars will cause the jars to be localized to the working directory, so we can simply specify the jar
        // names for the classpaths, as they'll be found in the working directory.
        //
        // This part looks more complicated than it is because we need to append the jars if the user already set things for
        // these options
        determineSparkJarsAndClasspath(actionConf, jarPath);
        boolean addedExecutorClasspath = false;
        boolean addedDriverClasspath = false;
        boolean addedDistFiles = false;
        boolean addedJars = false;
        String sparkOpts = actionConf.get(SparkActionExecutor.SPARK_OPTS);
        if (StringUtils.isNotEmpty(sparkOpts)) {
            String[] sparkOptions = sparkOpts.split(DELIM);
            for (int i = 0; i < sparkOptions.length; i++) {
                String opt = sparkOptions[i];
                if (sparkJars != null) {
                    if (opt.equals(JARS_OPTION)) {
                        sparkArgs.add(opt);
                        i++;
                        opt = sparkOptions[i];
                        opt = opt + "," + sparkJars;
                        addedJars = true;
                    } else if (yarnClientMode && opt.startsWith(DIST_FILES)) {
                        opt = opt + "," + sparkJars;
                        addedDistFiles = true;
                    }
                }
                if ((yarnClusterMode || yarnClientMode) && sparkClasspath != null) {
                    if (opt.startsWith(EXECUTOR_CLASSPATH)) {
                        opt = opt + File.pathSeparator + sparkClasspath;
                        addedExecutorClasspath = true;
                    }
                    if (opt.startsWith(DRIVER_CLASSPATH)) {
                        opt = opt + File.pathSeparator + sparkClasspath;
                        addedDriverClasspath = true;
                    }
                }
                sparkArgs.add(opt);
            }
        }
        if (!addedJars && sparkJars != null) {
            sparkArgs.add("--jars");
            sparkArgs.add(sparkJars);
        }
        if ((yarnClusterMode || yarnClientMode) && sparkClasspath != null) {
            if (!addedExecutorClasspath) {
                sparkArgs.add("--conf");
                sparkArgs.add(EXECUTOR_CLASSPATH + sparkClasspath);
            }
            if (!addedDriverClasspath) {
                sparkArgs.add("--conf");
                sparkArgs.add(DRIVER_CLASSPATH + sparkClasspath);
            }
        }
        if (yarnClientMode && !addedDistFiles && sparkJars != null) {
            sparkArgs.add("--conf");
            sparkArgs.add(DIST_FILES + sparkJars);
        }

        if (!sparkArgs.contains(VERBOSE_OPTION)) {
            sparkArgs.add(VERBOSE_OPTION);
        }

        sparkArgs.add(jarPath);

        for (String arg : args) {
            sparkArgs.add(arg);
        }

        System.out.println("Spark Action Main class        : " + SparkSubmit.class.getName());
        System.out.println();
        System.out.println("Oozie Spark action configuration");
        System.out.println("=================================================================");
        System.out.println();
        for (String arg : sparkArgs) {
            System.out.println("                    " + arg);
        }
        System.out.println();
        runSpark(sparkArgs.toArray(new String[sparkArgs.size()]));
    }

    private void runSpark(String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();
        SparkSubmit.main(args);
    }

    private void determineSparkJarsAndClasspath(Configuration actionConf, String jarPath) {
        // distCache gets all of the Sharelib and user jars (from the Distributed Cache)
        // classpath gets all of the jars from the classpath, which includes the localized jars from the distCache
        // sparkJars becomes all of the full paths to the Sharelib and user jars from HDFS and the deduped local paths
        // sparkClasspath becomes all of the jar names in sparkJars (without paths)
        // We also remove the Spark job jar and job.jar
        String[] distCache = new String[]{};
        String dCache = actionConf.get("mapreduce.job.classpath.files");
        if (dCache != null) {
            distCache = dCache.split(",");
        }
        String[] classpath = System.getProperty("java.class.path").split(File.pathSeparator);
        StringBuilder cp = new StringBuilder();
        StringBuilder jars = new StringBuilder();
        HashSet<String> distCacheJars = new HashSet<String>(distCache.length);
        for (String path : distCache) {
            // Skip the job jar because it's already included elsewhere and Spark doesn't like duplicating it here
            if (!path.equals(jarPath)) {
                String name = path.substring(path.lastIndexOf("/") + 1);
                distCacheJars.add(name);
                cp.append(name).append(File.pathSeparator);
                jars.append(path).append(",");
            }
        }
        for (String path : classpath) {
            if (!path.startsWith("job.jar") && path.endsWith(".jar")) {
                String name = path.substring(path.lastIndexOf("/") + 1);
                if (!distCacheJars.contains(name)) {
                    jars.append(path).append(",");
                }
                cp.append(name).append(File.pathSeparator);
            }
        }
        if (cp.length() > 0) {
            cp.setLength(cp.length() - 1);
            sparkClasspath = cp.toString();
        }
        if (jars.length() > 0) {
            jars.setLength(jars.length() - 1);
            sparkJars = jars.toString();
        }
    }
}
