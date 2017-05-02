/*
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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.LauncherMain;

import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.GitActionExecutor;
import org.apache.oozie.util.XLog;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.transport.*;
import org.eclipse.jgit.util.FS;
import com.google.common.annotations.VisibleForTesting;

public class GitMain extends LauncherMain {

    private static final String HADOOP_USER = "user.name";
    private static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    private static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    private static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private static final String OOZIE_ACTION_CONF = "oozie.action.conf.xml";

    // Private configuration variables
    private String appName;
    private String workflowId;
    private String callbackUrl;
    private String jobTracker;
    @VisibleForTesting
    protected String nameNode;
    private String keyPath;
    private String destinationUri;
    private String gitUri;
    private String gitBranch;
    private String actionType;
    private String actionName;

    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    protected XLog LOG = XLog.getLog(getClass());

    public static void main(String[] args) throws Exception {
        run(GitMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Git Action Configuration");
        LOG.debug("Oozie Git Action Configuration");
        System.out
                .println("=============================================");
        LOG.debug("=============================================");
        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty(OOZIE_ACTION_CONF);
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [" + OOZIE_ACTION_CONF + "]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));

        parseActionConfiguration(actionConf);

        File localKey = null;
        if (keyPath != null) {
          localKey = getKeyFromFS(new Path(keyPath));
        }

        try {
            cloneRepoToFS(new Path(destinationUri), new URI(gitUri), gitBranch, localKey);
        }catch(Exception e){
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new GitMainException(e.getCause());
        }
    }

    /**
     * Gathers the Git authentication key from a FileSystem and copies it to a local
     * filesystem location
     *
     * @param location where the key is located (an HDFS URI)
     * @return the location to where the key was saved
     */
    @VisibleForTesting
    private File getKeyFromFS(Path location) throws IOException, URISyntaxException {
        String keyCopyMsg = "Copied keys to local container!";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(new URI(nameNode), conf);
        File key = new File(Files.createTempDirectory(
            Paths.get("."),
            "keys_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        String mkdirMsg = "Local mkdir called creating temp. dir at: " + key.getAbsolutePath();
        System.out.println(mkdirMsg);
        LOG.debug(mkdirMsg);

        fs.copyToLocalFile(location, new Path("file:///" +
            key.getAbsolutePath() + "/privkey"));
        System.out.println(keyCopyMsg);
        LOG.debug(keyCopyMsg);
        return(new File(key.getAbsolutePath() + "/privkey"));
    }

    /**
     * Clones a Git repository
     *
     * @param gitSrc - URI to the Git repository being cloned
     * @param branch - String for the Git branch (or null)
     * @param outputDir - local file reference where the repository should be cloned
     * @param credentialFile - local file path containing repository authentication key or null
     */
    private void cloneRepo(URI gitSrc, String branch, File outputDir, final File credentialFile) throws GitMainException {
        final SshSessionFactory sshSessionFactory = new JschConfigSessionFactory() {
            @Override
            protected void configure(OpenSshConfig.Host host, Session session) {

            }

            @Override
            protected JSch createDefaultJSch(FS fs) throws JSchException {
                JSch.setConfig("StrictHostKeyChecking", "no");
                JSch defaultJSch = super.createDefaultJSch(fs);
                if (credentialFile != null) {
                    defaultJSch.addIdentity(credentialFile.toString());
                }
                return defaultJSch;
            }
        };

        CloneCommand cloneCommand = Git.cloneRepository();
        cloneCommand.setURI(gitSrc.toString());

        if (gitSrc.getScheme().toLowerCase() == "ssh") {
          cloneCommand.setTransportConfigCallback(new TransportConfigCallback() {
              @Override
              public void configure(Transport transport) {
                  SshTransport sshTransport = (SshTransport)transport;
                  sshTransport.setSshSessionFactory(sshSessionFactory);
              }
          });
        }

        cloneCommand.setDirectory(outputDir);
        // set our branch identifier
        if (branch != null) {
            cloneCommand.setBranchesToClone(Arrays.asList("refs/heads/" + branch));
        }

        try {
            cloneCommand.call();
        } catch (GitAPIException e) {
            String unableToCloneMsg = "Unable to clone Git repo: " + e;
            e.printStackTrace();
            LOG.error(unableToCloneMsg);
            throw new GitMainException(unableToCloneMsg);
        }
    }

    /**
     * Clone a Git repo up to a FileSystem
     *
     * @param destination - FileSystem path to which repository should be cloned
     * @param gitSrc - Git repo URI to clone from
     * @param branch - Git branch to clone
     * @param credentialFile - local file path containing repository authentication key or null
     * @throws Exception
     */
    private String cloneRepoToFS(Path destination, URI gitSrc, String branch, File credentialFile) throws Exception {
        String finishedCopyMsg = "Finished the copy to " + destination.toString() + "!";
        String finishedCloneingMsg = "Finished cloning to local";

        File tempD = new File(Files.createTempDirectory(
            Paths.get("."),
            "git_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        String localMkdirMsg = "Local mkdir called creating temp. dir at: " + tempD.getAbsolutePath();
        System.out.println(localMkdirMsg);
        LOG.debug(localMkdirMsg);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(destination.toUri(), conf);

        cloneRepo(gitSrc, branch,
            tempD, credentialFile);

        // create a list of files and directories to upload
        File src = new File(tempD.getAbsolutePath());
        ArrayList<Path> srcs = new ArrayList<Path>(1000);
        for (File p:src.listFiles()) {
          srcs.add(new Path(p.toString()));
        }

        System.out.println(finishedCloneingMsg);
        LOG.debug(finishedCloneingMsg);

        fs.mkdirs(destination);
        fs.copyFromLocalFile(false, true, srcs.toArray(new Path[0]), destination);
        System.out.println(finishedCopyMsg);
        LOG.debug(finishedCopyMsg);
        return(destination.toString());
    }

    /**
     * Validate a URI is well formed
     *
     * @param String URI string to test
     * @returns URI from string
     * @throws GitMainException
     */
    private URI validUri(String testUri) throws GitMainException {
        try {
            return(new URI(testUri));
        } catch (URISyntaxException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a proper URI: " + testUri + " exception "
                    + e.toString());
        }
    }

    /**
     * Parse action configuration and set configuration variables
     *
     * @param Oozie action configuration
     * @throws RuntimeException upon any parse failure
     */
    private void parseActionConfiguration(Configuration actionConf) throws GitMainException, ActionExecutorException {
        // APP_NAME
        GitActionExecutor.VerifyActionConf confChecker = new GitActionExecutor.VerifyActionConf(actionConf,"GIT011");
        appName = confChecker.returnActionConfNotNullFatal(GitActionExecutor.APP_NAME);
        //WORKFLOW_ID
        workflowId = confChecker.returnActionConfNotNullFatal(GitActionExecutor.WORKFLOW_ID);

        // CALLBACK_URL
        callbackUrl = confChecker.returnActionConfNotNullFatal(GitActionExecutor.CALLBACK_URL);

        // JOB_TRACKER
        jobTracker = confChecker.returnActionConfNotNullFatal(GitActionExecutor.JOB_TRACKER);

        //NAME_NODE
        nameNode = confChecker.returnActionConfNotNullFatal(GitActionExecutor.NAME_NODE);

        // DESTINATION_URI
        destinationUri = confChecker.returnActionConfNotNullFatal(GitActionExecutor.DESTINATION_URI);
        try {
            FileSystem fs = FileSystem.get(validUri(destinationUri), actionConf);
            destinationUri = fs.makeQualified(new Path(destinationUri)).toString();
        } catch (IOException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a valid filesystem for URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        }
        // GIT_URI
        gitUri = confChecker.returnActionConfNotNullFatal(GitActionExecutor.GIT_URI);
        if (validUri(gitUri).getScheme() == null) {
          throw new GitMainException("Action Configuration does not have "
                  + "a proper URI " + gitUri);
        }
        // GIT_BRANCH
        gitBranch = actionConf.get(GitActionExecutor.GIT_BRANCH);

        // KEY_PATH
        keyPath = actionConf.get(GitActionExecutor.KEY_PATH);
        // ACTION_TYPE
        actionType = confChecker.returnActionConfNotNullFatal(GitActionExecutor.ACTION_TYPE);
        // ACTION_NAME
        actionName = confChecker.returnActionConfNotNullFatal(GitActionExecutor.ACTION_NAME);
    }

    /**
     * Used by GitMain to wrap a Throwable when an Exception occurs
     */
    @SuppressWarnings("serial")
    static class GitMainException extends Exception {
        public GitMainException(Throwable t) {
            super(t);
        }

        public GitMainException(String t) {
            super(t);
        }
    }
}
