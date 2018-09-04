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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.oozie.action.hadoop.GitOperations.GitOperationsException;

import com.google.common.annotations.VisibleForTesting;

public class GitMain extends LauncherMain {

    private static final String OOZIE_ACTION_CONF = "oozie.action.conf.xml";

    private String nameNode;
    private String keyPath;
    private String destinationUri;
    private String gitUri;
    private String gitBranch;
    public static void main(final String[] args) throws Exception {
        run(GitMain.class, args);
    }

    @VisibleForTesting
    void setNameNode(final String nameNode) {
        this.nameNode = nameNode;
    }

    @Override
    protected void run(final String[] args) throws Exception {
        System.out.println("=============================================");
        System.out.println("Oozie Git Action Configuration");
        System.out.println("=============================================");

        final Configuration actionConf = prepareActionConf();
        parseActionConfiguration(actionConf);
        final File localKey = getLocalKeyFile();
        final GitOperations gitRepo = new GitOperations(new URI(gitUri), gitBranch, localKey);

        try {
            gitRepo.cloneRepoToFS(new Path(destinationUri));
        }
        catch (final IOException | GitOperationsException e){
            System.err.println(e.getMessage());
            throw e;
        }
    }

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File created without user input")
    private Configuration prepareActionConf() {
        final Configuration actionConf = new Configuration(false);

        final String actionXml = System.getProperty(OOZIE_ACTION_CONF);
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [" + OOZIE_ACTION_CONF + "]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));
        return actionConf;
    }

    private File getLocalKeyFile() throws IOException, URISyntaxException {
        File localKey = null;

        if (keyPath != null) {
            localKey = getKeyFromFS(new Path(keyPath));
        }

        return localKey;
    }

    /**
     * Gathers the Git authentication key from a FileSystem and copies it to a local
     * filesystem location
     *
     * @param location where the key is located (an HDFS URI)
     * @return the location to where the key was saved
     */
    @VisibleForTesting
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File created without user input")
    File getKeyFromFS(final Path location) throws IOException, URISyntaxException {
        final String keyCopyMsg = "Copied keys to local container!";

        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.newInstance(new URI(nameNode), conf);

        final File key = createTempDir("git");

        fs.copyToLocalFile(location, new Path("file:///" +
            key.getAbsolutePath() + "/privkey"));
        System.out.println(keyCopyMsg);

        return new File(key.getAbsolutePath() + "/privkey");
    }

    /**
     * Create a local temporary directory
     *
     * @param prefix string to use as a prefix to the directory
     * @return file path of temp. directory (will be set to delete on exit)
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File created without user input")
    static File createTempDir(final String prefix) throws IOException {
        final File tempDir = new File(Files.createTempDirectory(
            Paths.get("."),
            prefix + "_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                .fromString("rwx------")))
            .toString());
        tempDir.deleteOnExit();

        final String localMkdirMsg = "Local mkdir called creating temp. dir at: " + tempDir.getAbsolutePath();
        System.out.println(localMkdirMsg);

        return tempDir;
    }

    /**
     * Validate a URI is well formed and has a scheme
     *
     * @param testUri URI string to test
     * @return URI from string
     * @throws OozieActionConfiguratorException if the <code>testUri</code> fails any validity checks
     */
    private URI isValidUri(final String testUri) throws OozieActionConfiguratorException {
        final URI uri;
        try {
            uri = new URI(testUri);
        } catch (final URISyntaxException e) {
            throw new OozieActionConfiguratorException("Action Configuration does not have "
                    + "a proper URI: " + testUri + " exception "
                    + e.toString());
        }
        if (uri.getScheme() == null) {
            throw new OozieActionConfiguratorException("Action Configuration does not have "
                    + "a proper URI " + testUri + " null scheme.");
        }
        return uri;
    }

    /**
     * Parse action configuration and set configuration variables
     *
     * @param actionConf Oozie action configuration
     * @throws OozieActionConfiguratorException upon any required properties missing
     */
    private void parseActionConfiguration(final Configuration actionConf) throws OozieActionConfiguratorException {
        final GitActionExecutor.ActionConfVerifier confChecker = new GitActionExecutor.ActionConfVerifier(actionConf);

        nameNode = confChecker.checkAndGetTrimmed(GitActionExecutor.NAME_NODE);
        destinationUri = confChecker.checkAndGetTrimmed(GitActionExecutor.DESTINATION_URI);
        try {
            final FileSystem fs = FileSystem.get(isValidUri(destinationUri), actionConf);
            destinationUri = fs.makeQualified(new Path(destinationUri)).toString();
        } catch (final IOException e) {
            throw new OozieActionConfiguratorException("Action Configuration does not have "
                    + "a valid filesystem for URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        }
        gitUri = isValidUri(confChecker.checkAndGetTrimmed(GitActionExecutor.GIT_URI)).toString();
        gitBranch = actionConf.get(GitActionExecutor.GIT_BRANCH);
        keyPath = actionConf.get(GitActionExecutor.KEY_PATH);
    }
}
