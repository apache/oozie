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
import java.util.ArrayList;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.JschConfigSessionFactory;
import org.eclipse.jgit.transport.OpenSshConfig;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.util.FS;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

class GitOperations {
    private final URI srcURL;
    private final String branch;
    private final File credentialFile;

    GitOperations(final URI gitSrc, final String branch, final File credentialFile) {
       this.srcURL = gitSrc;
       this.branch = branch;
       this.credentialFile = credentialFile;
    }

    /**
     * Used by GitOperations to wrap a Throwable when an Exception occurs
     */
    @SuppressWarnings("serial")
    static class GitOperationsException extends Exception {
        GitOperationsException(final String message, Throwable throwable) {
            super(message, throwable);
        }
    }

    /**
     * Clones a Git repository
     * @param outputDir location in which to clone the Git repository
     * @throws GitOperationsException if the Git clone fails
     */
    private void cloneRepo(final File outputDir) throws GitOperationsException {
        final SshSessionFactory sshSessionFactory = new JschConfigSessionFactory() {
            @Override
            protected void configure(final OpenSshConfig.Host host, final Session session) {
                // nop
            }

            @Override
            protected JSch createDefaultJSch(final FS fs) throws JSchException {
                JSch.setConfig("StrictHostKeyChecking", "no");
                final JSch defaultJSch = super.createDefaultJSch(fs);

                if (credentialFile != null) {
                    defaultJSch.addIdentity(credentialFile.toString());
                }

                return defaultJSch;
            }
        };

        final CloneCommand cloneCommand = Git.cloneRepository();
        cloneCommand.setURI(srcURL.toString());

        if (srcURL.getScheme().toLowerCase().equals("ssh")) {
          cloneCommand.setTransportConfigCallback(new TransportConfigCallback() {
              @Override
              public void configure(final Transport transport) {
                  final SshTransport sshTransport = (SshTransport)transport;
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
        } catch (final GitAPIException e) {
            throw new GitOperationsException("Unable to clone Git repo: ", e);
        }
    }

    /**
     * Clone a Git repo up to a FileSystem
     *
     * @param destination - Hadoop FileSystem path to which repository should be cloned
     * @throws GitOperationsException if the Git operations fail
     * @throws IOException if the HDFS or local file system operations fail
     */
    @SuppressFBWarnings(value ="PATH_TRAVERSAL_IN", justification = "Path is created runtime")
    String cloneRepoToFS(final Path destination) throws IOException, GitOperationsException {
        final File tempDir = GitMain.createTempDir("git");

        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(destination.toUri(), conf);

        cloneRepo(tempDir);

        // create a list of files and directories to upload
        final File src = new File(tempDir.getAbsolutePath());
        final ArrayList<Path> srcs = new ArrayList<Path>(1000);
        final File[] sourceFiles = src.listFiles();
        if (sourceFiles != null) {
            for (final File sourceFile : sourceFiles) {
                srcs.add(new Path(sourceFile.toString()));
            }
        }

        System.out.println("Finished cloning to local");

        fs.mkdirs(destination);
        fs.copyFromLocalFile(false, true, srcs.toArray(new Path[0]), destination);

        System.out.println("Finished copying to " + destination.toString());

        return destination.toString();
    }

}
