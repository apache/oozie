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

import org.apache.commons.io.FileUtils;

import org.apache.oozie.util.XLog;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.Daemon;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

class GitServer {
    private static final XLog LOG = XLog.getLog(GitServer.class);

    /**
     * A simple git server serving anynymous git: protocol
     */
    private final Map<String, Repository> repositories = new HashMap<>();
    private Daemon server;
    private final int localPort;

    GitServer() throws IOException {
        LOG.info("Creating Git server");

        this.localPort = findAvailablePort();

        LOG.info("Git server created, port {0} will be used", this.localPort);
    }

    void start() throws IOException {
        LOG.info("Starting Git server on port {0}", this.localPort);

        this.server = new Daemon(new InetSocketAddress(this.localPort));
        this.server.getService("git-receive-pack").setEnabled(true);
        this.server.setRepositoryResolver(new EmptyRepositoryResolverImplementation());
        this.server.start();

        LOG.info("Git server started");
    }

    int getLocalPort() {
        return localPort;
    }

    private int findAvailablePort() throws IOException {
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            final int availablePort = serverSocket.getLocalPort();
            LOG.info("Found available port {0}", availablePort);
            return availablePort;
        }
    }

    void stopAndCleanupReposServer() {
        cleanUpRepos();
        this.server.stop();
    }

    /**
     * A method to:
     * <ul>
     *     <li>remove all files on disk for all repositories</li>
     *     <li>clear the repositories listed for the {@link GitServer}</li>
     * </ul>
     */
    private void cleanUpRepos() {
        for (final Repository repository : repositories.values()) {
            final File workTree = repository.getWorkTree();
            try {
                FileUtils.deleteDirectory(workTree.getParentFile());
            }
            catch (final IOException e) {
                LOG.warn("Could not delete parent directory of working tree: ", e);
            }
        }
        repositories.clear();
    }

    /**
     * A simple class RepositoryResolver to provide an empty repository for non-existant repo requests
     */
    private final class EmptyRepositoryResolverImplementation implements
            RepositoryResolver<DaemonClient> {

        @Override
        public Repository open(final DaemonClient client, final String name) {
            Repository repo = repositories.get(name);
            if (repo == null) {
                try {
                    final Path workDir = Files.createTempDirectory("GitTestSetup");
                    //git init
                    repo = FileRepositoryBuilder.create(new File(workDir.resolve(name).toFile(), ".git"));
                    repo.create();
                    // commit into the filesystem
                    final Git git = new Git(repo);
                    // one needs an initial commit for a proper clone
                    addEmptyCommit(git);
                    git.close();
                    // serve the Git repo
                    repositories.put(name, repo);
                }
                catch (final Exception e) {
                    throw new RuntimeException();
                }
            }
            return repo;
        }

        /**
         * Add an empty commit to a Git repository
         * @param git a Git repository to add an empty commit to
         */
        void addEmptyCommit(final Git git) {
            try {
                git.commit().setMessage("Empty commit").call();
            }
            catch (final GitAPIException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
