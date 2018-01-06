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

package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.oozie.util.XLog;

public class RawLocalFileSystem extends FileSystem {

    public RawLocalFileSystem() {
        // No error here as FileSystem should initialize when loading from ServiceLoader
        XLog.getLog(this.getClass()).info("Initializing restricted local file system");
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public String getScheme() {
        // Called in FileSystem initialization using ServiceLoader
        return "file";
    }

    @Override
    protected URI getCanonicalUri() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected int getDefaultPort() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public String getCanonicalServiceName() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path makeQualified(Path path) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected void checkPath(Path path) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, short replication) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
            short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
            short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected FSDataOutputStream primitiveCreate(Path f, FsPermission absolutePermission, EnumSet<CreateFlag> flag,
            int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt)
            throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected boolean primitiveMkdir(Path f, FsPermission absolutePermission) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected void primitiveMkdir(Path f, FsPermission absolutePermission, boolean createParent) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
            short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
            short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean createNewFile(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream append(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public short getReplication(Path src) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean setReplication(Path src, short replication) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected void rename(Path src, Path dst, Rename... options) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean delete(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected void processDeleteOnExit() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean exists(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean isDirectory(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean isFile(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public long getLength(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
            throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path getHomeDirectory() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected Path getInitialWorkingDirectory() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public long getUsed() throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public long getBlockSize(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public long getDefaultBlockSize() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public short getDefaultReplication() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public short getDefaultReplication(Path path) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected Path fixRelativePart(Path p) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException,
            UnsupportedFileSystemException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus getFileLinkStatus(Path f)
            throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean supportsSymlinks() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    protected Path resolveLink(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FsStatus getStatus() throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public URI getUri() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        throw new UnsupportedOperationException("Accessing local file system is not allowed");
    }

}
