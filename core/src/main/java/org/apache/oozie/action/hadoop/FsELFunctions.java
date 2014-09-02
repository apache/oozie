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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;

/**
 * EL function for fs action executor.
 */
public class FsELFunctions {

    private static FileSystem getFileSystem(URI uri) throws HadoopAccessorException {
        WorkflowJob workflow = DagELFunctions.getWorkflow();
        String user = workflow.getUser();
        String group = workflow.getGroup();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        JobConf conf = has.createJobConf(uri.getAuthority());
        return has.createFileSystem(user, uri, conf);
    }

    /**
     * Get file status.
     *
     * @param pathUri fs path uri
     * @return file status
     * @throws URISyntaxException
     * @throws IOException
     * @throws Exception
     */
    private static FileStatus getFileStatus(String pathUri) throws Exception {
        URI uri = new URI(pathUri);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        Path p = new Path(path);
        return fs.exists(p) ? fs.getFileStatus(p) : null;
    }

    /**
     * Return if a path exists.
     *
     * @param pathUri file system path uri.
     * @return <code>true</code> if the path exists, <code>false</code> if it does not.
     * @throws Exception
     */
    public static boolean fs_exists(String pathUri) throws Exception {
        URI uri = new URI(pathUri);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        return fs.exists(new Path(path));
    }

    /**
     * Return if a path is a directory.
     *
     * @param pathUri fs path uri.
     * @return <code>true</code> if the path exists and it is a directory, <code>false</code> otherwise.
     * @throws Exception
     */
    public static boolean fs_isDir(String pathUri) throws Exception {
        boolean isDir = false;
        FileStatus fileStatus = getFileStatus(pathUri);
        if (fileStatus != null) {
            isDir = fileStatus.isDir();
        }
        return isDir;
    }

    /**
     * Return the len of a file.
     *
     * @param pathUri file system path uri.
     * @return the file len in bytes, -1 if the file does not exist or if it is a directory.
     * @throws Exception
     */
    public static long fs_fileSize(String pathUri) throws Exception {
        long len = -1;
        FileStatus fileStatus = getFileStatus(pathUri);
        if (fileStatus != null) {
            len = fileStatus.getLen();
        }
        return len;
    }

    /**
     * Return the size of all files in the directory, it is not recursive.
     *
     * @param pathUri file system path uri.
     * @return the size of all files in the directory, -1 if the directory does not exist or if it is a file.
     * @throws Exception
     */
    public static long fs_dirSize(String pathUri) throws Exception {
        URI uri = new URI(pathUri);
        String path = uri.getPath();
        long size = -1;
        try {
            FileSystem fs = getFileSystem(uri);
            Path p = new Path(path);
            if (fs.exists(p) && !fs.isFile(p)) {
                FileStatus[] stati = fs.listStatus(p);
                size = 0;
                if (stati != null) {
                    for (FileStatus status : stati) {
                        if (!status.isDir()) {
                            size += status.getLen();
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return size;
    }

    /**
     * Return the file block size in bytes.
     *
     * @param pathUri file system path uri.
     * @return the block size of the file in bytes, -1 if the file does not exist or if it is a directory.
     * @throws Exception
     */
    public static long fs_blockSize(String pathUri) throws Exception {
        long blockSize = -1;
        FileStatus fileStatus = getFileStatus(pathUri);
        if (fileStatus != null) {
            blockSize = fileStatus.getBlockSize();
        }
        return blockSize;
    }

}
