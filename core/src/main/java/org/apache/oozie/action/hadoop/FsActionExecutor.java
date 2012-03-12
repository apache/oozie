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
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

/**
 * File system action executor. <p/> This executes the file system mkdir, move and delete commands
 */
public class FsActionExecutor extends ActionExecutor {

    public FsActionExecutor() {
        super("fs");
    }

    Path getPath(Element element, String attribute) {
        String str = element.getAttributeValue(attribute).trim();
        return new Path(str);
    }

    void validatePath(Path path, boolean withScheme) throws ActionExecutorException {
        String scheme = path.toUri().getScheme();
        if (withScheme) {
            if (scheme == null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS001",
                                                  "Missing scheme in path [{0}]", path);
            }
            else {
                if (!scheme.equals("hdfs")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS002",
                                                      "Scheme [{0}] not supported in path [{1}]", scheme, path);
                }
            }
        }
        else { 
            if (scheme != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS003",
                                                  "Scheme [{0}] not allowed in path [{1}]", scheme, path);
            }
        }
    }

    void validateSameNN(Path source, Path dest) throws ActionExecutorException {
        Path destPath = new Path(source, dest);
        String t = destPath.toUri().getScheme() + destPath.toUri().getAuthority();
        String s = source.toUri().getScheme() + source.toUri().getAuthority();

        //checking whether NN prefix of source and target is same. can modify this to adjust for a set of multiple whitelisted NN
        if(!t.equals(s)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS007",
                    "move, target NN URI different from that of source", dest);
        }
    }

    @SuppressWarnings("unchecked")
    void doOperations(Context context, Element element) throws ActionExecutorException {
        try {
            FileSystem fs = context.getAppFileSystem();
            boolean recovery = fs.exists(getRecoveryPath(context));
            if (!recovery) {
                fs.mkdirs(getRecoveryPath(context));
            }
            for (Element commandElement : (List<Element>) element.getChildren()) {
                String command = commandElement.getName();
                if (command.equals("mkdir")) {
                    Path path = getPath(commandElement, "path");
                    mkdir(context, path);
                }
                else {
                    if (command.equals("delete")) {
                        Path path = getPath(commandElement, "path");
                        delete(context, path);
                    }
                    else {
                        if (command.equals("move")) {
                            Path source = getPath(commandElement, "source");
                            Path target = getPath(commandElement, "target");
                            move(context, source, target, recovery);
                        }
                        else {
                            if (command.equals("chmod")) {
                                Path path = getPath(commandElement, "path");
                                String str = commandElement.getAttributeValue("dir-files");
                                boolean dirFiles = (str == null) || Boolean.parseBoolean(str);
                                String permissionsMask = commandElement.getAttributeValue("permissions").trim();
                                chmod(context, path, permissionsMask, dirFiles);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * @param path
     * @param context
     * @return FileSystem
     * @throws HadoopAccessorException
     */
    private FileSystem getFileSystemFor(Path path, Context context) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        JobConf conf = has.createJobConf(path.toUri().getAuthority());
        XConfiguration.copy(context.getProtoActionConf(), conf);
        return has.createFileSystem(user, path.toUri(), conf);
    }

    /**
     * @param path
     * @param user
     * @param group
     * @return FileSystem
     * @throws HadoopAccessorException
     */
    private FileSystem getFileSystemFor(Path path, String user) throws HadoopAccessorException {
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        JobConf jobConf = has.createJobConf(path.toUri().getAuthority());
        return has.createFileSystem(user, path.toUri(), jobConf);
    }

    void mkdir(Context context, Path path) throws ActionExecutorException {
        try {
            validatePath(path, true);
            FileSystem fs = getFileSystemFor(path, context);

            if (!fs.exists(path)) {
                if (!fs.mkdirs(path)) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS004",
                                                      "mkdir, path [{0}] could not create directory", path);
                }
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * Delete path
     *
     * @param context
     * @param path
     * @throws ActionExecutorException
     */
    public void delete(Context context, Path path) throws ActionExecutorException {
        try {
            validatePath(path, true);
            FileSystem fs = getFileSystemFor(path, context);

            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS005",
                                                      "delete, path [{0}] could not delete path", path);
                }
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * Delete path
     *
     * @param user
     * @param group
     * @param path
     * @throws ActionExecutorException
     */
    public void delete(String user, String group, Path path) throws ActionExecutorException {
        try {
            validatePath(path, true);
            FileSystem fs = getFileSystemFor(path, user);

            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS005",
                            "delete, path [{0}] could not delete path", path);
                }
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * Move source to target
     *
     * @param context
     * @param source
     * @param target
     * @param recovery
     * @throws ActionExecutorException
     */
    public void move(Context context, Path source, Path target, boolean recovery) throws ActionExecutorException {
        try {
            validatePath(source, true);
            validateSameNN(source, target);
            FileSystem fs = getFileSystemFor(source, context);

            if (!fs.exists(source) && !recovery) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS006",
                                                  "move, source path [{0}] does not exist", source);
            }

            if (!fs.rename(source, target) && !recovery) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS008",
                                                  "move, could not move [{0}] to [{1}]", source, target);
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    void chmod(Context context, Path path, String permissions, boolean dirFiles) throws ActionExecutorException {
        try {
            validatePath(path, true);
            FileSystem fs = getFileSystemFor(path, context);

            if (!fs.exists(path)) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS009",
                                                  "chmod, path [{0}] does not exist", path);
            }

            FileStatus pathStatus = fs.getFileStatus(path);

            Path[] paths;
            if (dirFiles && pathStatus.isDir()) {
                FileStatus[] filesStatus = fs.listStatus(path);
                paths = new Path[filesStatus.length];
                for (int i = 0; i < filesStatus.length; i++) {
                    paths[i] = filesStatus[i].getPath();
                }
            }
            else {
                paths = new Path[]{path};
            }

            FsPermission newFsPermission = createShortPermission(permissions, path);
            fs.setPermission(path, newFsPermission);
            for (Path p : paths) {
                fs.setPermission(p, newFsPermission);
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    FsPermission createShortPermission(String permissions, Path path) throws ActionExecutorException {
        if (permissions.length() == 3) {
            char user = permissions.charAt(0);
            char group = permissions.charAt(1);
            char other = permissions.charAt(2);
            int useri = user - '0';
            int groupi = group - '0';
            int otheri = other - '0';
            int mask = useri * 100 + groupi * 10 + otheri;
            short omask = Short.parseShort(Integer.toString(mask), 8);
            return new FsPermission(omask);
        }
        else {
            if (permissions.length() == 10) {
                return FsPermission.valueOf(permissions);
            }
            else {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FS010",
                                                  "chmod, path [{0}] invalid permissions mask [{1}]", path, permissions);
            }
        }
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            context.setStartData("-", "-", "-");
            Element actionXml = XmlUtils.parseXml(action.getConf());
            doOperations(context, actionXml);
            context.setExecutionData("OK", null);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = externalStatus.equals("OK") ? WorkflowAction.Status.OK :
                                       WorkflowAction.Status.ERROR;
        context.setEndData(status, getActionSignal(status));
        if (!context.getProtoActionConf().getBoolean("oozie.action.keep.action.dir", false)) {
            try {
                FileSystem fs = context.getAppFileSystem();
                fs.delete(context.getActionDir(), true);
            }
            catch (Exception ex) {
                throw convertException(ex);
            }
        }
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }

    /**
     * @param context
     * @return
     * @throws HadoopAccessorException
     * @throws IOException
     * @throws URISyntaxException
     */
    public Path getRecoveryPath(Context context) throws HadoopAccessorException, IOException, URISyntaxException {
        return new Path(context.getActionDir(), "fs-" + context.getRecoveryId());
    }

}
