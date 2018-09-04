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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class GitActionExecutor extends JavaActionExecutor {

    private static final String GIT_MAIN_CLASS_NAME =
            "org.apache.oozie.action.hadoop.GitMain";
    public static final String GIT_ACTION_TYPE = "git";

    static final String APP_NAME = "oozie.app.name";
    static final String WORKFLOW_ID = "oozie.workflow.id";
    static final String CALLBACK_URL = "oozie.callback.url";
    static final String RESOURCE_MANAGER = "oozie.resource.manager";
    static final String NAME_NODE = "oozie.name.node";
    static final String GIT_URI = "oozie.git.source.uri";
    static final String GIT_BRANCH = "oozie.git.branch";
    static final String DESTINATION_URI = "oozie.git.destination.uri";
    static final String KEY_PATH = "oozie.git.key.path";
    static final String ACTION_TYPE = "oozie.action.type";
    static final String ACTION_NAME = "oozie.action.name";

    public GitActionExecutor() {
        super(GIT_ACTION_TYPE);
    }

    @Override
    public List<Class<?>> getLauncherClasses() {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        try {
            classes.add(Class.forName(GIT_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS,
                GIT_MAIN_CLASS_NAME);
    }

    @Override
    public void initActionType() {
        super.initActionType();

        registerErrors();
    }

    private void registerErrors() {
        registerError(UnknownHostException.class.getName(), ErrorType.TRANSIENT, "GIT001");
        registerError(AccessControlException.class.getName(), ErrorType.NON_TRANSIENT,
                "GIT002");
        registerError(DiskChecker.DiskOutOfSpaceException.class.getName(),
                ErrorType.NON_TRANSIENT, "GIT003");
        registerError(org.apache.hadoop.hdfs.protocol.QuotaExceededException.class.getName(),
                ErrorType.NON_TRANSIENT, "GIT004");
        registerError(org.apache.hadoop.hdfs.server.namenode.SafeModeException.class.getName(),
                ErrorType.NON_TRANSIENT, "GIT005");
        registerError(ConnectException.class.getName(), ErrorType.TRANSIENT, "GIT006");
        registerError(JDOMException.class.getName(), ErrorType.ERROR, "GIT007");
        registerError(FileNotFoundException.class.getName(), ErrorType.ERROR, "GIT008");
        registerError(IOException.class.getName(), ErrorType.TRANSIENT, "GIT009");
        registerError(NullPointerException.class.getName(), ErrorType.ERROR, "GIT010");
    }

    @Override
    Configuration setupActionConf(Configuration actionConf, Context context,
                                  Element actionXml, Path appPath) throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);

        Namespace ns = actionXml.getNamespace();

        ActionConfVerifier confChecker = new ActionConfVerifier(actionConf);

        confChecker.checkTrimAndSet(GitActionExecutor.NAME_NODE, actionXml.getChild("name-node", ns));

        confChecker.checkTrimAndSet(GitActionExecutor.DESTINATION_URI,
                actionXml.getChild("destination-uri", ns));

        confChecker.checkTrimAndSet(GitActionExecutor.GIT_URI, actionXml.getChild("git-uri", ns));

        confChecker.trimAndSet(KEY_PATH, actionXml.getChild("key-path", ns));
        String keyPath = actionConf.get(KEY_PATH);
        if (keyPath != null && keyPath.length() > 0) {
            try {
                confChecker.verifyKeyPermissions(context.getAppFileSystem(), new Path(keyPath));
            } catch (HadoopAccessorException|URISyntaxException|IOException e){
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "GIT011", XLog
                        .format("Not able to verify permissions on key file {0}", keyPath), e);
            }
        }

        confChecker.trimAndSet(GIT_BRANCH, actionXml.getChild("branch", ns));

        actionConf.set(ACTION_TYPE, getType());
        actionConf.set(ACTION_NAME, GIT_ACTION_TYPE);

        return actionConf;
    }

    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return GIT_ACTION_TYPE;
    }

    static class ActionConfVerifier {
        private final Configuration actionConf;

        /**
         * Create ActionConfVerifier checker which will set action conf values and throw
         * ActionExecutorException's with the exception code provided
         *  @param  actionConf       the actionConf in which to set values
         *
         */
        ActionConfVerifier(Configuration actionConf) {
            this.actionConf = actionConf;
        }

        /**
         * Validates Git key permissions are secure on disk and throw an exception if not.
         * Otherwise exit out gracefully
         */
        void verifyKeyPermissions(FileSystem fs, Path keyPath) throws IOException, ActionExecutorException{
            String failedPermsWarning = "The permissions on the access key {0} are considered insecure: {1}";
            FileStatus status = fs.getFileStatus(keyPath);
            FsPermission perms = status.getPermission();
            // check standard permissioning for other's read access
            if (perms.getOtherAction().and(FsAction.READ) == FsAction.READ) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "GIT012", XLog
                        .format(failedPermsWarning, keyPath, perms.toString()));
            }
            // check if any ACLs have been specified which allow others read access
            if (perms.getAclBit()) {
                List<AclEntry> aclEntries = new ArrayList<>(fs.getAclStatus(keyPath).getEntries());
                for (AclEntry acl: aclEntries) {
                    if (acl.getType() == AclEntryType.OTHER && acl.getPermission().and(FsAction.READ) == FsAction.READ) {
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "GIT013", XLog
                                .format(failedPermsWarning, keyPath, perms.toString()));
                    }
                }
            }
        }

        /**
         * Calls helper function to verify value not null and throw an exception if so.
         * Otherwise, set actionConf value displayName to XML trimmed text value
         */
        void checkTrimAndSet(String displayName, Element value) {
            Preconditions.checkNotNull(value, "Action Configuration does not have [%s] property", displayName);
            actionConf.set(displayName, value.getTextTrim());
        }

        /**
        * Calls helper function to verify value not null and throw an exception if so.
        * Otherwise, set actionConf value displayName to value
        */
        void checkAndSet(String displayName, String value) {
            Preconditions.checkNotNull(value, "Action Configuration does not have [%s] property", displayName);
            actionConf.set(displayName, value);
        }

        /**
         * *f value is null, does nothing
         * If value is not null, sets actionConf value displayName to XML trimmed text value
         */
        void trimAndSet(String displayName, Element value) {
            if (value != null) {
                actionConf.set(displayName, value.getTextTrim());
            }
        }

        /**
         * Calls helper function to verify name not null and throw an exception if so.
         * Otherwise, returns actionConf value
         * @param name - actionConf value to return
         */
        String checkAndGetTrimmed(String name) {
            Preconditions.checkNotNull(actionConf.getTrimmed(name), "Action Configuration does not have [%s] property", name);
            return actionConf.getTrimmed(name);
        }
    }
}
