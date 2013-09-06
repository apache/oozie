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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class ShareLibService implements Service {

    public static final String LAUNCHERJAR_LIB_RETENTION = CONF_PREFIX
            + "ShareLibService.temp.sharelib.retention.days";

    private Services services;
    private Map<String, List<Path>> shareLibLauncherMap = new HashMap<String, List<Path>>();
    private static XLog LOG = XLog.getLog(ShareLibService.class);

    public void init(Services services) throws ServiceException {
        this.services = services;
        String formattedDate = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance(
                TimeZone.getTimeZone("GMT")).getTime());
        Path tmpShareLibPath = new Path(services.get(WorkflowAppService.class).getSystemLibPath(), "tmp-"
                + formattedDate);
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        URI uri = tmpShareLibPath.toUri();
        try {
            FileSystem fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
            copyLauncherJarsToShareLib(fs, tmpShareLibPath);
            purgeShareLibs(fs, tmpShareLibPath);
        }
        catch (Exception e) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), e.getMessage(), e);
        }
    }

    private void copyLauncherJarsToShareLib(FileSystem fs, Path tmpShareLibPath) throws IOException,
            ClassNotFoundException {
        ActionService actionService = Services.get().get(ActionService.class);
        List<Class> classes = JavaActionExecutor.getCommonLauncherClasses();
        Path baseDir = new Path(tmpShareLibPath, JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        copyJarContainingClasses(classes, fs, baseDir, JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        Set<String> actionTypes = actionService.getActionTypes();
        for (String key : actionTypes) {
            ActionExecutor executor = actionService.getExecutor(key);
            if (executor instanceof JavaActionExecutor) {
                JavaActionExecutor jexecutor = (JavaActionExecutor) executor;
                classes = jexecutor.getLauncherClasses();
                if (classes != null) {
                    String type = executor.getType();
                    Path executorDir = new Path(baseDir, type);
                    copyJarContainingClasses(classes, fs, executorDir, type);
                }
            }
        }
    }

    private void copyJarContainingClasses(List<Class> classes, FileSystem fs, Path executorDir, String type)
            throws IOException {
        fs.mkdirs(executorDir);
        fs.setPermission(executorDir, FsPermission.valueOf("-rwxr-xr-x"));
        Set<String> localJarSet = new HashSet<String>();
        for (Class c : classes) {
            String localJar = findContainingJar(c);
            if (localJar != null) {
                localJarSet.add(localJar);
            }
            else {
                throw new IOException("No jar containing " + c + " found");
            }
        }
        List<Path> listOfPaths = new ArrayList<Path>();
        for (String localJarStr : localJarSet) {
            File localJar = new File(localJarStr);
            fs.copyFromLocalFile(new Path(localJar.getPath()), executorDir);
            Path path = new Path(executorDir, localJar.getName());
            fs.setPermission(path, FsPermission.valueOf("-rw-r--r--"));
            listOfPaths.add(path);
            LOG.info(localJar.getName() + " uploaded to " + executorDir.toString());
        }
        shareLibLauncherMap.put(type, listOfPaths);
    }

    public List<Path> getActionSystemLibCommonJars(String type) {
        return shareLibLauncherMap.get(type);
    }

    @VisibleForTesting
    protected String findContainingJar(Class clazz) {
        ClassLoader loader = clazz.getClassLoader();
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration itr = loader.getResources(classFile); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                        // URLDecoder is a misnamed class, since it actually
                        // decodes
                        // x-www-form-urlencoded MIME type rather than actual
                        // URL encoding (which the file path has). Therefore it
                        // would
                        // decode +s to ' 's which is incorrect (spaces are
                        // actually
                        // either unencoded or encoded as "%20"). Replace +s
                        // first, so
                        // that they are kept sacred during the decoding
                        // process.
                        toReturn = toReturn.replaceAll("\\+", "%2B");
                        toReturn = URLDecoder.decode(toReturn, "UTF-8");
                        toReturn = toReturn.replaceAll("!.*$", "");
                        return toReturn;
                    }
                }
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return null;
    }

    private void purgeShareLibs(FileSystem fs, Path launcherJarLibPath) throws IOException, ParseException {
        Configuration conf = services.getConf();
        Path executorLibBasePath = services.get(WorkflowAppService.class).getSystemLibPath();
        PathFilter directoryFilter = new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("tmp-");
            }
        };
        FileStatus[] dirList = fs.listStatus(executorLibBasePath, directoryFilter);
        Arrays.sort(dirList, new Comparator<FileStatus>() {
            // sort in desc order
            @Override
            public int compare(FileStatus o1, FileStatus o2) {
                return o2.getPath().getName().compareTo(o1.getPath().getName());
            }

        });
        Date current = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
        // Always keep top two, so start counter from 3
        long retentionTime = 1000 * 60 * 60 * 24 * conf.getInt(LAUNCHERJAR_LIB_RETENTION, 7);
        for (int i = 2; i < dirList.length; i++) {
            Path dirPath = dirList[i].getPath();
            Date ts = new SimpleDateFormat("yyyyMMddHHmmss").parse(dirPath.getName().split("tmp-", -1)[1]);
            if ((current.getTime() - ts.getTime()) > retentionTime) {
                fs.delete(dirPath, true);
                LOG.info("Deleted old launcher jar lib directory {0}", dirPath.getName());
            }
        }
    }

    public void destroy() {
        shareLibLauncherMap.clear();
    }

    public Class<? extends Service> getInterface() {
        return ShareLibService.class;
    }

}
