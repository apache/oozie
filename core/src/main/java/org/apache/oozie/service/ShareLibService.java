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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

public class ShareLibService implements Service {

    public static final String LAUNCHERJAR_LIB_RETENTION = CONF_PREFIX + "ShareLibService.temp.sharelib.retention.days";

    public static final String SHARELIB_MAPPING_FILE = CONF_PREFIX + "ShareLibService.mapping.file";

    public static final String SHIP_LAUNCHER_JAR = "oozie.action.ship.launcher.jar";

    private static final String PERMISSION_STRING = "-rwxr-xr-x";

    public static final String LAUNCHER_PREFIX = "launcher_";

    public static final String SHARED_LIB_PREFIX = "lib_";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private Services services;

    private Map<String, List<Path>> shareLibMap = new HashMap<String, List<Path>>();

    private Map<String, List<Path>> launcherLibMap = new HashMap<String, List<Path>>();

    private static XLog LOG = XLog.getLog(ShareLibService.class);

    private String sharelibMappingFile;

    private boolean isShipLauncherEnabled = false;

    public static String SHARE_LIB_CONF_PREFIX = "oozie";

    private boolean shareLibLoadAttempted = false;

    FileSystem fs;

    @Override
    public void init(Services services) throws ServiceException {
        this.services = services;
        try {
            sharelibMappingFile = services.getConf().get(SHARELIB_MAPPING_FILE, "");
            isShipLauncherEnabled = services.getConf().getBoolean(SHIP_LAUNCHER_JAR, false);
            Path launcherlibPath = getLauncherlibPath();
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            URI uri = launcherlibPath.toUri();
            fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
            updateLauncherLib();
            updateShareLib();
            purgeLibs(fs, LAUNCHER_PREFIX);
            purgeLibs(fs, SHARED_LIB_PREFIX);
        }
        catch (Exception e) {
            LOG.error("Not able to cache shareLib. Admin need to issue oozlie cli command to update sharelib.", e);
        }

    }

    /**
     * Recursively change permissions.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */

    private void updateLauncherLib() throws IOException {
        if (isShipLauncherEnabled) {
            Path launcherlibPath = getLauncherlibPath();
            setupLauncherLibPath(fs, launcherlibPath);
            recursiveChangePermissions(fs, launcherlibPath, FsPermission.valueOf(PERMISSION_STRING));
        }

    }

    /**
     * Copy launcher jars to Temp directory
     *
     * @param fs the FileSystem
     * @param tmpShareLibPath destination path
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    private void setupLauncherLibPath(FileSystem fs, Path tmpLauncherLibPath) throws IOException {

        ActionService actionService = Services.get().get(ActionService.class);
        List<Class> classes = JavaActionExecutor.getCommonLauncherClasses();
        Path baseDir = new Path(tmpLauncherLibPath, JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        copyJarContainingClasses(classes, fs, baseDir, JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        Set<String> actionTypes = actionService.getActionTypes();
        for (String key : actionTypes) {
            ActionExecutor executor = actionService.getExecutor(key);
            if (executor instanceof JavaActionExecutor) {
                JavaActionExecutor jexecutor = (JavaActionExecutor) executor;
                classes = jexecutor.getLauncherClasses();
                if (classes != null) {
                    String type = executor.getType();
                    Path executorDir = new Path(tmpLauncherLibPath, type);
                    copyJarContainingClasses(classes, fs, executorDir, type);
                }
            }
        }
    }

    /**
     * Recursive change permissions.
     *
     * @param fs the FileSystem
     * @param path the Path
     * @param perm is permission
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void recursiveChangePermissions(FileSystem fs, Path path, FsPermission fsPerm) throws IOException {
        fs.setPermission(path, fsPerm);
        FileStatus[] filesStatus = fs.listStatus(path);
        for (int i = 0; i < filesStatus.length; i++) {
            Path p = filesStatus[i].getPath();
            if (filesStatus[i].isDir()) {
                recursiveChangePermissions(fs, p, fsPerm);
            }
            else {
                fs.setPermission(p, fsPerm);
            }
        }
    }

    /**
     * Copy jar containing classes.
     *
     * @param classes the classes
     * @param fs the FileSystem
     * @param executorDir is Path
     * @param type is actionKey
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void copyJarContainingClasses(List<Class> classes, FileSystem fs, Path executorDir, String type)
            throws IOException {
        fs.mkdirs(executorDir);
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
            listOfPaths.add(path);
            LOG.info(localJar.getName() + " uploaded to " + executorDir.toString());
        }
        launcherLibMap.put(type, listOfPaths);

    }

    /**
     * Gets the path recursively.
     *
     * @param fs the FileSystem
     * @param rootDir the root directory
     * @param listOfPaths the list of paths
     * @return the path recursively
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void getPathRecursively(FileSystem fs, Path rootDir, List<Path> listOfPaths) throws IOException {
        if (rootDir == null) {
            return;
        }

        FileStatus[] status = fs.listStatus(rootDir);
        if (status == null) {
            LOG.info("Shared lib " + rootDir + " doesn't exist, not adding to cache");
            return;
        }

        for (FileStatus file : status) {
            if (file.isDir()) {
                getPathRecursively(fs, file.getPath(), listOfPaths);
            }
            else {
                listOfPaths.add(file.getPath());
            }
        }
    }

    /**
     * Gets the action system lib common jars.
     *
     * @param actionKey the action key
     * @return List of paths
     * @throws IOException
     */
    public List<Path> getShareLibJars(String actionKey) throws IOException {
        // Sharelib map is empty means that on previous or startup attempt of
        // caching sharelib has failed.Trying to reload
        if (shareLibMap.isEmpty() && !shareLibLoadAttempted) {
            synchronized (ShareLibService.class) {
                if (shareLibMap.isEmpty()) {
                    updateShareLib();
                    shareLibLoadAttempted = true;
                }
            }
        }
        return shareLibMap.get(actionKey);
    }

    /**
     * Gets the launcher jars.
     *
     * @param actionKey the action key
     * @return launcher jars paths
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public List<Path> getSystemLibJars(String actionKey) throws IOException {
        List<Path> returnList = new ArrayList<Path>();
        // Sharelib map is empty means that on previous or startup attempt of
        // caching launcher jars has failed.Trying to reload
        if (isShipLauncherEnabled) {
            if (launcherLibMap.isEmpty()) {
                synchronized (ShareLibService.class) {
                    if (launcherLibMap.isEmpty()) {
                        updateLauncherLib();
                    }
                }
            }
            returnList = launcherLibMap.get(actionKey);
        }
        if (actionKey.equals(JavaActionExecutor.OOZIE_COMMON_LIBDIR)) {
            List<Path> sharelibList = getShareLibJars(actionKey);
            if (sharelibList != null) {
                returnList.addAll(sharelibList);
            }
        }
        return returnList;
    }

    /**
     * Find containing jar containing.
     *
     * @param clazz the clazz
     * @return the string
     */
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

    /**
     * Purge libs.
     *
     * @param fs the fs
     * @param prefix the prefix
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    private void purgeLibs(FileSystem fs, final String prefix) throws IOException, ParseException {
        Configuration conf = services.getConf();
        Path executorLibBasePath = services.get(WorkflowAppService.class).getSystemLibPath();

        PathFilter directoryFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(prefix);
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
            String name = dirPath.getName().toString();
            String time = name.substring(prefix.length());
            Date d = dateFormat.parse(time);
            if ((current.getTime() - d.getTime()) > retentionTime) {
                fs.delete(dirPath, true);
                LOG.info("Deleted old launcher jar lib directory {0}", dirPath.getName());
            }
        }
    }

    @Override
    public void destroy() {
        shareLibMap.clear();
        launcherLibMap.clear();

    }

    @Override
    public Class<? extends Service> getInterface() {
        return ShareLibService.class;
    }

    /**
     * Update share lib cache.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void updateShareLib() throws IOException {
        Map<String, List<Path>> tempShareLibMap = new HashMap<String, List<Path>>();

        if (!StringUtils.isEmpty(sharelibMappingFile)) {
            loadShareLibMetaFile(tempShareLibMap, sharelibMappingFile);
        }
        else {
            loadShareLibfromDFS(tempShareLibMap);
        }
        shareLibMap = tempShareLibMap;

    }

    /**
     * Update share lib cache. Parse the share lib directory and each sub
     * directory is a action key
     *
     * @param shareLibMap the share lib jar map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void loadShareLibfromDFS(Map<String, List<Path>> shareLibMap) throws IOException {
        Path shareLibpath = getLatestLibPath(services.get(WorkflowAppService.class).getSystemLibPath(),
                SHARED_LIB_PREFIX);

        if (shareLibpath == null) {
            LOG.info("No share lib directory found");
            return;

        }

        FileStatus[] dirList = fs.listStatus(shareLibpath);

        if (dirList == null) {
            return;
        }

        for (FileStatus dir : dirList) {
            List<Path> listOfPaths = new ArrayList<Path>();
            getPathRecursively(fs, dir.getPath(), listOfPaths);
            shareLibMap.put(dir.getPath().getName(), listOfPaths);
            LOG.info("Share lib for " + dir.getPath().getName() + ":" +  listOfPaths);

        }

    }

    /**
     * Load share lib text file. Sharelib mapping files contains list of
     * key=value. where key is the action key and value is the DFS location of
     * sharelib files.
     *
     * @param shareLibMap the share lib jar map
     * @param sharelipFileMapping the sharelip file mapping
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("unchecked")
    private void loadShareLibMetaFile(Map<String, List<Path>> shareLibMap, String sharelibFileMapping)
            throws IOException {


        Path shareFileMappingPath= new Path(sharelibFileMapping);
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        FileSystem filesystem = FileSystem.get(has.createJobConf(shareFileMappingPath.toUri().getAuthority()));
        XConfiguration conf = new XConfiguration(filesystem.open(shareFileMappingPath));
        Iterator<Map.Entry<String, String>> it = conf.iterator();


        while (it.hasNext()) {
            Map.Entry<String, String> en = it.next();
            if (en.getKey().toLowerCase().startsWith(SHARE_LIB_CONF_PREFIX)) {
                String key = en.getKey().substring(SHARE_LIB_CONF_PREFIX.length() + 1);
                String pathList[] = en.getValue().split(",");
                List<Path> listOfPaths = new ArrayList<Path>();
                for (String dfsPath : pathList) {
                    getPathRecursively(fs, new Path(dfsPath), listOfPaths);
                }
                shareLibMap.put(key, listOfPaths);
                LOG.info("Share lib for " + en.getKey() + ":" +  listOfPaths);


            }
            else {
                LOG.info(" Not adding " + en.getKey() + " to sharelib, not prefix with " + SHARE_LIB_CONF_PREFIX);
            }

        }
    }

    /**
     * Gets the launcherlib path.
     *
     * @return the launcherlib path
     */
    private Path getLauncherlibPath() {
        String formattedDate = dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());
        Path tmpLauncherLibPath = new Path(services.get(WorkflowAppService.class).getSystemLibPath(), LAUNCHER_PREFIX
                + formattedDate);
        return tmpLauncherLibPath;
    }

    /**
     * Gets the Latest lib path.
     *
     * @param rootDir the root dir
     * @param prefix the prefix
     * @return latest lib path
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public Path getLatestLibPath(Path rootDir, final String prefix) throws IOException {
        Date max = new Date(0L);
        Path path = null;
        PathFilter directoryFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(prefix);
            }
        };

        FileStatus[] files = fs.listStatus(rootDir, directoryFilter);
        for (FileStatus file : files) {
            String name = file.getPath().getName().toString();
            String time = name.substring(prefix.length());
            Date d = null;
            try {
                d = dateFormat.parse(time);
            }
            catch (ParseException e) {
                continue;
            }
            if (d.compareTo(max) > 0) {
                path = file.getPath();
                max = d;
            }
        }
        return path;
    }
}
