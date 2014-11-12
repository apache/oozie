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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
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
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.hadoop.utils.HadoopShims;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

import org.apache.oozie.ErrorCode;

public class ShareLibService implements Service, Instrumentable {

    public static final String LAUNCHERJAR_LIB_RETENTION = CONF_PREFIX + "ShareLibService.temp.sharelib.retention.days";

    public static final String SHARELIB_MAPPING_FILE = CONF_PREFIX + "ShareLibService.mapping.file";

    public static final String SHIP_LAUNCHER_JAR = "oozie.action.ship.launcher.jar";

    public static final String PURGE_INTERVAL = CONF_PREFIX + "ShareLibService.purge.interval";

    public static final String FAIL_FAST_ON_STARTUP = CONF_PREFIX + "ShareLibService.fail.fast.on.startup";

    private static final String PERMISSION_STRING = "-rwxr-xr-x";

    public static final String LAUNCHER_PREFIX = "launcher_";

    public static final String SHARED_LIB_PREFIX = "lib_";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private Services services;

    private Map<String, List<Path>> shareLibMap = new HashMap<String, List<Path>>();

    private Map<String, List<Path>> launcherLibMap = new HashMap<String, List<Path>>();

    //symlink mapping. Oozie keeps on checking symlink path and if changes, Oozie reloads the sharelib
    private Map<String, Map<Path, Path>> symlinkMapping = new HashMap<String, Map<Path, Path>>();

    private static XLog LOG = XLog.getLog(ShareLibService.class);

    private String sharelibMappingFile;

    private boolean isShipLauncherEnabled = false;

    public static String SHARE_LIB_CONF_PREFIX = "oozie";

    private boolean shareLibLoadAttempted = false;

    private String sharelibMetaFileOldTimeStamp;

    private String sharelibDirOld;

    FileSystem fs;

    final long retentionTime = 1000 * 60 * 60 * 24 * ConfigurationService.getInt(LAUNCHERJAR_LIB_RETENTION);

    @Override
    public void init(Services services) throws ServiceException {
        this.services = services;
        sharelibMappingFile = ConfigurationService.get(services.getConf(), SHARELIB_MAPPING_FILE);
        isShipLauncherEnabled = ConfigurationService.getBoolean(services.getConf(), SHIP_LAUNCHER_JAR);
        boolean failOnfailure = ConfigurationService.getBoolean(services.getConf(), FAIL_FAST_ON_STARTUP);
        Path launcherlibPath = getLauncherlibPath();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        URI uri = launcherlibPath.toUri();
        try {
            fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
            updateLauncherLib();
            updateShareLib();
        }
        catch (Throwable e) {
            if (failOnfailure) {
                LOG.error("Sharelib initialization fails", e);
                throw new ServiceException(ErrorCode.E0104, getClass().getName(), "Sharelib initialization fails. ", e);
            }
            else {
                // We don't want to actually fail init by throwing an Exception, so only create the ServiceException and
                // log it
                ServiceException se = new ServiceException(ErrorCode.E0104, getClass().getName(),
                        "Not able to cache sharelib. An Admin needs to install the sharelib with oozie-setup.sh and issue the "
                                + "'oozie admin' CLI command to update the sharelib", e);
                LOG.error(se);
            }
        }
        Runnable purgeLibsRunnable = new Runnable() {
            @Override
            public void run() {
                System.out.flush();
                try {
                    // Only one server should purge sharelib
                    if (Services.get().get(JobsConcurrencyService.class).isLeader()) {
                        final Date current = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
                        purgeLibs(fs, LAUNCHER_PREFIX, current);
                        purgeLibs(fs, SHARED_LIB_PREFIX, current);
                    }
                }
                catch (IOException e) {
                    LOG.error("There was an issue purging the sharelib", e);
                }
            }
        };
        services.get(SchedulerService.class).schedule(purgeLibsRunnable, 10,
                ConfigurationService.getInt(services.getConf(), PURGE_INTERVAL) * 60 * 60 * 24,
                SchedulerService.Unit.SEC);
    }

    /**
     * Recursively change permissions.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */

    private void updateLauncherLib() throws IOException {
        if (isShipLauncherEnabled) {
            if (fs == null) {
                Path launcherlibPath = getLauncherlibPath();
                HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
                URI uri = launcherlibPath.toUri();
                fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
            }
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
     * @param type is sharelib key
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

        try {
            if(fs.isFile(new Path(new URI(rootDir.toString()).getPath()))){
                listOfPaths.add(rootDir);
                return;
            }
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
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

    public Map<String, List<Path>> getShareLib() {
        return shareLibMap;
    }

    private Map<String, Map<Path, Path>> getSymlinkMapping() {
        return symlinkMapping;
    }

    /**
     * Gets the action sharelib lib jars.
     *
     * @param shareLibKey the sharelib key
     * @return List of paths
     * @throws IOException
     */
    public List<Path> getShareLibJars(String shareLibKey) throws IOException {
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
        checkSymlink(shareLibKey);
        return shareLibMap.get(shareLibKey);
    }

    private void checkSymlink(String shareLibKey) throws IOException {
        if (!HadoopShims.isSymlinkSupported() || symlinkMapping.get(shareLibKey) == null
                || symlinkMapping.get(shareLibKey).isEmpty()) {
            return;
        }

        HadoopShims fileSystem = new HadoopShims(fs);
        for (Path path : symlinkMapping.get(shareLibKey).keySet()) {
            if (!symlinkMapping.get(shareLibKey).get(path).equals(fileSystem.getSymLinkTarget(path))) {
                synchronized (ShareLibService.class) {
                    Map<String, List<Path>> tempShareLibMap = new HashMap<String, List<Path>>(shareLibMap);
                    Map<String, Map<Path, Path>> tmpSymlinkMapping = new HashMap<String, Map<Path, Path>>(symlinkMapping);

                    LOG.info(MessageFormat.format("Symlink target for [{0}] has changed, was [{1}], now [{2}]", shareLibKey,
                            path, fileSystem.getSymLinkTarget(path)));
                    loadShareLibMetaFile(tempShareLibMap, tmpSymlinkMapping, sharelibMappingFile, shareLibKey);
                    shareLibMap = tempShareLibMap;
                    symlinkMapping = tmpSymlinkMapping;
                    return;
                }

            }
        }

    }

    /**
     * Gets the launcher jars.
     *
     * @param shareLibKey the shareLib key
     * @return launcher jars paths
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public List<Path> getSystemLibJars(String shareLibKey) throws IOException {
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
            if (launcherLibMap.get(shareLibKey) != null) {
                returnList.addAll(launcherLibMap.get(shareLibKey));
            }
        }
        if (shareLibKey.equals(JavaActionExecutor.OOZIE_COMMON_LIBDIR)) {
            List<Path> sharelibList = getShareLibJars(shareLibKey);
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
     */
    private void purgeLibs(FileSystem fs, final String prefix, final Date current) throws IOException {
        Path executorLibBasePath = services.get(WorkflowAppService.class).getSystemLibPath();
        PathFilter directoryFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if (path.getName().startsWith(prefix)) {
                    String name = path.getName();
                    String time = name.substring(prefix.length());
                    Date d = null;
                    try {
                        d = dateFormat.parse(time);
                    }
                    catch (ParseException e) {
                        return false;
                    }
                    return (current.getTime() - d.getTime()) > retentionTime;
                }
                else {
                    return false;
                }
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

        // Logic is to keep all share-lib between current timestamp and 7days old + 1 latest sharelib older than 7 days.
        // refer OOZIE-1761
        for (int i = 1; i < dirList.length; i++) {
            Path dirPath = dirList[i].getPath();
            fs.delete(dirPath, true);
            LOG.info("Deleted old launcher jar lib directory {0}", dirPath.getName());
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
     * @return the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public Map<String, String> updateShareLib() throws IOException {
        Map<String, String> status = new HashMap<String, String>();

        if (fs == null) {
            Path launcherlibPath = getLauncherlibPath();
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            URI uri = launcherlibPath.toUri();
            fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
        }

        Map<String, List<Path>> tempShareLibMap = new HashMap<String, List<Path>>();
        Map<String, Map<Path, Path>> tmpSymlinkMapping = new HashMap<String, Map<Path, Path>>();

        if (!StringUtils.isEmpty(sharelibMappingFile.trim())) {
            String sharelibMetaFileNewTimeStamp = JsonUtils.formatDateRfc822(
                    new Date(fs.getFileStatus(new Path(sharelibMappingFile)).getModificationTime()), "GMT");
            loadShareLibMetaFile(tempShareLibMap, tmpSymlinkMapping, sharelibMappingFile, null);
            status.put("sharelibMetaFile", sharelibMappingFile);
            status.put("sharelibMetaFileNewTimeStamp", sharelibMetaFileNewTimeStamp);
            status.put("sharelibMetaFileOldTimeStamp", sharelibMetaFileOldTimeStamp);
            sharelibMetaFileOldTimeStamp = sharelibMetaFileNewTimeStamp;
        }
        else {
            Path shareLibpath = getLatestLibPath(services.get(WorkflowAppService.class).getSystemLibPath(),
                    SHARED_LIB_PREFIX);
            loadShareLibfromDFS(tempShareLibMap, shareLibpath);

            if (shareLibpath != null) {
                status.put("sharelibDirNew", shareLibpath.toString());
                status.put("sharelibDirOld", sharelibDirOld);
                sharelibDirOld = shareLibpath.toString();
            }

        }
        shareLibMap = tempShareLibMap;
        symlinkMapping = tmpSymlinkMapping;
        return status;
    }

    /**
     * Update share lib cache. Parse the share lib directory and each sub directory is a action key
     *
     * @param shareLibMap the share lib jar map
     * @param shareLibpath the share libpath
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void loadShareLibfromDFS(Map<String, List<Path>> shareLibMap, Path shareLibpath) throws IOException {

        if (shareLibpath == null) {
            LOG.info("No share lib directory found");
            return;

        }

        FileStatus[] dirList = fs.listStatus(shareLibpath);

        if (dirList == null) {
            return;
        }

        for (FileStatus dir : dirList) {
            if (!dir.isDir()) {
                continue;
            }
            List<Path> listOfPaths = new ArrayList<Path>();
            getPathRecursively(fs, dir.getPath(), listOfPaths);
            shareLibMap.put(dir.getPath().getName(), listOfPaths);
            LOG.info("Share lib for " + dir.getPath().getName() + ":" + listOfPaths);

        }

    }

    /**
     * Load share lib text file. Sharelib mapping files contains list of key=value. where key is the action key and
     * value is the DFS location of sharelib files.
     *
     * @param shareLibMap the share lib jar map
     * @param sharelipFileMapping the sharelip file mapping
     * @param symlinkMapping the symlink mapping
     * @parm shareLibKey the sharelib key
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void loadShareLibMetaFile(Map<String, List<Path>> shareLibMap,
            Map<String, Map<Path, Path>> symlinkMapping, String sharelibFileMapping, String shareLibKey)
            throws IOException {

        Path shareFileMappingPath = new Path(sharelibFileMapping);
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        FileSystem filesystem = FileSystem.get(has.createJobConf(shareFileMappingPath.toUri().getAuthority()));
        Properties prop = new Properties();
        prop.load(filesystem.open(new Path(sharelibFileMapping)));

        for (Object keyObject : prop.keySet()) {
            String key = (String) keyObject;
            String mapKey = key.substring(SHARE_LIB_CONF_PREFIX.length() + 1);
            if (key.toLowerCase().startsWith(SHARE_LIB_CONF_PREFIX)
                    && (shareLibKey == null || shareLibKey.equals(mapKey))) {
                loadSharelib(shareLibMap, symlinkMapping, mapKey, ((String) prop.get(key)).split(","));
            }
        }
    }

    private void loadSharelib(Map<String, List<Path>> tmpShareLibMap, Map<String, Map<Path, Path>> tmpSymlinkMapping,
            String shareLibKey, String pathList[]) throws IOException {
        List<Path> listOfPaths = new ArrayList<Path>();
        Map<Path, Path> symlinkMappingforAction = new HashMap<Path, Path>();
        HadoopShims fileSystem = new HadoopShims(fs);

        for (String dfsPath : pathList) {
            Path path = new Path(dfsPath);

            getPathRecursively(fs, path, listOfPaths);
            if (HadoopShims.isSymlinkSupported() && fileSystem.isSymlink(path)) {
                symlinkMappingforAction.put(path, fileSystem.getSymLinkTarget(path));
            }
        }
        if (HadoopShims.isSymlinkSupported()) {
            LOG.info("symlink for " + shareLibKey + ":" + symlinkMappingforAction);
            tmpSymlinkMapping.put(shareLibKey, symlinkMappingforAction);
        }
        tmpShareLibMap.put(shareLibKey, listOfPaths);
        LOG.info("Share lib for " + shareLibKey + ":" + listOfPaths);
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
        //If there are no timestamped directories, fall back to root directory
        if (path == null) {
            path = rootDir;
        }
        return path;
    }

    /**
     * Instruments the log service.
     * <p/>
     * It sets instrumentation variables indicating the location of the sharelib and launcherlib
     *
     * @param instr instrumentation to use.
     */
    @Override
    public void instrument(Instrumentation instr) {
        instr.addVariable("libs", "sharelib.source", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                if (!StringUtils.isEmpty(sharelibMappingFile.trim())) {
                    return SHARELIB_MAPPING_FILE;
                }
                return WorkflowAppService.SYSTEM_LIB_PATH;
            }
        });
        instr.addVariable("libs", "sharelib.mapping.file", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                if (!StringUtils.isEmpty(sharelibMappingFile.trim())) {
                    return sharelibMappingFile;
                }
                return "(none)";
            }
        });
        instr.addVariable("libs", "sharelib.system.libpath", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                String sharelibPath = "(unavailable)";
                try {
                    Path libPath = getLatestLibPath(services.get(WorkflowAppService.class).getSystemLibPath(),
                            SHARED_LIB_PREFIX);
                    if (libPath != null) {
                        sharelibPath = libPath.toUri().toString();
                    }
                }
                catch (IOException ioe) {
                    // ignore exception because we're just doing instrumentation
                }
                return sharelibPath;
            }
        });
        instr.addVariable("libs", "sharelib.mapping.file.timestamp", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                if (!StringUtils.isEmpty(sharelibMetaFileOldTimeStamp)) {
                    return sharelibMetaFileOldTimeStamp;
                }
                return "(none)";
            }
        });
        instr.addVariable("libs", "sharelib.keys", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                Map<String, List<Path>> shareLib = getShareLib();
                if (shareLib != null && !shareLib.isEmpty()) {
                    Set<String> keySet = shareLib.keySet();
                    return keySet.toString();
                }
                return "(unavailable)";
            }
        });
        instr.addVariable("libs", "launcherlib.system.libpath", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                return getLauncherlibPath().toUri().toString();
            }
        });
        instr.addVariable("libs", "sharelib.symlink.mapping", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                Map<String, Map<Path, Path>> shareLibSymlinkMapping = getSymlinkMapping();
                if (shareLibSymlinkMapping != null && !shareLibSymlinkMapping.isEmpty()
                        && shareLibSymlinkMapping.values() != null && !shareLibSymlinkMapping.values().isEmpty()) {
                    StringBuffer bf = new StringBuffer();
                    for (Entry<String, Map<Path, Path>> key : shareLibSymlinkMapping.entrySet())
                        if (key.getKey() != null && !key.getValue().isEmpty()) {
                            for (Path path : key.getValue().keySet()) {
                                bf.append(path).append("(").append(key).append(")").append("=>")
                                        .append(shareLibSymlinkMapping.get(key).get(path)).append(",");
                            }

                        }
                    return bf.toString();
                }
                return "(none)";
            }
        });

    }

    /**
     * Returns file system for shared libraries.
     * <p/>
     * If WorkflowAppService#getSystemLibPath doesn't have authority then a default one assumed
     *
     * @return file system for shared libraries
     */
    public FileSystem getFileSystem() {
        return fs;
    }
}
