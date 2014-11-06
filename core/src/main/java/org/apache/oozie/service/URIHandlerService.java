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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.action.hadoop.LauncherURIHandlerFactory;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.util.XLog;

public class URIHandlerService implements Service {

    private static final String CONF_PREFIX = Service.CONF_PREFIX + "URIHandlerService.";
    public static final String URI_HANDLERS = CONF_PREFIX + "uri.handlers";
    public static final String URI_HANDLER_DEFAULT = CONF_PREFIX + "uri.handler.default";
    public static final String URI_HANDLER_SUPPORTED_SCHEMES_PREFIX = CONF_PREFIX + "uri.handler.";
    public static final String URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX = ".supported.schemes";

    private static XLog LOG = XLog.getLog(URIHandlerService.class);
    private Configuration launcherConf;
    private Set<Class<?>> launcherClassesToShip;
    private Map<String, URIHandler> cache;
    private URIHandler defaultHandler;

    @Override
    public void init(Services services) throws ServiceException {
        try {
            init(services.getConf());
        }
        catch (Exception e) {
            throw new ServiceException(ErrorCode.E0902, e);
        }
    }

    private void init(Configuration conf) throws ClassNotFoundException {
        cache = new HashMap<String, URIHandler>();

        String[] classes = ConfigurationService.getStrings(conf, URI_HANDLERS);
        for (String classname : classes) {
            Class<?> clazz = Class.forName(classname.trim());
            URIHandler uriHandler = (URIHandler) ReflectionUtils.newInstance(clazz, null);
            uriHandler.init(conf);
            for (String scheme : uriHandler.getSupportedSchemes()) {
                cache.put(scheme, uriHandler);
            }
        }

        Class<?> defaultClass = conf.getClass(URI_HANDLER_DEFAULT, null);
        defaultHandler = (defaultClass == null) ? new FSURIHandler() : (URIHandler) ReflectionUtils.newInstance(
                defaultClass, null);
        defaultHandler.init(conf);
        for (String scheme : defaultHandler.getSupportedSchemes()) {
            cache.put(scheme, defaultHandler);
        }

        initLauncherClassesToShip();
        initLauncherURIHandlerConf();

        LOG.info("Loaded urihandlers {0}", Arrays.toString(classes));
        LOG.info("Loaded default urihandler {0}", defaultHandler.getClass().getName());
    }

    /**
     * Initialize classes that need to be shipped for using LauncherURIHandler in the launcher job
     */
    private void initLauncherClassesToShip(){
        launcherClassesToShip = new HashSet<Class<?>>();
        launcherClassesToShip.add(LauncherURIHandlerFactory.class);
        launcherClassesToShip.add(LauncherURIHandler.class);
        for (URIHandler handler : cache.values()) {
            launcherClassesToShip.add(handler.getLauncherURIHandlerClass());
            List<Class<?>> classes = handler.getClassesForLauncher();
            if (classes != null) {
                launcherClassesToShip.addAll(classes);
            }
        }
        launcherClassesToShip.add(defaultHandler.getLauncherURIHandlerClass());
    }

    /**
     * Initialize configuration required for using LauncherURIHandler in the launcher job
     */
    private void initLauncherURIHandlerConf() {
        launcherConf = new Configuration(false);

        for (URIHandler handler : cache.values()) {
            for (String scheme : handler.getSupportedSchemes()) {
                String schemeConf = LauncherURIHandlerFactory.CONF_LAUNCHER_URIHANDLER_SCHEME_PREFIX + scheme;
                launcherConf.set(schemeConf, handler.getLauncherURIHandlerClass().getName());
            }
        }

        for (String scheme : defaultHandler.getSupportedSchemes()) {
            String schemeConf = LauncherURIHandlerFactory.CONF_LAUNCHER_URIHANDLER_SCHEME_PREFIX + scheme;
            launcherConf.set(schemeConf, defaultHandler.getLauncherURIHandlerClass().getName());
        }
    }

    @Override
    public void destroy() {
        Set<URIHandler> handlers = new HashSet<URIHandler>();
        handlers.addAll(cache.values());
        for (URIHandler handler : handlers) {
            handler.destroy();
        }
        cache.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return URIHandlerService.class;
    }

    /**
     * Return the classes to be shipped to the launcher
     * @return the set of classes to be shipped to the launcher
     */
    public Set<Class<?>> getClassesForLauncher() {
        return launcherClassesToShip;
    }

    /**
     * Return the configuration required to use LauncherURIHandler in the launcher
     * @return configuration
     */
    public Configuration getLauncherConfig() {
        return launcherConf;
    }

    public URIHandler getURIHandler(String uri) throws URIHandlerException {
        try {
            return getURIHandler(new URI(uri));
        }
        catch (URISyntaxException e) {
            throw new URIHandlerException(ErrorCode.E0902, e);
        }
    }

    public URIHandler getURIHandler(URI uri) throws URIHandlerException {
        return getURIHandler(uri, false);
    }

    public URIHandler getURIHandler(URI uri, boolean validateURI) throws URIHandlerException {
        if (uri.getScheme() == null) {
            if (validateURI) {
                throw new URIHandlerException(ErrorCode.E0905, uri);
            }
            else {
                return defaultHandler;
            }
        }
        else {
            URIHandler handler = cache.get(uri.getScheme());
            if (handler == null) {
                handler = cache.get("*");
                if (handler == null) {
                    throw new URIHandlerException(ErrorCode.E0904, uri.getScheme(), uri.toString());
                }
            }
            return handler;
        }
    }

    /**
     * Get the URI with scheme://host:port removing the path
     * @param uri uri template
     * @return URI with authority and scheme
     * @throws URIHandlerException
     */
    public URI getAuthorityWithScheme(String uri) throws URIHandlerException {
        int index = uri.indexOf("://");
        try {
            if (index == -1) {
                LOG.trace("Relative path for uri-template "+uri);
                return new URI("/");
            }
            if (uri.indexOf(":///") != -1) {
                return new URI(uri.substring(0, index + 4));
            }
            int pathIndex = uri.indexOf("/", index + 4);
            if (pathIndex == -1) {
                return new URI(uri.substring(0));
            }
            else {
                return new URI(uri.substring(0, pathIndex));
            }
        }
        catch (URISyntaxException e) {
            throw new URIHandlerException(ErrorCode.E0906, uri, e);
        }
    }

}
