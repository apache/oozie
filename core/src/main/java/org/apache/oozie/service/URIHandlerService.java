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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.URIContext;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class URIHandlerService implements Service {

    private static final String CONF_PREFIX = Service.CONF_PREFIX + "URIHandlerService.";
    public static final String URI_HANDLERS = CONF_PREFIX + "uri.handlers";
    public static final String URI_HANDLER_DEFAULT = CONF_PREFIX + "uri.handler.default";
    public static final String URI_HANDLER_SUPPORTED_SCHEMES_PREFIX = CONF_PREFIX + "uri.handler.";
    public static final String URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX = ".supported.schemes";

    private static XLog LOG = XLog.getLog(URIHandlerService.class);
    private Configuration conf;
    private Configuration backendConf;
    private Map<String, URIHandler> cache;
    private Set<Class<?>> classesToShip;
    private URIHandler defaultHandler;

    @Override
    public void init(Services services) throws ServiceException {
        try {
            init(services.getConf(), true);
        }
        catch (Exception e) {
            throw new ServiceException(ErrorCode.E0902, e);
        }
    }

    public void init(Configuration conf, boolean isFrontEnd) throws ClassNotFoundException {
        this.conf = conf;
        cache = new HashMap<String, URIHandler>();

        String[] classes = conf.getStrings(URI_HANDLERS, FSURIHandler.class.getName());
        for (String classname : classes) {
            Class<?> clazz = Class.forName(classname);
            URIHandler uriHandler = (URIHandler) ReflectionUtils.newInstance(clazz, null);
            uriHandler.init(conf, isFrontEnd);
            for (String scheme : uriHandler.getSupportedSchemes()) {
                cache.put(scheme, uriHandler);
            }
        }

        Class<?> defaultClass = conf.getClass(URI_HANDLER_DEFAULT, null);
        defaultHandler = (defaultClass == null) ? new FSURIHandler() : (URIHandler) ReflectionUtils.newInstance(
                defaultClass, null);
        defaultHandler.init(conf, isFrontEnd);
        for (String scheme : defaultHandler.getSupportedSchemes()) {
            cache.put(scheme, defaultHandler);
        }

        if (isFrontEnd) {
            initClassesToShip();
            initURIServiceBackendConf();
        }

        LOG.info("Loaded urihandlers {0}", Arrays.toString(classes));
        LOG.info("Loaded default urihandler {0}", defaultHandler.getClass().getName());
    }

    /**
     * Initialize classes that need to be shipped for using URIHandlerService in the launcher job
     */
    private void initClassesToShip(){
        classesToShip = new HashSet<Class<?>>();
        classesToShip.add(Service.class);
        classesToShip.add(ServiceException.class);
        classesToShip.add(URIHandlerService.class);
        classesToShip.add(URIHandler.class);
        classesToShip.add(URIContext.class);
        classesToShip.add(ErrorCode.class);
        classesToShip.add(XException.class);
        classesToShip.add(URIAccessorException.class);
        // XLog, XLog$Level, XLog$Info, XLog$Info$InfoThreadLocal. Could not find a way to
        // get anonymous inner classes. So created InfoThreadLocal class.
        classesToShip.addAll(getDeclaredClasses(XLog.class));
        classesToShip.add(ParamChecker.class);

        for (URIHandler handler : cache.values()) {
            classesToShip.add(handler.getClass());
            classesToShip.addAll(handler.getClassesToShip());
        }
    }

    /**
     * Initialize configuration required for using URIHandlerService in the launcher job
     */
    private void initURIServiceBackendConf() {
        backendConf = new Configuration(false);
        String handlersConf = conf.get(URI_HANDLERS);
        if (handlersConf != null) {
            backendConf.set(URI_HANDLERS, handlersConf);
        }
        String defaultHandlerConf = conf.get(URI_HANDLER_DEFAULT);
        if (defaultHandlerConf != null) {
            backendConf.set(URI_HANDLER_DEFAULT, defaultHandlerConf);
        }

        for (URIHandler handler : cache.values()) {
            String schemeConf = URI_HANDLER_SUPPORTED_SCHEMES_PREFIX + handler.getClass().getSimpleName()
                    + URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX;
            backendConf.set(schemeConf, collectionToString(handler.getSupportedSchemes()));
        }
    }

    private String collectionToString(Collection<String> strs) {
        if (strs.size() == 0) {
            return "";
        }
        StringBuilder sbuf = new StringBuilder();
        for (String str : strs) {
            sbuf.append(str);
            sbuf.append(",");
        }
        sbuf.setLength(sbuf.length() - 1);
        return sbuf.toString();
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
    public Set<Class<?>> getURIHandlerClassesToShip() {
        return classesToShip;
    }

    /**
     * Return the configuration required to instantiate URIHandlerService in the launcher
     * @return configuration
     */
    public Configuration getURIHandlerServiceConfig() {
        return backendConf;
    }

    public URIHandler getURIHandler(String uri) throws URIAccessorException {
        try {
            return getURIHandler(new URI(uri));
        }
        catch (URISyntaxException e) {
            throw new URIAccessorException(ErrorCode.E0902, e);
        }
    }

    public URIHandler getURIHandler(URI uri) throws URIAccessorException {
        return getURIHandler(uri, false);
    }

    public URIHandler getURIHandler(URI uri, boolean validateURI) throws URIAccessorException {
        if (uri.getScheme() == null) {
            if (validateURI) {
                throw new URIAccessorException(ErrorCode.E0905, uri);
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
                    throw new URIAccessorException(ErrorCode.E0904, uri.getScheme(), uri.toString());
                }
            }
            return handler;
        }
    }

    /**
     * Get the URI with scheme://host:port removing the path
     * @param uri
     * @return
     * @throws URIAccessorException
     */
    public URI getAuthorityWithScheme(String uri) throws URIAccessorException {
        int index = uri.indexOf("://");
        if (index == -1) {
            throw new URIAccessorException(ErrorCode.E0905, uri);
        }
        try {
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
            throw new URIAccessorException(ErrorCode.E1025, uri, e);
        }
    }

    private Collection<Class<?>> getDeclaredClasses(Class<?> clazz) {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        Stack<Class<?>> stack = new Stack<Class<?>>();
        stack.push(clazz);
        do {
          clazz = stack.pop();
          classes.add(clazz);
          for (Class<?> dclazz : clazz.getDeclaredClasses()) {
              stack.push(dclazz);
          }
        } while (!stack.isEmpty());
        return classes;
    }

}
