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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.util.XLog;

public class URIHandlerService implements Service {

    private static final String CONF_PREFIX = Service.CONF_PREFIX + "URIHandlerService.";
    public static final String URI_HANDLERS = CONF_PREFIX + "uri.handlers";
    public static final String URI_HANDLER_DEFAULT = CONF_PREFIX + "uri.handler.default";

    private static XLog LOG = XLog.getLog(URIHandlerService.class);
    private Configuration conf;
    private Map<String, URIHandler> cache;
    private URIHandler defaultHandler;

    @Override
    public void init(Services services) throws ServiceException {
        conf = services.getConf();
        cache = new HashMap<String, URIHandler>();
        String[] classes = conf.getStrings(URI_HANDLERS, FSURIHandler.class.getName());
        try {
            for (String classname : classes) {
                Class<?> clazz = Class.forName(classname);
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
            LOG.info("Loaded urihandlers {0}", (Object[])classes);
            LOG.info("Loaded default urihandler {0}", defaultHandler.getClass().getName());
        }
        catch (ClassNotFoundException e) {
            throw new ServiceException(ErrorCode.E0902, e);
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
    public URI stripPath(String uri) throws URIAccessorException {
        int index = uri.indexOf("://");
        if (index == -1) {
            throw new URIAccessorException(ErrorCode.E0905, uri);
        }
        int pathIndex = uri.indexOf("/", index + 4);
        try {
            if (pathIndex == -1) {
                return new URI(uri.substring(index + 3));
            }
            else {
                return new URI(uri.substring(index + 3, pathIndex));
            }
        }
        catch (URISyntaxException e) {
            throw new URIAccessorException(ErrorCode.E1025, uri, e);
        }
    }

}
