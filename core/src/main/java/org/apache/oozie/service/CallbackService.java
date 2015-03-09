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

import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.hadoop.conf.Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.MessageFormat;

/**
 * Service that generates and parses callback URLs.
 */
public class CallbackService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CallbackService.";

    public static final String CONF_BASE_URL = CONF_PREFIX + "base.url";

    public static final String CONF_EARLY_REQUEUE_MAX_RETRIES = CONF_PREFIX + "early.requeue.max.retries";

    private Configuration oozieConf;
    private int earlyRequeueMaxRetries;

    /**
     * Initialize the service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
        oozieConf = services.getConf();
        earlyRequeueMaxRetries = ConfigurationService.getInt(CONF_EARLY_REQUEUE_MAX_RETRIES);
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface of the Dag engine service.
     *
     * @return {@link CallbackService}.
     */
    public Class<? extends Service> getInterface() {
        return CallbackService.class;
    }

    private static final String ID_PARAM = "id=";
    private static final String STATUS_PARAM = "status=";
    private static final String CALL_BACK_QUERY_STRING = "{0}?" + ID_PARAM + "{1}" + "&" + STATUS_PARAM + "{2}";

    /**
     * Create a callback URL.
     *
     * @param actionId action ID for the callback URL.
     * @param externalStatusVar variable for the caller to inject the external status.
     * @return the callback URL.
     */
    public String createCallBackUrl(String actionId, String externalStatusVar) {
        ParamChecker.notEmpty(actionId, "actionId");
        ParamChecker.notEmpty(externalStatusVar, "externalStatusVar");
        //TODO: figure out why double encoding is happening in case of hadoop callbacks.
        String baseCallbackUrl = ConfigurationService.get(oozieConf, CONF_BASE_URL);
        return MessageFormat.format(CALL_BACK_QUERY_STRING, baseCallbackUrl, actionId, externalStatusVar);
    }

    private String getParam(String str, String name) {
        String value = null;
        int start = str.indexOf(name);
        if (start > -1) {
            int end = str.indexOf("&", start + 1);
            start = start + name.length();
            value = (end > -1) ? str.substring(start, end) : str.substring(start);
        }
        return value;
    }

    /**
     * Return if a callback URL is valid or not.
     *
     * @param callback callback URL (it can be just the callback portion of it).
     * @return <code>true</code> if the callback URL is valid, <code>false</code> if it is not.
     */
    public boolean isValid(String callback) {
        return callback != null && getParam(callback, ID_PARAM) != null && getParam(callback, STATUS_PARAM) != null;
    }

    /**
     * Return the action ID from a callback URL.
     *
     * @param callback callback URL (it can be just the callback portion of it).
     * @return the action ID from a callback URL.
     */
    public String getActionId(String callback) {
        try {
            return URLDecoder.decode(getParam(ParamChecker.notEmpty(callback, "callback"), ID_PARAM), "UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Return the action external status from a callback URL.
     *
     * @param callback callback URL (it can be just the callback portion of it).
     * @return the action external status from a callback URL.
     */
    public String getExternalStatus(String callback) {
        try {
            return URLDecoder.decode(getParam(ParamChecker.notEmpty(callback, "callback"), STATUS_PARAM), "UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getEarlyRequeueMaxRetries() {
        return earlyRequeueMaxRetries;
    }
}
