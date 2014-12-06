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

package org.apache.oozie.client.rest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.BulkResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Server-side implementation class of the client interface BulkResponse
 * Declares all the bulk request specific user parameters and handling as JSON object
 */
public class BulkResponseImpl implements BulkResponse, JsonBean {
    private BundleJobBean bundle;
    private CoordinatorJobBean coordinator;
    private CoordinatorActionBean action;

    public static final String BULK_FILTER_BUNDLE = "bundle";
    public static final String BULK_FILTER_COORD = "coordinators";
    public static final String BULK_FILTER_LEVEL = "filterlevel";
    public static final String BULK_FILTER_STATUS = "actionstatus";
    public static final String BULK_FILTER_START_CREATED_EPOCH = "startcreatedtime";
    public static final String BULK_FILTER_END_CREATED_EPOCH = "endcreatedtime";
    public static final String BULK_FILTER_START_NOMINAL_EPOCH = "startscheduledtime";
    public static final String BULK_FILTER_END_NOMINAL_EPOCH = "endscheduledtime";
    public static final String BULK_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:SS'Z'";

    public static final Set<String> BULK_FILTER_NAMES = new HashSet<String>();

    static {

        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_BUNDLE);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_COORD);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_LEVEL);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_STATUS);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_START_CREATED_EPOCH);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_END_CREATED_EPOCH);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_START_NOMINAL_EPOCH);
        BULK_FILTER_NAMES.add(BulkResponseImpl.BULK_FILTER_END_NOMINAL_EPOCH);

    }

    /**
     * Construct JSON object using the bulk request object and the associated tags
     */
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    /**
     * Construct JSON object using the bulk request object and the associated tags
     */
    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();

        json.put(JsonTags.BULK_RESPONSE_BUNDLE, bundle.toJSONObject());
        json.put(JsonTags.BULK_RESPONSE_COORDINATOR, coordinator.toJSONObject());
        json.put(JsonTags.BULK_RESPONSE_ACTION, action.toJSONObject());

        return json;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BulkResponse#getBundle()
     */
    @Override
    public BundleJobBean getBundle() {
        return bundle;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BulkResponse#getCoordinator()
     */
    @Override
    public CoordinatorJobBean getCoordinator() {
        return coordinator;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BulkResponse#getAction()
     */
    @Override
    public CoordinatorActionBean getAction() {
        return action;
    }

    /**
     * Sets the bundle comprising this bulk response object
     * @param BundleJobBean
     */
    public void setBundle(BundleJobBean bj) {
        this.bundle = bj;
    }

    /**
     * Sets the coordinator comprising this bulk response object
     * @param CoordinatorJobBean
     */
    public void setCoordinator(CoordinatorJobBean cj) {
        this.coordinator = cj;
    }

    /**
     * Sets the coord action comprising this bulk response object
     * @param CoordinatorActionBean
     */
    public void setAction(CoordinatorActionBean ca) {
        this.action = ca;
    }

    /**
     * Convert a nodes list into a JSONArray.
     *
     * @param actions nodes list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends BulkResponseImpl> responses, String timeZoneId) {
        JSONArray array = new JSONArray();
        for (BulkResponseImpl response : responses) {
            array.add(response.toJSONObject(timeZoneId));
        }
        return array;
    }
}
