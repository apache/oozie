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
package org.apache.oozie.coord;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

/**
 * This class implements the EL function for HCat datasets in coordinator
 */

public class HCatELFunctions {
    private static XLog LOG = XLog.getLog(HCatELFunctions.class);
    private static final Configuration EMPTY_CONF = new Configuration(true);

    enum EVENT_TYPE {
        input, output
    }

    /* Workflow Parameterization EL functions */

    /**
     * Return true if partitions exists or false if not.
     *
     * @param uri hcatalog partition uri.
     * @return <code>true</code> if the uri exists, <code>false</code> if it does not.
     * @throws Exception
     */
    public static boolean hcat_exists(String uri) throws Exception {
        URI hcatURI = new URI(uri);
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        URIHandler handler = uriService.getURIHandler(hcatURI);
        WorkflowJob workflow = DagELFunctions.getWorkflow();
        String user = workflow.getUser();
        return handler.exists(hcatURI, EMPTY_CONF, user);
    }

    /* Coord EL functions */

    /**
     * Echo the same EL function without evaluating anything
     *
     * @param dataInName
     * @return the same EL function
     */
    public static String ph1_coord_database_echo(String dataName, String type) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataName);
        return echoUnResolved("database", "'" + dataName + "', '" + type + "'");
    }

    public static String ph1_coord_table_echo(String dataName, String type) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataName);
        return echoUnResolved("table", "'" + dataName + "', '" + type + "'");
    }

    public static String ph1_coord_dataInPartitionPigFilter_echo(String dataInName) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataInName);
        return echoUnResolved("dataInPartitionPigFilter", "'" + dataInName + "'");
    }

    public static String ph1_coord_dataInPartitionMin_echo(String dataInName, String partition) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataInName);
        return echoUnResolved("dataInPartitionMin", "'" + dataInName + "', '" + partition + "'");
    }

    public static String ph1_coord_dataInPartitionMax_echo(String dataInName, String partition) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataInName);
        return echoUnResolved("dataInPartitionMax", "'" + dataInName + "', '" + partition + "'");
    }

    public static String ph1_coord_dataOutPartitions_echo(String dataOutName) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataOutName);
        return echoUnResolved("dataOutPartitions", "'" + dataOutName + "'");
    }

    public static String ph1_coord_dataOutPartitionValue_echo(String dataOutName, String partition) {
        // Checking if the dataIn/dataOut is correct?
        isValidDataEvent(dataOutName);
        return echoUnResolved("dataOutPartitionValue", "'" + dataOutName + "', '" + partition + "'");
    }

    /**
     * Extract the hcat DB name from the URI-template associate with
     * 'dataInName'. Caller needs to specify the EL-evaluator level variable
     * 'oozie.coord.el.dataset.bean' with synchronous dataset object
     * (SyncCoordDataset)
     *
     * @param dataInName
     * @return DB name
     */
    public static String ph3_coord_database(String dataName, String type) {
        HCatURI hcatURI = getURIFromResolved(dataName, type);
        if (hcatURI != null) {
            return hcatURI.getDb();
        }
        else {
            return "";
        }
    }

    /**
     * Extract the hcat Table name from the URI-template associate with
     * 'dataInName'. Caller needs to specify the EL-evaluator level variable
     * 'oozie.coord.el.dataset.bean' with synchronous dataset object
     * (SyncCoordDataset)
     *
     * @param dataInName
     * @return Table name
     */
    public static String ph3_coord_table(String dataName, String type) {
        HCatURI hcatURI = getURIFromResolved(dataName, type);
        if (hcatURI != null) {
            return hcatURI.getTable();
        }
        else {
            return "";
        }
    }

    /**
     * Used to specify the HCat partition filter which is input dependency for workflow job.<p/> Look for two evaluator-level
     * variables <p/> A) .datain.<DATAIN_NAME> B) .datain.<DATAIN_NAME>.unresolved <p/> A defines the current list of
     * HCat URIs. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the partition filter.
     *
     * @param dataInName : Datain name
     */
    public static String ph3_coord_dataInPartitionPigFilter(String dataInName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uris = (String) eval.getVariable(".datain." + dataInName);
        Boolean unresolved = (Boolean) eval.getVariable(".datain." + dataInName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataInPartitionPigFilter('" + dataInName + "')}";
        }
        return createPartitionFilter(uris);
    }

    /**
     * Used to specify the HCat partition's value defining output for workflow job.<p/> Look for two evaluator-level
     * variables <p/> A) .dataout.<DATAOUT_NAME> B) .dataout.<DATAOUT_NAME>.unresolved <p/> A defines the current list of
     * HCat URIs. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the partition value.
     *
     * @param dataOutName : Dataout name
     * @param partitionName : Specific partition name whose value is wanted
     */
    public static String ph3_coord_dataOutPartitionValue(String dataOutName, String partitionName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uri = (String) eval.getVariable(".dataout." + dataOutName);
        Boolean unresolved = (Boolean) eval.getVariable(".dataout." + dataOutName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataOutPartitionValue('" + dataOutName + "', '" + partitionName + "')}";
        }
        try {
            HCatURI hcatUri = new HCatURI(uri);
            return hcatUri.getPartitionValue(partitionName);
        }
        catch(URISyntaxException urie) {
            LOG.warn("Exception with uriTemplate [{0}]. Reason [{1}]: ", uri, urie);
            throw new RuntimeException("HCat URI can't be parsed " + urie);
        }
    }

    /**
     * Used to specify the entire HCat partition defining output for workflow job.<p/> Look for two evaluator-level
     * variables <p/> A) .dataout.<DATAOUT_NAME> B) .dataout.<DATAOUT_NAME>.unresolved <p/> A defines the data-out
     * HCat URI. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the partition.
     *
     * @param dataOutName : DataOut name
     */
    public static String ph3_coord_dataOutPartitions(String dataOutName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uri = (String) eval.getVariable(".dataout." + dataOutName);
        Boolean unresolved = (Boolean) eval.getVariable(".dataout." + dataOutName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataOutPartition('" + dataOutName + "')}";
        }
        try {
            return new HCatURI(uri).toPartitionStringHCatStorer();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Parsing exception for HCatURI " + uri + ". details: " + e);
        }
    }

    /**
     * Used to specify the MAXIMUM value of an HCat partition which is input dependency for workflow job.<p/> Look for two evaluator-level
     * variables <p/> A) .datain.<DATAIN_NAME> B) .datain.<DATAIN_NAME>.unresolved <p/> A defines the current list of
     * HCat URIs. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the max partition value.
     *
     * @param dataInName : Datain name
     * @param partitionName : Specific partition name whose MAX value is wanted
     */
    public static String ph3_coord_dataInPartitionMin(String dataInName, String partitionName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uris = (String) eval.getVariable(".datain." + dataInName);
        Boolean unresolved = (Boolean) eval.getVariable(".datain." + dataInName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataInPartitionMin('" + dataInName + "')}";
        }
        String minPartition = null;
        if (uris != null) {
            String[] uriList = uris.split(CoordELFunctions.DIR_SEPARATOR);
            // get the partition values list and find minimum
            try {
                // initialize minValue with first partition value
                minPartition = new HCatURI(uriList[0]).getPartitionValue(partitionName);
                if (minPartition == null || minPartition.isEmpty()) {
                    throw new RuntimeException("No value in data-in uri for partition key: " + partitionName);
                }
                for (int i = 1; i < uriList.length; i++) {
                        String value = new HCatURI(uriList[i]).getPartitionValue(partitionName);
                        if(value.compareTo(minPartition) < 0) { //sticking to string comparison since some numerical date
                                                                //values can also contain letters e.g. 20120101T0300Z (UTC)
                            minPartition = value;
                        }
                }
            }
            catch(URISyntaxException urie) {
                throw new RuntimeException("HCat URI can't be parsed " + urie);
            }
        }
        else {
            LOG.warn("URI is null");
            return null;
        }
        return minPartition;
    }

    /**
     * Used to specify the MINIMUM value of an HCat partition which is input dependency for workflow job.<p/> Look for two evaluator-level
     * variables <p/> A) .datain.<DATAIN_NAME> B) .datain.<DATAIN_NAME>.unresolved <p/> A defines the current list of
     * HCat URIs. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the min partition value.
     *
     * @param dataInName : Datain name
     * @param partitionName : Specific partition name whose MIN value is wanted
     */
    public static String ph3_coord_dataInPartitionMax(String dataInName, String partitionName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uris = (String) eval.getVariable(".datain." + dataInName);
        Boolean unresolved = (Boolean) eval.getVariable(".datain." + dataInName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataInPartitionMin('" + dataInName + "')}";
        }
        String maxPartition = null;
        if (uris != null) {
            String[] uriList = uris.split(CoordELFunctions.DIR_SEPARATOR);
            // get the partition values list and find minimum
            try {
                // initialize minValue with first partition value
                maxPartition = new HCatURI(uriList[0]).getPartitionValue(partitionName);
                if (maxPartition == null || maxPartition.isEmpty()) {
                    throw new RuntimeException("No value in data-in uri for partition key: " + partitionName);
                }
                for(int i = 1; i < uriList.length; i++) {
                        String value = new HCatURI(uriList[i]).getPartitionValue(partitionName);
                        if(value.compareTo(maxPartition) > 0) {
                            maxPartition = value;
                        }
                }
            }
            catch(URISyntaxException urie) {
                throw new RuntimeException("HCat URI can't be parsed " + urie);
            }
        }
        else {
            LOG.warn("URI is null");
            return null;
        }
        return maxPartition;
    }

    private static String createPartitionFilter(String uris) {
        String[] uriList = uris.split(CoordELFunctions.DIR_SEPARATOR);
        StringBuilder filter = new StringBuilder("");
        if (uriList.length > 0) {
            for (String uri : uriList) {
                if (filter.length() > 0) {
                    filter.append(" OR ");
                }
                try {
                    filter.append(new HCatURI(uri).toPigPartitionFilter());
                }
                catch (URISyntaxException e) {
                    throw new RuntimeException("Parsing exception for HCatURI " + uri + ". details: " + e);
                }
            }
        }
        return filter.toString();
    }

    private static HCatURI getURIFromResolved(String dataInName, String type) {
        StringBuilder uriTemplate = new StringBuilder();
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uris;
        if(type.equals(EVENT_TYPE.input.toString())) {
            uris = (String) eval.getVariable(".datain." + dataInName);
        }
        else { //type=output
            uris = (String) eval.getVariable(".dataout." + dataInName);
        }
        if (uris != null) {
            String[] uri = uris.split(CoordELFunctions.DIR_SEPARATOR, -1);
            uriTemplate.append(uri[0]);
        }
        else {
            LOG.warn("URI is NULL");
            return null;
        }
        LOG.info("uriTemplate [{0}] ", uriTemplate);
        HCatURI hcatURI;
        try {
            hcatURI = new HCatURI(uriTemplate.toString());
        }
        catch (URISyntaxException e) {
            LOG.info("uriTemplate [{0}]. Reason [{1}]: ", uriTemplate, e);
            throw new RuntimeException("HCat URI can't be parsed " + e);
        }
        return hcatURI;
    }

    private static boolean isValidDataEvent(String dataInName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String val = (String) eval.getVariable("oozie.dataname." + dataInName);
        if (val == null || (val.equals("data-in") == false && val.equals("data-out") == false)) {
            LOG.error("dataset name " + dataInName + " is not valid. val :" + val);
            throw new RuntimeException("data set name " + dataInName + " is not valid");
        }
        return true;
    }

    private static String echoUnResolved(String functionName, String n) {
        return echoUnResolvedPre(functionName, n, "coord:");
    }

    private static String echoUnResolvedPre(String functionName, String n, String prefix) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return prefix + functionName + "(" + n + ")"; // Unresolved
    }

}