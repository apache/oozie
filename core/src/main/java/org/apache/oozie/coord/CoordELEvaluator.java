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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

/**
 * This class provide different evaluators required at different stages
 */
public class CoordELEvaluator {
    public static final Integer MINUTE = 1;
    public static final Integer HOUR = 60 * MINUTE;

    /**
     * Create an evaluator to be used in resolving configuration vars and frequency constant/functions (used in Stage
     * 1)
     *
     * @param conf : Configuration containing property variables
     * @return configured ELEvaluator
     */
    public static ELEvaluator createELEvaluatorForGroup(Configuration conf, String group) {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(group);
        setConfigToEval(eval, conf);
        return eval;
    }

    /**
     * Create a new Evaluator to resolve the EL functions and variables using action creation time (Phase 2)
     *
     * @param event : Xml element for data-in element usually enclosed by <data-in(out)> tag
     * @param appInst : Application Instance related information such as Action creation Time
     * @param conf :Configuration to substitute any variables
     * @return configured ELEvaluator
     * @throws Exception : If there is any date-time string in wrong format, the exception is thrown
     */
    public static ELEvaluator createInstancesELEvaluator(Element event, SyncCoordAction appInst, Configuration conf)
            throws Exception {
        return createInstancesELEvaluator("coord-action-create", event, appInst, conf);
    }

    public static ELEvaluator createInstancesELEvaluator(String tag, Element event, SyncCoordAction appInst,
                                                         Configuration conf) throws Exception {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(tag);
        setConfigToEval(eval, conf);
        SyncCoordDataset ds = getDSObject(event);
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        return eval;
    }

    public static ELEvaluator createELEvaluatorForDataEcho(Configuration conf, String group,
                                                           HashMap<String, String> dataNameList) throws Exception {
        ELEvaluator eval = createELEvaluatorForGroup(conf, group);
        for (Iterator<String> it = dataNameList.keySet().iterator(); it.hasNext();) {
            String key = it.next();
            String value = dataNameList.get(key);
            eval.setVariable("oozie.dataname." + key, value);
        }
        return eval;
    }

    /**
     * Create a new evaluator for Lazy resolve (phase 3). For example, coord_latest(n) and coord_actualTime()function
     * should be resolved when all other data dependencies are met.
     *
     * @param actualTime : Action start time
     * @param nominalTime : Action creation time
     * @param dEvent :XML element for data-in element usually enclosed by <data-in(out)> tag
     * @param conf :Configuration to substitute any variables
     * @return configured ELEvaluator
     * @throws Exception : If there is any date-time string in wrong format, the exception is thrown
     */
    public static ELEvaluator createLazyEvaluator(Date actualTime, Date nominalTime, Element dEvent, Configuration conf)
            throws Exception {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-start");
        setConfigToEval(eval, conf);
        SyncCoordDataset ds = getDSObject(dEvent);
        SyncCoordAction appInst = new SyncCoordAction();
        appInst.setNominalTime(nominalTime);
        appInst.setActualTime(actualTime);
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        eval.setVariable(CoordELFunctions.CONFIGURATION, conf);
        return eval;
    }

    /**
     * Create a SLA evaluator to be used during Materialization
     *
     * @param nominalTime
     * @param conf
     * @return
     * @throws Exception
     */
    public static ELEvaluator createSLAEvaluator(Date nominalTime, Configuration conf) throws Exception {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-sla-create");
        setConfigToEval(eval, conf);
        SyncCoordAction appInst = new SyncCoordAction();// TODO:
        appInst.setNominalTime(nominalTime);
        CoordELFunctions.configureEvaluator(eval, null, appInst);
        return eval;
    }

    /**
     * Create an Evaluator to resolve dataIns and dataOuts of an application instance (used in stage 3)
     *
     * @param eJob : XML element for the application instance
     * @param conf :Configuration to substitute any variables
     * @return configured ELEvaluator
     * @throws Exception : If there is any date-time string in wrong format, the exception is thrown
     */
    public static ELEvaluator createDataEvaluator(Element eJob, Configuration conf, String actionId) throws Exception {
        ELEvaluator e = Services.get().get(ELService.class).createEvaluator("coord-action-start");
        setConfigToEval(e, conf);
        SyncCoordAction appInst = new SyncCoordAction();
        String strNominalTime = eJob.getAttributeValue("action-nominal-time");
        if (strNominalTime != null) {
            appInst.setNominalTime(DateUtils.parseDateUTC(strNominalTime));
            appInst.setActionId(actionId);
            appInst.setName(eJob.getAttributeValue("name"));
        }
        String strActualTime = eJob.getAttributeValue("action-actual-time");
        if (strActualTime != null) {
            appInst.setActualTime(DateUtils.parseDateUTC(strActualTime));
        }
        CoordELFunctions.configureEvaluator(e, null, appInst);
        Element events = eJob.getChild("input-events", eJob.getNamespace());
        if (events != null) {
            for (Element data : (List<Element>) events.getChildren("data-in", eJob.getNamespace())) {
                if (data.getChild("uris", data.getNamespace()) != null) {
                    String uris = data.getChild("uris", data.getNamespace()).getTextTrim();
                    uris = uris.replaceAll(CoordELFunctions.INSTANCE_SEPARATOR, CoordELFunctions.DIR_SEPARATOR);
                    e.setVariable(".datain." + data.getAttributeValue("name"), uris);
                }
                else {
                }
                if (data.getChild("unresolved-instances", data.getNamespace()) != null) {
                    e.setVariable(".datain." + data.getAttributeValue("name") + ".unresolved", "true"); // TODO:
                    // check
                    // null
                }
            }
        }
        events = eJob.getChild("output-events", eJob.getNamespace());
        if (events != null) {
            for (Element data : (List<Element>) events.getChildren("data-out", eJob.getNamespace())) {
                if (data.getChild("uris", data.getNamespace()) != null) {
                    String uris = data.getChild("uris", data.getNamespace()).getTextTrim();
                    uris = uris.replaceAll(CoordELFunctions.INSTANCE_SEPARATOR, CoordELFunctions.DIR_SEPARATOR);
                    e.setVariable(".dataout." + data.getAttributeValue("name"), uris);
                }
                else {
                }// TODO
                if (data.getChild("unresolved-instances", data.getNamespace()) != null) {
                    e.setVariable(".dataout." + data.getAttributeValue("name") + ".unresolved", "true"); // TODO:
                    // check
                    // null
                }
            }
        }
        return e;
    }

    /**
     * Create a new Evaluator to resolve URI temple with time specific constant
     *
     * @param strDate : Date-time
     * @return configured ELEvaluator
     * @throws Exception If there is any date-time string in wrong format, the exception is thrown
     */
    public static ELEvaluator createURIELEvaluator(String strDate) throws Exception {
        ELEvaluator eval = new ELEvaluator();
        Calendar date = Calendar.getInstance(TimeZone.getTimeZone("UTC")); // TODO:UTC
        // always???
        date.setTime(DateUtils.parseDateUTC(strDate));
        eval.setVariable("YEAR", date.get(Calendar.YEAR));
        eval.setVariable("MONTH", make2Digits(date.get(Calendar.MONTH) + 1));
        eval.setVariable("DAY", make2Digits(date.get(Calendar.DAY_OF_MONTH)));
        eval.setVariable("HOUR", make2Digits(date.get(Calendar.HOUR_OF_DAY)));
        eval.setVariable("MINUTE", make2Digits(date.get(Calendar.MINUTE)));
        return eval;
    }

    /**
     * Create Dataset object using the Dataset XML information
     *
     * @param eData
     * @return
     * @throws Exception
     */
    private static SyncCoordDataset getDSObject(Element eData) throws Exception {
        SyncCoordDataset ds = new SyncCoordDataset();
        Element eDataset = eData.getChild("dataset", eData.getNamespace());
        // System.out.println("eDATA :"+ XmlUtils.prettyPrint(eData));
        Date initInstance = DateUtils.parseDateUTC(eDataset.getAttributeValue("initial-instance"));
        ds.setInitInstance(initInstance);
        if (eDataset.getAttributeValue("frequency") != null) {
            int frequency = Integer.parseInt(eDataset.getAttributeValue("frequency"));
            ds.setFrequency(frequency);
            ds.setType("SYNC");
            if (eDataset.getAttributeValue("freq_timeunit") == null) {
                throw new RuntimeException("No freq_timeunit defined in data set definition\n"
                        + XmlUtils.prettyPrint(eDataset));
            }
            ds.setTimeUnit(TimeUnit.valueOf(eDataset.getAttributeValue("freq_timeunit")));
            if (eDataset.getAttributeValue("timezone") == null) {
                throw new RuntimeException("No timezone defined in data set definition\n"
                        + XmlUtils.prettyPrint(eDataset));
            }
            ds.setTimeZone(DateUtils.getTimeZone(eDataset.getAttributeValue("timezone")));
            if (eDataset.getAttributeValue("end_of_duration") == null) {
                throw new RuntimeException("No end_of_duration defined in data set definition\n"
                        + XmlUtils.prettyPrint(eDataset));
            }
            ds.setEndOfDuration(TimeUnit.valueOf(eDataset.getAttributeValue("end_of_duration")));

            Element doneFlagElement = eDataset.getChild("done-flag", eData.getNamespace());
            String doneFlag = CoordUtils.getDoneFlag(doneFlagElement);
            ds.setDoneFlag(doneFlag);
        }
        else {
            ds.setType("ASYNC");
        }
        String name = eDataset.getAttributeValue("name");
        ds.setName(name);
        // System.out.println(name + " VAL "+ eDataset.getChild("uri-template",
        // eData.getNamespace()));
        String uriTemplate = eDataset.getChild("uri-template", eData.getNamespace()).getTextTrim();
        ds.setUriTemplate(uriTemplate);
        // ds.setTimeUnit(TimeUnit.MINUTES);
        return ds;
    }

    /**
     * Set all job configurations properties into evaluator.
     *
     * @param eval : Evaluator to set variables
     * @param conf : configurations to set Evaluator
     */
    private static void setConfigToEval(ELEvaluator eval, Configuration conf) {
        for (Map.Entry<String, String> entry : conf) {
            eval.setVariable(entry.getKey(), entry.getValue().trim());
        }
    }

    /**
     * make any one digit number to two digit string pre-appending a"0"
     *
     * @param num : number to make sting
     * @return :String of length at least two digit.
     */
    private static String make2Digits(int num) {
        String ret = "" + num;
        if (num <= 9) {
            ret = "0" + ret;
        }
        return ret;
    }
}
