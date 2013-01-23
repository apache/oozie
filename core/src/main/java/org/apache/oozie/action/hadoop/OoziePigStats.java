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
package org.apache.oozie.action.hadoop;

import java.util.Map;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.json.simple.JSONObject;

/**
 * Class to collect the Pig statistics for a Pig action
 *
 */
public class OoziePigStats extends ActionStats {
    PigStats pigStats = null;

    public OoziePigStats(PigStats pigStats) {
        this.currentActionType = ActionType.PIG;
        this.pigStats = pigStats;
    }

    /**
     * The PigStats API is used to collect the statistics and the result is returned as a JSON String.
     *
     * @return a JSON string
     */
    @SuppressWarnings("unchecked")
    @Override
    public String toJSON() {
        JSONObject pigStatsGroup = new JSONObject();
        pigStatsGroup.put("ACTION_TYPE", getCurrentActionType().toString());

        // pig summary related counters
        pigStatsGroup.put("BYTES_WRITTEN", Long.toString(pigStats.getBytesWritten()));
        pigStatsGroup.put("DURATION", Long.toString(pigStats.getDuration()));
        pigStatsGroup.put("ERROR_CODE", Long.toString(pigStats.getErrorCode()));
        pigStatsGroup.put("ERROR_MESSAGE", pigStats.getErrorMessage());
        pigStatsGroup.put("FEATURES", pigStats.getFeatures());
        pigStatsGroup.put("HADOOP_VERSION", pigStats.getHadoopVersion());
        pigStatsGroup.put("NUMBER_JOBS", Long.toString(pigStats.getNumberJobs()));
        pigStatsGroup.put("PIG_VERSION", pigStats.getPigVersion());
        pigStatsGroup.put("PROACTIVE_SPILL_COUNT_OBJECTS", Long.toString(pigStats.getProactiveSpillCountObjects()));
        pigStatsGroup.put("PROACTIVE_SPILL_COUNT_RECORDS", Long.toString(pigStats.getProactiveSpillCountRecords()));
        pigStatsGroup.put("RECORD_WRITTEN", Long.toString(pigStats.getRecordWritten()));
        pigStatsGroup.put("RETURN_CODE", Long.toString(pigStats.getReturnCode()));
        pigStatsGroup.put("SCRIPT_ID", pigStats.getScriptId());
        pigStatsGroup.put("SMM_SPILL_COUNT", Long.toString(pigStats.getSMMSpillCount()));

        PigStats.JobGraph jobGraph = pigStats.getJobGraph();
        StringBuffer sb = new StringBuffer();
        String separator = ",";

        for (JobStats jobStats : jobGraph) {
            // Get all the HadoopIds and put them as comma separated string for JOB_GRAPH
            String hadoopId = jobStats.getJobId();
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(hadoopId);
            // Hadoop Counters for pig created MR job
            pigStatsGroup.put(hadoopId, toJSONFromJobStats(jobStats));
        }
        pigStatsGroup.put("JOB_GRAPH", sb.toString());
        return pigStatsGroup.toJSONString();
    }

    // MR job related counters
    @SuppressWarnings("unchecked")
    private static JSONObject toJSONFromJobStats(JobStats jobStats) {
        JSONObject jobStatsGroup = new JSONObject();

        // hadoop counters
        jobStatsGroup.put(PigStatsUtil.HDFS_BYTES_WRITTEN, Long.toString(jobStats.getHdfsBytesWritten()));
        jobStatsGroup.put(PigStatsUtil.MAP_INPUT_RECORDS, Long.toString(jobStats.getMapInputRecords()));
        jobStatsGroup.put(PigStatsUtil.MAP_OUTPUT_RECORDS, Long.toString(jobStats.getMapOutputRecords()));
        jobStatsGroup.put(PigStatsUtil.REDUCE_INPUT_RECORDS, Long.toString(jobStats.getReduceInputRecords()));
        jobStatsGroup.put(PigStatsUtil.REDUCE_OUTPUT_RECORDS, Long.toString(jobStats.getReduceOutputRecords()));
        // currently returns null; pig bug
        jobStatsGroup.put("HADOOP_COUNTERS", toJSONFromCounters(jobStats.getHadoopCounters()));

        // pig generated hadoop counters and other stats
        jobStatsGroup.put("Alias", jobStats.getAlias());
        jobStatsGroup.put("AVG_MAP_TIME", Long.toString(jobStats.getAvgMapTime()));
        jobStatsGroup.put("AVG_REDUCE_TIME", Long.toString(jobStats.getAvgREduceTime()));
        jobStatsGroup.put("BYTES_WRITTEN", Long.toString(jobStats.getBytesWritten()));
        jobStatsGroup.put("ERROR_MESSAGE", jobStats.getErrorMessage());
        jobStatsGroup.put("FEATURE", jobStats.getFeature());
        jobStatsGroup.put("JOB_ID", jobStats.getJobId());
        jobStatsGroup.put("MAX_MAP_TIME", Long.toString(jobStats.getMaxMapTime()));
        jobStatsGroup.put("MIN_MAP_TIME", Long.toString(jobStats.getMinMapTime()));
        jobStatsGroup.put("MAX_REDUCE_TIME", Long.toString(jobStats.getMaxReduceTime()));
        jobStatsGroup.put("MIN_REDUCE_TIME", Long.toString(jobStats.getMinReduceTime()));
        jobStatsGroup.put("NUMBER_MAPS", Long.toString(jobStats.getNumberMaps()));
        jobStatsGroup.put("NUMBER_REDUCES", Long.toString(jobStats.getNumberReduces()));
        jobStatsGroup.put("PROACTIVE_SPILL_COUNT_OBJECTS", Long.toString(jobStats.getProactiveSpillCountObjects()));
        jobStatsGroup.put("PROACTIVE_SPILL_COUNT_RECS", Long.toString(jobStats.getProactiveSpillCountRecs()));
        jobStatsGroup.put("RECORD_WRITTEN", Long.toString(jobStats.getRecordWrittern()));
        jobStatsGroup.put("SMMS_SPILL_COUNT", Long.toString(jobStats.getSMMSpillCount()));
        jobStatsGroup.put("MULTI_STORE_COUNTERS", toJSONFromMultiStoreCounters(jobStats.getMultiStoreCounters()));

        return jobStatsGroup;

    }

    // multistorecounters to JSON
    @SuppressWarnings("unchecked")
    private static JSONObject toJSONFromMultiStoreCounters(Map<String, Long> map) {
        JSONObject group = new JSONObject();
        for (String cName : map.keySet()) {
            group.put(cName, map.get(cName));
        }
        return group;

    }

    // hadoop counters to JSON
    @SuppressWarnings("unchecked")
    private static JSONObject toJSONFromCounters(Counters counters) {
        if (counters == null) {
            return null;
        }

        JSONObject groups = new JSONObject();
        for (String gName : counters.getGroupNames()) {
            JSONObject group = new JSONObject();
            for (Counter counter : counters.getGroup(gName)) {
                String cName = counter.getName();
                Long cValue = counter.getValue();
                group.put(cName, Long.toString(cValue));
            }
            groups.put(gName, group);
        }
        return groups;

    }

}
