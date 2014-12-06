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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Daemon;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

/**
 * This class sets up a simple thread that sleeps for a short period of time. If the sleep takes significantly longer than its
 * target time, it implies that the JVM or host machine has paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message and updates the Instrumentation.
 *
 * Adapted from org.apache.hadoop.util.JvmPauseMonitor
 */
public class JvmPauseMonitorService implements Service {

    private static XLog LOG = XLog.getLog(JvmPauseMonitorService.class);

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JvmPauseMonitorService.";

    /**
     * The target sleep time
     */
    private static final long SLEEP_INTERVAL_MS = 500;

    /**
     * log WARN if we detect a pause longer than this threshold
     */
    private long warnThresholdMs;
    public static final String WARN_THRESHOLD_KEY = CONF_PREFIX + "warn-threshold.ms";

    /**
     * log INFO if we detect a pause longer than this threshold
     */
    private long infoThresholdMs;
    public static final String INFO_THRESHOLD_KEY = CONF_PREFIX + "info-threshold.ms";

    private Thread monitorThread;
    private volatile boolean shouldRun = true;
    private Instrumentation instrumentation;

    @Override
    public void init(Services services) throws ServiceException {
        warnThresholdMs = ConfigurationService.getLong(services.getConf(), WARN_THRESHOLD_KEY);
        infoThresholdMs = ConfigurationService.getLong(services.getConf(), INFO_THRESHOLD_KEY);

        instrumentation = services.get(InstrumentationService.class).get();

        Preconditions.checkState(monitorThread == null,
                "Already started");
        monitorThread = new Daemon(new Monitor());
        monitorThread.start();
    }

    @Override
    public void destroy() {
        shouldRun = false;
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Class<? extends Service> getInterface() {
        return JvmPauseMonitorService.class;
    }

    private String formatMessage(long extraSleepTime, Map<String, GcTimes> gcTimesAfterSleep,
            Map<String, GcTimes> gcTimesBeforeSleep) {
        Set<String> gcBeanNames = Sets.intersection(gcTimesAfterSleep.keySet(), gcTimesBeforeSleep.keySet());
        List<String> gcDiffs = Lists.newArrayList();
        for (String name : gcBeanNames) {
            GcTimes diff = gcTimesAfterSleep.get(name).subtract(gcTimesBeforeSleep.get(name));
            if (diff.gcCount != 0) {
                gcDiffs.add("GC pool '" + name + "' had collection(s): " + diff.toString());
            }
        }

        String ret = "Detected pause in JVM or host machine (eg GC): pause of approximately " + extraSleepTime + "ms\n";
        if (gcDiffs.isEmpty()) {
            ret += "No GCs detected";
        } else {
            ret += Joiner.on("\n").join(gcDiffs);
        }
        return ret;
    }

    private Map<String, GcTimes> getGcTimes() {
        Map<String, GcTimes> map = Maps.newHashMap();
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            map.put(gcBean.getName(), new GcTimes(gcBean));
        }
        return map;
    }

    private static class GcTimes {

        private GcTimes(GarbageCollectorMXBean gcBean) {
            gcCount = gcBean.getCollectionCount();
            gcTimeMillis = gcBean.getCollectionTime();
        }

        private GcTimes(long count, long time) {
            this.gcCount = count;
            this.gcTimeMillis = time;
        }

        private GcTimes subtract(GcTimes other) {
            return new GcTimes(this.gcCount - other.gcCount, this.gcTimeMillis - other.gcTimeMillis);
        }

        @Override
        public String toString() {
            return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
        }

        private long gcCount;
        private long gcTimeMillis;
    }

    private class Monitor implements Runnable {

        @Override
        public void run() {
            Stopwatch sw = new Stopwatch();
            Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
            while (shouldRun) {
                sw.reset().start();
                try {
                    Thread.sleep(SLEEP_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    return;
                }
                long extraSleepTime = sw.elapsedMillis() - SLEEP_INTERVAL_MS;
                Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

                if (extraSleepTime > warnThresholdMs) {
                    LOG.warn(formatMessage(extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
                    instrumentation.incr("jvm", "pause.warn-threshold", 1);
                } else if (extraSleepTime > infoThresholdMs) {
                    LOG.info(formatMessage(extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
                    instrumentation.incr("jvm", "pause.info-threshold", 1);
                }
                instrumentation.incr("jvm", "pause.extraSleepTime", extraSleepTime);

                gcTimesBeforeSleep = gcTimesAfterSleep;
            }
        }
    }
}
