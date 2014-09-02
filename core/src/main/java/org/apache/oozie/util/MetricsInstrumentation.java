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


package org.apache.oozie.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Instrumentation framework that is mostly compatible with {@link Instrumentation} but is backed by Codahale Metrics.  This class
 * was designed to minimize the changes required to switch from {@link Instrumentation} to {@link MetricsInstrumentation} by keeping
 * the same API.  However, certain operations are obviously implemented differently or are no longer needed; and the output format
 * is a little different.  Internally, this class maps Cron to {@link com.codahale.metrics.Timer}, Variable to {@link Gauge},
 * counter to {@link Counter}, and Sampler to {@link Histogram}.
 */
@SuppressWarnings("unchecked")
public class MetricsInstrumentation extends Instrumentation {

    private final MetricRegistry metricRegistry;
    private transient ObjectMapper jsonMapper;
    private ScheduledExecutorService scheduler;
    private final LoadingCache<String, Counter> counters;
    private final Map<String, Gauge> gauges;
    private final LoadingCache<String, com.codahale.metrics.Timer> timers;
    private final Map<String, Histogram> histograms;
    private Lock timersLock;
    private Lock gaugesLock;
    private Lock countersLock;
    private Lock histogramsLock;

    private static final TimeUnit RATE_UNIT = TimeUnit.MILLISECONDS;
    private static final TimeUnit DURATION_UNIT = TimeUnit.MILLISECONDS;

    /**
     * Creates the MetricsInstrumentation and starts taking some metrics.
     */
    public MetricsInstrumentation() {
        metricRegistry = new MetricRegistry();

        timersLock = new ReentrantLock();
        gaugesLock = new ReentrantLock();
        countersLock = new ReentrantLock();
        histogramsLock = new ReentrantLock();

        // Used for writing the json for the metrics (see com.codahale.metrics.servlets.MetricsServlet)
        // The "false" is to prevent it from printing out all of the values used in the histograms and timers
        this.jsonMapper = new ObjectMapper().registerModule(new MetricsModule(RATE_UNIT, DURATION_UNIT, false));

        // Register the JVM memory gauges and prefix the keys
        MemoryUsageGaugeSet memorySet = new MemoryUsageGaugeSet();
        for (String key : memorySet.getMetrics().keySet()) {
            metricRegistry.register(MetricRegistry.name("jvm", "memory", key), memorySet.getMetrics().get(key));
        }

        // By setting this up as a cache, if a counter doesn't exist when we try to retrieve it, it will automatically be created
        counters = CacheBuilder.newBuilder().build(
                new CacheLoader<String, Counter>() {
                    @Override
                    public Counter load(String key) throws Exception {
                        Counter counter = new Counter();
                        metricRegistry.register(key, counter);
                        return counter;
                    }
                }
        );
        timers = CacheBuilder.newBuilder().build(
                new CacheLoader<String, com.codahale.metrics.Timer>() {
                    @Override
                    public com.codahale.metrics.Timer load(String key) throws Exception {
                        com.codahale.metrics.Timer timer
                                = new com.codahale.metrics.Timer(new ExponentiallyDecayingReservoir());
                        metricRegistry.register(key, timer);
                        return timer;
                    }
                }
        );
        gauges = new ConcurrentHashMap<String, Gauge>();
        histograms = new ConcurrentHashMap<String, Histogram>();
    }

    /**
     * Add a cron to an instrumentation timer. The timer is created if it does not exists. <p/>
     * Internally, this is backed by a {@link com.codahale.metrics.Timer}.
     *
     * @param group timer group.
     * @param name timer name.
     * @param cron cron to add to the timer.
     */
    @Override
    public void addCron(String group, String name, Cron cron) {
        String key = MetricRegistry.name(group, name, "timer");
        try {
            timersLock.lock();
            com.codahale.metrics.Timer timer = timers.get(key);
            timer.update(cron.getOwn(), TimeUnit.MILLISECONDS);
        } catch(ExecutionException ee) {
            throw new RuntimeException(ee);
        } finally {
            timersLock.unlock();
        }
    }

    /**
     * Add an instrumentation variable. <p/>
     * Internally, this is backed by a {@link Gauge}.
     *
     * @param group counter group.
     * @param name counter name.
     * @param variable variable to add.
     */
    @Override
    public void addVariable(String group, String name, final Variable variable) {
        Gauge gauge = new Gauge() {
            @Override
            public Object getValue() {
                return variable.getValue();
            }
        };
        String key = MetricRegistry.name(group, name);

        try {
            gaugesLock.lock();
            gauges.put(key, gauge);
            // Metrics throws an Exception if we don't do this when the key already exists
            if (metricRegistry.getGauges().containsKey(key)) {
                XLog.getLog(MetricsInstrumentation.class).debug("A Variable with name [" + key + "] already exists. "
                        + " The old Variable will be overwritten, but this is not recommended");
                metricRegistry.remove(key);
            }
            metricRegistry.register(key, gauge);
        } finally {
            gaugesLock.unlock();
        }
    }

   /**
     * Increment an instrumentation counter. The counter is created if it does not exists. <p/>
     * Internally, this is backed by a {@link Counter}.
     *
     * @param group counter group.
     * @param name counter name.
     * @param count increment to add to the counter.
     */
    @Override
    public void incr(String group, String name, long count) {
        String key = MetricRegistry.name(group, name);
        try {
            countersLock.lock();
            counters.get(key).inc(count);
        } catch(ExecutionException ee) {
            throw new RuntimeException(ee);
        } finally {
            countersLock.unlock();
        }
    }

    /**
     * Add a sampling variable. <p/>
     * Internally, this is backed by a biased (decaying) {@link Histogram}.
     *
     * @param group timer group.
     * @param name timer name.
     * @param period (ignored)
     * @param interval sampling frequency, how often the variable is probed.
     * @param variable variable to sample.
     */
    @Override
    public void addSampler(String group, String name, int period, int interval, Variable<Long> variable) {
        if (scheduler == null) {
            throw new IllegalStateException("scheduler not set, cannot sample");
        }
        Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
        Sampler sampler = new Sampler(variable, histogram);
        scheduler.scheduleAtFixedRate(sampler, 0, interval, TimeUnit.SECONDS);
        String key = MetricRegistry.name(group, name, "histogram");
        try {
            histogramsLock.lock();
            histograms.put(key, histogram);
            // Metrics throws an Exception if we don't do this when the key already exists
            if (metricRegistry.getHistograms().containsKey(key)) {
                XLog.getLog(MetricsInstrumentation.class).debug("A Sampler with name [" + key + "] already exists. "
                        + " The old Sampler will be overwritten, but this is not recommended");
                metricRegistry.remove(key);
            }
            metricRegistry.register(key, histogram);
        } finally {
            histogramsLock.unlock();
        }
    }

    public static class Sampler implements Runnable {
        private final Variable<Long> variable;
        private final Histogram histogram;
        public Sampler(Variable<Long> variable, Histogram histogram) {
            this.variable = variable;
            this.histogram = histogram;
        }

        @Override
        public void run() {
            histogram.update(variable.getValue());
        }
    }

    /**
     * Set the scheduler instance to handle the samplers.
     *
     * @param scheduler scheduler instance.
     */
    @Override
    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Return the string representation of the instrumentation.  It does a JSON pretty-print.
     *
     * @return the string representation of the instrumentation.
     */
    @Override
    public String toString() {
        try {
            return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metricRegistry);
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

    /**
     * Converts the current state of the metrics and writes them to the OutputStream.
     *
     * @param os The OutputStream to write the metrics to
     * @throws IOException
     */
    public void writeJSONResponse(OutputStream os) throws IOException {
        jsonMapper.writer().writeValue(os, metricRegistry);
    }

    /**
     * Returns the MetricRegistry: public for unit tests -- do not use.
     *
     * @return the MetricRegistry
     */
    @VisibleForTesting
    MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    /**
     * Not Supported: throws {@link UnsupportedOperationException}
     *
     * @return nothing
     */
    @Override
    public Map<String, Map<String, Map<String, Object>>> getAll() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not Supported: throws {@link UnsupportedOperationException}
     *
     * @return nothing
     */
    @Override
    public Map<String, Map<String, Element<Long>>> getCounters() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not Supported: throws {@link UnsupportedOperationException}
     *
     * @return nothing
     */
    @Override
    public Map<String, Map<String, Element<Double>>> getSamplers() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not Supported: throws {@link UnsupportedOperationException}
     *
     * @return nothing
     */
    @Override
    public Map<String, Map<String, Element<Timer>>> getTimers() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not Supported: throws {@link UnsupportedOperationException}
     *
     * @return nothing
     */
    @Override
    public Map<String, Map<String, Element<Variable>>> getVariables() {
        throw new UnsupportedOperationException();
    }
}
