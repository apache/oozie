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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Instrumentation framework that supports Timers, Counters, Variables and Sampler instrumentation elements. <p/> All
 * instrumentation elements have a group and a name.
 */
public class Instrumentation {
    private ScheduledExecutorService scheduler;
    private Lock counterLock;
    private Lock timerLock;
    private Lock variableLock;
    private Lock samplerLock;
    private Configuration configuration;
    private Map<String, Map<String, Map<String, Object>>> all;
    private Map<String, Map<String, Element<Long>>> counters;
    private Map<String, Map<String, Element<Timer>>> timers;
    private Map<String, Map<String, Element<Variable>>> variables;
    private Map<String, Map<String, Element<Double>>> samplers;

    /**
     * Instrumentation constructor.
     */
    @SuppressWarnings("unchecked")
    public Instrumentation() {
        counterLock = new ReentrantLock();
        timerLock = new ReentrantLock();
        variableLock = new ReentrantLock();
        samplerLock = new ReentrantLock();
        all = new LinkedHashMap<String, Map<String, Map<String, Object>>>();
        counters = new ConcurrentHashMap<String, Map<String, Element<Long>>>();
        timers = new ConcurrentHashMap<String, Map<String, Element<Timer>>>();
        variables = new ConcurrentHashMap<String, Map<String, Element<Variable>>>();
        samplers = new ConcurrentHashMap<String, Map<String, Element<Double>>>();
        all.put("variables", (Map<String, Map<String, Object>>) (Object) variables);
        all.put("samplers", (Map<String, Map<String, Object>>) (Object) samplers);
        all.put("counters", (Map<String, Map<String, Object>>) (Object) counters);
        all.put("timers", (Map<String, Map<String, Object>>) (Object) timers);
    }

    /**
     * Set the scheduler instance to handle the samplers.
     *
     * @param scheduler scheduler instance.
     */
    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Cron is a stopwatch that can be started/stopped several times. <p/> This class is not thread safe, it does not
     * need to be. <p/> It keeps track of the total time (first start to last stop) and the running time (total time
     * minus the stopped intervals). <p/> Once a Cron is complete it must be added to the corresponding group/name in a
     * Instrumentation instance.
     */
    public static class Cron {
        private long start;
        private long end;
        private long lapStart;
        private long own;
        private long total;
        private boolean running;

        /**
         * Creates new Cron, stopped, in zero.
         */
        public Cron() {
            running = false;
        }

        /**
         * Start the cron. It cannot be already started.
         */
        public void start() {
            if (!running) {
                if (lapStart == 0) {
                    lapStart = System.currentTimeMillis();
                    if (start == 0) {
                        start = lapStart;
                        end = start;
                    }
                }
                running = true;
            }
        }

        /**
         * Stops the cron. It cannot be already stopped.
         */
        public void stop() {
            if (running) {
                end = System.currentTimeMillis();
                if (start == 0) {
                    start = end;
                }
                total = end - start;
                if (lapStart > 0) {
                    own += end - lapStart;
                    lapStart = 0;
                }
                running = false;
            }
        }

        /**
         * Return the start time of the cron. It must be stopped.
         *
         * @return the start time of the cron.
         */
        public long getStart() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return start;
        }

        /**
         * Return the end time of the cron.  It must be stopped.
         *
         * @return the end time of the cron.
         */
        public long getEnd() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return end;
        }

        /**
         * Return the total time of the cron. It must be stopped.
         *
         * @return the total time of the cron.
         */
        public long getTotal() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return total;
        }

        /**
         * Return the own time of the cron. It must be stopped.
         *
         * @return the own time of the cron.
         */
        public long getOwn() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return own;
        }

    }

    /**
     * Gives access to a snapshot of an Instrumentation element (Counter, Timer). <p/> Instrumentation element snapshots
     * are returned by the {@link Instrumentation#getCounters()} and {@link Instrumentation#getTimers()} ()} methods.
     */
    public interface Element<T> {

        /**
         * Return the snapshot value of the Intrumentation element.
         *
         * @return the snapshot value of the Intrumentation element.
         */
        T getValue();
    }

    /**
     * Counter Instrumentation element.
     */
    private static class Counter extends AtomicLong implements Element<Long> {

        /**
         * Return the counter snapshot.
         *
         * @return the counter snapshot.
         */
        public Long getValue() {
            return get();
        }

        /**
         * Return the String representation of the counter value.
         *
         * @return the String representation of the counter value.
         */
        public String toString() {
            return Long.toString(get());
        }

    }

    /**
     * Timer Instrumentation element.
     */
    public static class Timer implements Element<Timer> {
        Lock lock = new ReentrantLock();
        private long ownTime;
        private long totalTime;
        private long ticks;
        private long ownSquareTime;
        private long totalSquareTime;
        private long ownMinTime;
        private long ownMaxTime;
        private long totalMinTime;
        private long totalMaxTime;

        /**
         * Timer constructor. <p/> It is project private for test purposes.
         */
        Timer() {
        }

        /**
         * Return the String representation of the timer value.
         *
         * @return the String representation of the timer value.
         */
        public String toString() {
            return XLog.format("ticks[{0}] totalAvg[{1}] ownAvg[{2}]", ticks, getTotalAvg(), getOwnAvg());
        }

        /**
         * Return the timer snapshot.
         *
         * @return the timer snapshot.
         */
        public Timer getValue() {
            try {
                lock.lock();
                Timer timer = new Timer();
                timer.ownTime = ownTime;
                timer.totalTime = totalTime;
                timer.ticks = ticks;
                timer.ownSquareTime = ownSquareTime;
                timer.totalSquareTime = totalSquareTime;
                timer.ownMinTime = ownMinTime;
                timer.ownMaxTime = ownMaxTime;
                timer.totalMinTime = totalMinTime;
                timer.totalMaxTime = totalMaxTime;
                return timer;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Add a cron to a timer. <p/> It is project private for test purposes.
         *
         * @param cron Cron to add.
         */
        void addCron(Cron cron) {
            try {
                lock.lock();
                long own = cron.getOwn();
                long total = cron.getTotal();
                ownTime += own;
                totalTime += total;
                ticks++;
                ownSquareTime += own * own;
                totalSquareTime += total * total;
                if (ticks == 1) {
                    ownMinTime = own;
                    ownMaxTime = own;
                    totalMinTime = total;
                    totalMaxTime = total;
                }
                else {
                    ownMinTime = Math.min(ownMinTime, own);
                    ownMaxTime = Math.max(ownMaxTime, own);
                    totalMinTime = Math.min(totalMinTime, total);
                    totalMaxTime = Math.max(totalMaxTime, total);
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Return the own accumulated computing time by the timer.
         *
         * @return own accumulated computing time by the timer.
         */
        public long getOwn() {
            return ownTime;
        }

        /**
         * Return the total accumulated computing time by the timer.
         *
         * @return total accumulated computing time by the timer.
         */
        public long getTotal() {
            return totalTime;
        }

        /**
         * Return the number of times a cron was added to the timer.
         *
         * @return the number of times a cron was added to the timer.
         */
        public long getTicks() {
            return ticks;
        }

        /**
         * Return the sum of the square own times. <p/> It can be used to calculate the standard deviation.
         *
         * @return the sum of the square own timer.
         */
        public long getOwnSquareSum() {
            return ownSquareTime;
        }

        /**
         * Return the sum of the square total times. <p/> It can be used to calculate the standard deviation.
         *
         * @return the sum of the square own timer.
         */
        public long getTotalSquareSum() {
            return totalSquareTime;
        }

        /**
         * Returns the own minimum time.
         *
         * @return the own minimum time.
         */
        public long getOwnMin() {
            return ownMinTime;
        }

        /**
         * Returns the own maximum time.
         *
         * @return the own maximum time.
         */
        public long getOwnMax() {
            return ownMaxTime;
        }

        /**
         * Returns the total minimum time.
         *
         * @return the total minimum time.
         */
        public long getTotalMin() {
            return totalMinTime;
        }

        /**
         * Returns the total maximum time.
         *
         * @return the total maximum time.
         */
        public long getTotalMax() {
            return totalMaxTime;
        }

        /**
         * Returns the own average time.
         *
         * @return the own average time.
         */
        public long getOwnAvg() {
            return (ticks != 0) ? ownTime / ticks : 0;
        }

        /**
         * Returns the total average time.
         *
         * @return the total average time.
         */
        public long getTotalAvg() {
            return (ticks != 0) ? totalTime / ticks : 0;
        }

        /**
         * Returns the total time standard deviation.
         *
         * @return the total time standard deviation.
         */
        public double getTotalStdDev() {
            return evalStdDev(ticks, totalTime, totalSquareTime);
        }

        /**
         * Returns the own time standard deviation.
         *
         * @return the own time standard deviation.
         */
        public double getOwnStdDev() {
            return evalStdDev(ticks, ownTime, ownSquareTime);
        }

        private double evalStdDev(long n, long sn, long ssn) {
            return (n < 2) ? -1 : Math.sqrt((n * ssn - sn * sn) / (n * (n - 1)));
        }

    }

    /**
     * Add a cron to an instrumentation timer. The timer is created if it does not exists. <p/> This method is thread
     * safe.
     *
     * @param group timer group.
     * @param name timer name.
     * @param cron cron to add to the timer.
     */
    public void addCron(String group, String name, Cron cron) {
        Map<String, Element<Timer>> map = timers.get(group);
        if (map == null) {
            try {
                timerLock.lock();
                map = timers.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Timer>>();
                    timers.put(group, map);
                }
            }
            finally {
                timerLock.unlock();
            }
        }
        Timer timer = (Timer) map.get(name);
        if (timer == null) {
            try {
                timerLock.lock();
                timer = (Timer) map.get(name);
                if (timer == null) {
                    timer = new Timer();
                    map.put(name, timer);
                }
            }
            finally {
                timerLock.unlock();
            }
        }
        timer.addCron(cron);
    }

    /**
     * Increment an instrumentation counter. The counter is created if it does not exists. <p/> This method is thread
     * safe.
     *
     * @param group counter group.
     * @param name counter name.
     * @param count increment to add to the counter.
     */
    public void incr(String group, String name, long count) {
        Map<String, Element<Long>> map = counters.get(group);
        if (map == null) {
            try {
                counterLock.lock();
                map = counters.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Long>>();
                    counters.put(group, map);
                }
            }
            finally {
                counterLock.unlock();
            }
        }
        Counter counter = (Counter) map.get(name);
        if (counter == null) {
            try {
                counterLock.lock();
                counter = (Counter) map.get(name);
                if (counter == null) {
                    counter = new Counter();
                    map.put(name, counter);
                }
            }
            finally {
                counterLock.unlock();
            }
        }
        counter.addAndGet(count);
    }

    /**
     * Interface for instrumentation variables. <p/> For example a the database service could expose the number of
     * currently active connections.
     */
    public interface Variable<T> extends Element<T> {
    }

    /**
     * Add an instrumentation variable. The variable must not exist. <p/> This method is thread safe.
     *
     * @param group counter group.
     * @param name counter name.
     * @param variable variable to add.
     */
    @SuppressWarnings("unchecked")
    public void addVariable(String group, String name, Variable variable) {
        Map<String, Element<Variable>> map = variables.get(group);
        if (map == null) {
            try {
                variableLock.lock();
                map = variables.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Variable>>();
                    variables.put(group, map);
                }
            }
            finally {
                variableLock.unlock();
            }
        }
        if (map.containsKey(name)) {
            throw new RuntimeException(XLog.format("Variable group=[{0}] name=[{1}] already defined", group, name));
        }
        map.put(name, variable);
    }

    /**
     * Set the system configuration.
     *
     * @param configuration system configuration.
     */
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Return the JVM system properties.
     *
     * @return JVM system properties.
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getJavaSystemProperties() {
        return (Map<String, String>) (Object) System.getProperties();
    }

    /**
     * Return the OS environment used to start Oozie.
     *
     * @return the OS environment used to start Oozie.
     */
    public Map<String, String> getOSEnv() {
        return System.getenv();
    }

    /**
     * Return the current system configuration as a Map<String,String>.
     *
     * @return the current system configuration as a Map<String,String>.
     */
    public Map<String, String> getConfiguration() {
        final Configuration maskedConf = ConfigurationService.maskPasswords(configuration);

        return new Map<String, String>() {
            public int size() {
                return maskedConf.size();
            }

            public boolean isEmpty() {
                return maskedConf.size() == 0;
            }

            public boolean containsKey(Object o) {
                return maskedConf.get((String) o) != null;
            }

            public boolean containsValue(Object o) {
                throw new UnsupportedOperationException();
            }

            public String get(Object o) {
                return maskedConf.get((String) o);
            }

            public String put(String s, String s1) {
                throw new UnsupportedOperationException();
            }

            public String remove(Object o) {
                throw new UnsupportedOperationException();
            }

            public void putAll(Map<? extends String, ? extends String> map) {
                throw new UnsupportedOperationException();
            }

            public void clear() {
                throw new UnsupportedOperationException();
            }

            public Set<String> keySet() {
                Set<String> set = new LinkedHashSet<String>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry.getKey());
                }
                return set;
            }

            public Collection<String> values() {
                Set<String> set = new LinkedHashSet<String>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry.getValue());
                }
                return set;
            }

            public Set<Entry<String, String>> entrySet() {
                Set<Entry<String, String>> set = new LinkedHashSet<Entry<String, String>>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry);
                }
                return set;
            }
        };
    }

    /**
     * Return all the counters. <p/> This method is thread safe. <p/> The counters are live. The counter value is a
     * snapshot at the time the {@link Instrumentation.Element#getValue()} is invoked.
     *
     * @return all counters.
     */
    public Map<String, Map<String, Element<Long>>> getCounters() {
        return counters;
    }

    /**
     * Return all the timers. <p/> This method is thread safe. <p/> The timers are live. Once a timer is obtained, all
     * its values are consistent (they are snapshot at the time the {@link Instrumentation.Element#getValue()} is
     * invoked.
     *
     * @return all counters.
     */
    public Map<String, Map<String, Element<Timer>>> getTimers() {
        return timers;
    }

    /**
     * Return all the variables. <p/> This method is thread safe. <p/> The variables are live. The variable value is a
     * snapshot at the time the {@link Instrumentation.Element#getValue()} is invoked.
     *
     * @return all counters.
     */
    public Map<String, Map<String, Element<Variable>>> getVariables() {
        return variables;
    }

    /**
     * Return a map containing all variables, counters and timers.
     *
     * @return a map containing all variables, counters and timers.
     */
    public Map<String, Map<String, Map<String, Object>>> getAll() {
        return all;
    }

    /**
     * Return the string representation of the instrumentation.
     *
     * @return the string representation of the instrumentation.
     */
    public String toString() {
        String E = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder(4096);
        for (String element : all.keySet()) {
            sb.append(element).append(':').append(E);
            List<String> groups = new ArrayList<String>(all.get(element).keySet());
            Collections.sort(groups);
            for (String group : groups) {
                sb.append("  ").append(group).append(':').append(E);
                List<String> names = new ArrayList<String>(all.get(element).get(group).keySet());
                Collections.sort(names);
                for (String name : names) {
                    sb.append("    ").append(name).append(": ").append(((Element) all.get(element).
                            get(group).get(name)).getValue()).append(E);
                }
            }
        }
        return sb.toString();
    }

    private static class Sampler implements Element<Double>, Runnable {
        private Lock lock = new ReentrantLock();
        private int samplingInterval;
        private Variable<Long> variable;
        private long[] values;
        private int current;
        private long valuesSum;
        private double rate;

        public Sampler(int samplingPeriod, int samplingInterval, Variable<Long> variable) {
            this.samplingInterval = samplingInterval;
            this.variable = variable;
            values = new long[samplingPeriod / samplingInterval];
            valuesSum = 0;
            current = -1;
        }

        public int getSamplingInterval() {
            return samplingInterval;
        }

        public void run() {
            try {
                lock.lock();
                long newValue = variable.getValue();
                if (current == -1) {
                    valuesSum = newValue;
                    current = 0;
                    values[current] = newValue;
                }
                else {
                    current = (current + 1) % values.length;
                    valuesSum = valuesSum - values[current] + newValue;
                    values[current] = newValue;
                }
                rate = ((double) valuesSum) / values.length;
            }
            finally {
                lock.unlock();
            }
        }

        public Double getValue() {
            return rate;
        }
    }

    /**
     * Add a sampling variable. <p/> This method is thread safe.
     *
     * @param group timer group.
     * @param name timer name.
     * @param period sampling period to compute rate.
     * @param interval sampling frequency, how often the variable is probed.
     * @param variable variable to sample.
     */
    public void addSampler(String group, String name, int period, int interval, Variable<Long> variable) {
        if (scheduler == null) {
            throw new IllegalStateException("scheduler not set, cannot sample");
        }
        try {
            samplerLock.lock();
            Map<String, Element<Double>> map = samplers.get(group);
            if (map == null) {
                map = samplers.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Double>>();
                    samplers.put(group, map);
                }
            }
            if (map.containsKey(name)) {
                throw new RuntimeException(XLog.format("Sampler group=[{0}] name=[{1}] already defined", group, name));
            }
            Sampler sampler = new Sampler(period, interval, variable);
            map.put(name, sampler);
            scheduler.scheduleAtFixedRate(sampler, 0, sampler.getSamplingInterval(), TimeUnit.SECONDS);
        }
        finally {
            samplerLock.unlock();
        }
    }

    /**
     * Return all the samplers. <p/> This method is thread safe. <p/> The samplers are live. The sampler value is a
     * snapshot at the time the {@link Instrumentation.Element#getValue()} is invoked.
     *
     * @return all counters.
     */
    public Map<String, Map<String, Element<Double>>> getSamplers() {
        return samplers;
    }

}
