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

package org.apache.oozie.servlet;

import java.io.Serializable;
import java.lang.Thread.State;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class JVMInfo implements Serializable {

    private static Map<State, Integer> threadStateOrdering;
    private final MemoryMXBean memoryMXBean;
    private final ClassLoadingMXBean classLoadingMXBean;
    private final ThreadMXBean threadMXBean;
    private List<JThreadInfo> jThreadInfos;
    private String threadSortOrder;
    private Integer cpuMonitorTime = 0;
    private int blockedThreads = 0;
    private int runnableThreads = 0;
    private int waitingThreads = 0;
    private int timedWaitingThreads = 0;
    private int newThreads = 0;
    private int terminatedThreads = 0;

    static {
        threadStateOrdering = new HashMap<State, Integer>();
        threadStateOrdering.put(State.RUNNABLE, 1);
        threadStateOrdering.put(State.BLOCKED, 2);
        threadStateOrdering.put(State.WAITING, 3);
        threadStateOrdering.put(State.TIMED_WAITING, 4);
        threadStateOrdering.put(State.NEW, 5);
        threadStateOrdering.put(State.TERMINATED, 6);
    }

    public JVMInfo() {
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
    }

    public String getThreadSortOrder() {
        return threadSortOrder;
    }

    public void setThreadSortOrder(String threadSortOrder) {
        this.threadSortOrder = threadSortOrder;
    }

    public String getCpuMonitorTime() {
        return cpuMonitorTime.toString();
    }

    public void setCpuMonitorTime(String sleepTime) {
        if (sleepTime != null) {
            this.cpuMonitorTime = Integer.parseInt(sleepTime);
        }
    }

    public String getHeapMemoryUsage() {
        MemoryUsage hmu = memoryMXBean.getHeapMemoryUsage();
        StringBuffer sb = new StringBuffer(60);
        sb.append("INIT=").append(hmu.getInit());
        sb.append("&nbsp;&nbsp;USED=").append(hmu.getUsed());
        sb.append("&nbsp;&nbsp;COMMITTED=").append(hmu.getCommitted());
        sb.append("&nbsp;&nbsp;MAX=").append(hmu.getMax());
        return sb.toString();
    }

    public String getNonHeapMemoryUsage() {
        MemoryUsage nhmu = memoryMXBean.getNonHeapMemoryUsage();
        StringBuffer sb = new StringBuffer(60);
        sb.append("INIT=").append(nhmu.getInit());
        sb.append("&nbsp;&nbsp;USED=").append(nhmu.getUsed());
        sb.append("&nbsp;&nbsp;COMMITTED=").append(nhmu.getCommitted());
        sb.append("&nbsp;&nbsp;MAX=").append(nhmu.getMax());
        return sb.toString();
    }

    public String getClassLoadingInfo() {
        StringBuffer sb = new StringBuffer(150);
        sb.append("Total Loaded Classes=").append(classLoadingMXBean.getTotalLoadedClassCount());
        sb.append("&nbsp;&nbsp;Loaded Classes=").append(classLoadingMXBean.getLoadedClassCount());
        sb.append("&nbsp;&nbsp;Unloaded Classes=").append(classLoadingMXBean.getUnloadedClassCount());
        return sb.toString();
    }

    public String getThreadInfo() throws InterruptedException {
        getThreads();
        StringBuffer sb = new StringBuffer(150);
        sb.append("Thread Count=").append(threadMXBean.getThreadCount());
        sb.append("&nbsp;&nbsp;Peak Thread Count=").append(threadMXBean.getPeakThreadCount());
        sb.append("&nbsp;&nbsp;Total Started Threads=").append(threadMXBean.getTotalStartedThreadCount());
        sb.append("&nbsp;&nbsp;Deamon Threads=").append(threadMXBean.getDaemonThreadCount());
        sb.append("<br /> &nbsp;&nbsp;&nbsp;&nbsp;RUNNABLE=").append(runnableThreads);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;BLOCKED=").append(blockedThreads);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;WAITING=").append(waitingThreads);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;TIMED_WAITING=").append(timedWaitingThreads);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;NEW=").append(newThreads);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;TERMINATED=").append(terminatedThreads);
        sb.append("<br /> &nbsp;&nbsp;&nbsp;&nbsp;Time of Thread Dump=").append(new Date().toString());
        return sb.toString();
    }

    public Collection<JThreadInfo> getThreads() throws InterruptedException {
        if (jThreadInfos == null) {
            jThreadInfos = getThreadInfos();
            Collections.sort(jThreadInfos, new ThreadComparator(threadSortOrder));
        }
        return jThreadInfos;
    }

    private List<JThreadInfo> getThreadInfos() throws InterruptedException {
        ThreadInfo[] threads = threadMXBean.dumpAllThreads(false, false);
        Map<Long, JThreadInfo> oldThreadInfoMap = new HashMap<Long, JThreadInfo>();
        for (ThreadInfo thread : threads) {
            long cpuTime = -1L;
            long userTime = -1L;
            long threadId = thread.getThreadId();
            if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
                cpuTime = threadMXBean.getThreadCpuTime(threadId);
                userTime = threadMXBean.getThreadUserTime(threadId);
            }
            oldThreadInfoMap.put(thread.getThreadId(), new JThreadInfo(cpuTime, userTime, thread));
        }
        Thread.sleep(cpuMonitorTime);
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(false, false);
        List<JThreadInfo> jThreadInfos = new ArrayList<JThreadInfo>(threadInfos.length);
        for (int i = 0; i < threadInfos.length; i++) {
            ThreadInfo threadInfo = threadInfos[i];
            long threadId = threadInfo.getThreadId();
            long cpuTime = -1L;
            long userTime = -1L;
            if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
                JThreadInfo oldThread = oldThreadInfoMap.get(threadInfo.getThreadId());
                if (oldThread == null) {
                    cpuTime = threadMXBean.getThreadCpuTime(threadId);
                    userTime = threadMXBean.getThreadUserTime(threadId);
                }
                else {
                    cpuTime = threadMXBean.getThreadCpuTime(threadId) - oldThread.getCpuTime();
                    userTime = threadMXBean.getThreadUserTime(threadId) - oldThread.getUserTime();
                }
            }
            jThreadInfos.add(new JThreadInfo(cpuTime, userTime, threadInfo));
            switch (threadInfo.getThreadState()) {
                case RUNNABLE:
                    runnableThreads++;
                    break;
                case BLOCKED:
                    blockedThreads++;
                    break;
                case WAITING:
                    waitingThreads++;
                    break;
                case TIMED_WAITING:
                    timedWaitingThreads++;
                    break;
                case NEW:
                    newThreads++;
                    break;
                case TERMINATED:
                    terminatedThreads++;
                    break;
            }
        }
        return jThreadInfos;
    }

    public static final class JThreadInfo {
        private long cpuTime;
        private long userTime;
        private ThreadInfo threadInfo;

        public JThreadInfo(long cpuTime, long userTime, ThreadInfo threadInfo) {
            this.cpuTime = cpuTime;
            this.userTime = userTime;
            this.threadInfo = threadInfo;
        }

        public long getCpuTime() {
            return cpuTime;
        }

        public long getUserTime() {
            return userTime;
        }

        public ThreadInfo getThreadInfo() {
            return threadInfo;
        }

    }

    private static final class ThreadComparator implements Comparator<JThreadInfo> {

        private String threadSortOrder;

        public ThreadComparator(String threadSortOrder) {
            this.threadSortOrder = threadSortOrder;
        }

        @Override
        public int compare(JThreadInfo jt1, JThreadInfo jt2) {
            ThreadInfo o1 = jt1.getThreadInfo();
            ThreadInfo o2 = jt2.getThreadInfo();
            if ("cpu".equals(threadSortOrder)) {
                int result = (int) (jt2.getCpuTime() - jt1.getCpuTime());
                if (result == 0) {
                    return compareId(o1, o2);
                }
                return result;
            }
            else if ("name".equals(threadSortOrder)) {
                return compareName(o1, o2);
            }
            else {
                // state
                if (o1.getThreadState().equals(o2.getThreadState())) {
                    return compareName(o1, o2);
                }
                else {
                    return (int) threadStateOrdering.get(o1.getThreadState())
                            - threadStateOrdering.get(o2.getThreadState());
                }
            }
        }

        private int compareId(ThreadInfo o1, ThreadInfo o2) {
            Long id1 = o1.getThreadId();
            Long id2 = o2.getThreadId();
            return id1.compareTo(id2);
        }

        private int compareName(ThreadInfo o1, ThreadInfo o2) {
            int result = o1.getThreadName().compareTo(o2.getThreadName());
            if (result == 0) {
                Long id1 = o1.getThreadId();
                Long id2 = o2.getThreadId();
                return id1.compareTo(id2);
            }
            return result;
        }

    }

}
