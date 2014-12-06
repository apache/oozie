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

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import org.apache.log4j.Appender;
import org.apache.log4j.rolling.RollingPolicyBase;
import org.apache.log4j.rolling.RolloverDescription;
import org.apache.log4j.rolling.TimeBasedRollingPolicy;
import org.apache.log4j.rolling.TriggeringPolicy;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;

/**
 * Has the same behavior as the TimeBasedRollingPolicy.  Additionally, it will delete older logs (MaxHistory determines how many
 * older logs are retained).
 */
public class OozieRollingPolicy extends RollingPolicyBase implements TriggeringPolicy {

    /**
     * Unfortunately, TimeBasedRollingPolicy is declared final, so we can't subclass it; instead, we have to wrap it
     */
    private TimeBasedRollingPolicy tbrp;
    
    private Semaphore deleteSem;
    
    private Thread deleteThread;
    
    private int maxHistory = 720;       // (720 hours / 24 hours per day = 30 days) as default
    
    public int getMaxHistory() {
        return maxHistory;
    }

    public void setMaxHistory(int maxHistory) {
        this.maxHistory = maxHistory;
    }
    
    public OozieRollingPolicy() {
        deleteSem = new Semaphore(1);
        deleteThread = new Thread();
        tbrp = new TimeBasedRollingPolicy();
    }
    
    @Override
    public void activateOptions() {
        super.activateOptions();
        tbrp.setFileNamePattern(getFileNamePattern());
        tbrp.activateOptions();
    }
    
    @Override
    public RolloverDescription initialize(String file, boolean append) throws SecurityException {
        return tbrp.initialize(file, append);
    }
    
    @Override
    public RolloverDescription rollover(final String activeFile) throws SecurityException {
        return tbrp.rollover(activeFile);
    }
    
    @Override
    public boolean isTriggeringEvent(final Appender appender, final LoggingEvent event, final String filename, 
    final long fileLength) {
        if (maxHistory >= 0) {  // -1 = disable
            // Only delete old logs if we're not already deleting logs and another thread hasn't already started setting up to delete
            // the old logs
            if (deleteSem.tryAcquire()) {
                if (!deleteThread.isAlive()) {
                    // Do the actual deleting in a new thread in case its slow so we don't bottleneck anything else
                    deleteThread = new Thread() {
                        @Override
                        public void run() {
                            deleteOldFiles();
                        }
                    };
                    deleteThread.start();
                }
                deleteSem.release();
            }
        }
        return tbrp.isTriggeringEvent(appender, event, filename, fileLength);
    }
    
    private void deleteOldFiles() {
        ArrayList<FileInfo> fileList = new ArrayList<FileInfo>();
        XLogService xls = getXLogService();
        if (xls != null) {      // We need this to get the paths
            String oozieLogPath = xls.getOozieLogPath();
            String logFile = xls.getOozieLogName();
            if (oozieLogPath != null && logFile != null) {
                String[] children = new File(oozieLogPath).list();
                if (children != null) {
                    for (String child : children) {
                        if (child.startsWith(logFile) && !child.equals(logFile)) {
                            File childFile = new File(new File(oozieLogPath).getAbsolutePath(), child);
                            if (child.endsWith(".gz")) {
                                long gzFileCreationTime = getGZFileCreationTime(child);
                                if (gzFileCreationTime != -1) {
                                    fileList.add(new FileInfo(childFile.getAbsolutePath(), gzFileCreationTime));
                                }
                            } else{ 
                                long modTime = childFile.lastModified();
                                fileList.add(new FileInfo(childFile.getAbsolutePath(), modTime));
                            }
                        }
                    }
                }
            }
        }
        
        if (fileList.size() > maxHistory) {
            Collections.sort(fileList);
            
            for (int i = maxHistory; i < fileList.size(); i++) {
                new File(fileList.get(i).getFileName()).delete();
            }
        }
    }
    
    private long getGZFileCreationTime(String fileName) {
        // Default return value of -1 to exclude the file
        long returnVal = -1;
        Matcher m = XLogStreamer.gzTimePattern.matcher(fileName);
        if (m.matches() && m.groupCount() == 4) {
            int year = Integer.parseInt(m.group(1));
            int month = Integer.parseInt(m.group(2));
            int day = Integer.parseInt(m.group(3));
            int hour = Integer.parseInt(m.group(4));
            int minute = 0;
            Calendar calendarEntry = Calendar.getInstance();
            calendarEntry.set(year, month - 1, day, hour, minute); // give month-1(Say, 7 for August)
            long logFileStartTime = calendarEntry.getTimeInMillis();
            returnVal = logFileStartTime;
        }
        return returnVal;
    }
    
    class FileInfo implements Comparable<FileInfo> {
        String fileName;
        long modTime;

        public FileInfo(String fileName, long modTime) {
            this.fileName = fileName;
            this.modTime = modTime;
        }

        public String getFileName() {
            return fileName;
        }

        public long getModTime() {
            return modTime;
        }

        public int compareTo(FileInfo fileInfo) {
            // Note: the order is the reverse of XLogStreamer.FileInfo
            long diff = fileInfo.modTime - this.modTime;
            if (diff > 0) {
                return 1;
            }
            else if (diff < 0) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }
    
    // Needed for TestOozieRollingPolicy tests to be able to override getOozieLogPath() and getOozieLogName()
    // by overriding getXLogService()
    XLogService getXLogService() {
        if (Services.get() != null) {
            return Services.get().get(XLogService.class);
        }
        return null;
    }
}
