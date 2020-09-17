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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Appender;
import org.apache.log4j.pattern.ExtrasPatternParser;
import org.apache.log4j.rolling.RollingPolicyBase;
import org.apache.log4j.rolling.RolloverDescription;
import org.apache.log4j.rolling.TimeBasedRollingPolicy;
import org.apache.log4j.rolling.TriggeringPolicy;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.pattern.LiteralPatternConverter;
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

    String oozieLogDir;
    String logFileName;

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


    @SuppressWarnings("rawtypes")
    public void setFileNamePattern(String fnp) {
        super.setFileNamePattern(fnp);
        List converters = new ArrayList();
        StringBuffer file = new StringBuffer();
        ExtrasPatternParser
                .parse(fnp, converters, new ArrayList(), null, ExtrasPatternParser.getFileNamePatternRules());
        if (!converters.isEmpty()) {
            ((LiteralPatternConverter) converters.get(0)).format(null, file);
            File f = new File(file.toString());
            oozieLogDir = f.getParent();
            logFileName = f.getName();
        }
        else {
            XLogService xls = Services.get().get(XLogService.class);
            oozieLogDir = xls.getOozieLogPath();
            logFileName = xls.getOozieLogName();
        }

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
            // Only delete old logs if we're not already deleting logs and another thread hasn't already started setting
            // up to delete
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
        if (oozieLogDir != null && logFileName != null) {
            String[] children = new File(oozieLogDir).list();
            if (children != null) {
                for (String child : children) {
                    if (child.startsWith(logFileName) && !child.equals(logFileName)) {
                        File childFile = new File(new File(oozieLogDir).getAbsolutePath(), child);
                        if (child.endsWith(".gz")) {
                            long gzFileCreationTime = getGZFileCreationTime(child);
                            if (gzFileCreationTime != -1) {
                                fileList.add(new FileInfo(childFile.getAbsolutePath(), gzFileCreationTime));
                            }
                        }
                        else {
                            long modTime = childFile.lastModified();
                            fileList.add(new FileInfo(childFile.getAbsolutePath(), modTime));
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
        SimpleDateFormat formatter;
        String date = fileName.substring(logFileName.length(), fileName.length() - 3); //3 for .gz
        if (date.length() == 10) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        }
        try {
            return formatter.parse(date).getTime();
        }
        catch (ParseException e) {
            return -1;
        }
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

}
