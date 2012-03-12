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
package org.apache.oozie.command.wf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

/**
 * This Command is expected to be called when a Workflow moves to any terminal
 * state ( such as SUCCEEDED, KILLED, FAILED). This class primarily removes the
 * temporary directory created for specific workflow id
 */
public class WfEndXCommand extends WorkflowXCommand<Void> {

    private WorkflowJob job = null;

    public WfEndXCommand(WorkflowJob job) {
        super("wf_end", "wf_end", 1);
        this.job = job;
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED WFEndXCommand " + job.getId());
        deleteWFDir();
        LOG.debug("ENDED WFEndXCommand " + job.getId());
        return null;
    }

    private void deleteWFDir() throws CommandException {
        FileSystem fs;
        try {
            fs = getAppFileSystem(job);
            String wfDir = Services.get().getSystemId() + "/" + job.getId();
            Path wfDirPath = new Path(fs.getHomeDirectory(), wfDir);
            LOG.debug("WF tmp dir :" + wfDirPath);
            if (fs.exists(wfDirPath)) {
                fs.delete(wfDirPath, true);
            }
            else {
                LOG.debug("Tmp dir doesn't exist :" + wfDirPath);
            }
        }
        catch (Exception e) {
            LOG.error("Unable to delete WF temp dir of wf id :" + job.getId(), e);
            throw new CommandException(ErrorCode.E0819);
        }

    }

    protected FileSystem getAppFileSystem(WorkflowJob workflow) throws HadoopAccessorException, IOException,
            URISyntaxException {
        URI uri = new URI(workflow.getAppPath());
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        Configuration fsConf = has.createJobConf(uri.getAuthority());
        return has.createFileSystem(workflow.getUser(), uri, fsConf);
    }

    @Override
    public String getEntityKey() {
        return job.getId();
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() throws CommandException {

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

    }

}
