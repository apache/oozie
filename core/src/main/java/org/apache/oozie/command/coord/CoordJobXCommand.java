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

package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine.FILTER_COMPARATORS;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionsCountForJobIdJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsSubsetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command for loading a coordinator job information
 */
public class CoordJobXCommand extends CoordinatorXCommand<CoordinatorJobBean> {
    private final String id;
    private final boolean getActionInfo;
    private int offset = 1;
    private int len = Integer.MAX_VALUE;
    private boolean desc = false;
    private Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap;

    /**
     * Constructor for loading a coordinator job information
     *
     * @param id coord jobId
     */
    public CoordJobXCommand(String id) {
        this(id, null, 1, Integer.MAX_VALUE, false);
    }

    /**
     * Constructor for loading a coordinator job information
     * @param id coord jobId
     * @param filterMap
     * @param offset starting index in the list of actions belonging to the job
     * @param length number of actions to be returned
     * @param desc boolean for whether the actions returned are in descending order
     */
    public CoordJobXCommand(String id, Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap, int offset,
        int length, boolean desc) {
        super("job.info", "job.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
        this.getActionInfo = true;
        this.filterMap = filterMap;
        this.offset = offset;
        this.len = length;
        this.desc = desc;
    }

    /**
     * Constructor for loading a coordinator job information
     *
     * @param id coord jobId
     * @param getActionInfo false to ignore loading actions for the job
     */
    public CoordJobXCommand(String id, boolean getActionInfo) {
        super("job.info", "job.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
        this.getActionInfo = getActionInfo;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return this.id;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected CoordinatorJobBean execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorJobBean coordJob = null;
            if (jpaService != null) {
                coordJob = jpaService.execute(new CoordJobGetJPAExecutor(id));
                if (getActionInfo) {
                    int numAction = jpaService.execute(new CoordActionsCountForJobIdJPAExecutor(id));
                    List<CoordinatorActionBean> coordActions = null;
                    if (len == 0) {
                        coordActions = new ArrayList<CoordinatorActionBean>();
                    }
                    else {
                        coordActions = jpaService.execute(new CoordJobGetActionsSubsetJPAExecutor(id, filterMap, offset,
                                len, desc));
                    }
                    coordJob.setActions(coordActions);
                    coordJob.setNumActions(numAction);
                }
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            return coordJob;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}
