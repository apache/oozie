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

package org.apache.oozie.command;

import com.google.common.annotations.VisibleForTesting;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.executor.jpa.BundleJobsDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsGetFromCoordJobIdJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsCountNotForPurgeFromParentIdJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetFromParentIdJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.executor.jpa.WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsGetForPurgeJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.eclipse.jgit.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class is used to purge workflows, coordinators, and bundles.  It takes into account the relationships between workflows and
 * coordinators, and coordinators and bundles.  It also only acts on 'limit' number of items at a time to not overtax the DB and in
 * case something gets rolled back.  Also, children are always deleted before their parents in case of a rollback.
 */
public class PurgeXCommand extends XCommand<Void> {
    private JPAService jpaService = null;
    private int wfOlderThan;
    private int coordOlderThan;
    private int bundleOlderThan;
    private boolean purgeOldCoordAction = false;
    private final int limit;
    private List<String> wfList;
    private List<String> coordActionList;
    private List<String> coordList;
    private List<String> bundleList;
    private int wfDel;
    private int coordDel;
    private int coordActionDel;
    private int bundleDel;
    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

    interface JPAFunction<T, R> {
        R apply(T t) throws JPAExecutorException;
    }

    final JPAFunction<String, List<WorkflowJobBean>> getSubWorkflowJobBeansFunction = new JPAFunction<String,
            List<WorkflowJobBean>>() {
        @Override
        public List<WorkflowJobBean> apply(String wfId) throws JPAExecutorException {
            return PurgeXCommand.this.getSubWorkflowJobBeans(wfId);
        }
    };

    final JPAFunction<List<WorkflowJobBean>, List<String>> fetchTerminatedWorflowFunction = new JPAFunction<List<WorkflowJobBean>,
            List<String>>() {
        @Override
        public List<String> apply(List<WorkflowJobBean> wfBeanList) throws JPAExecutorException {
            return PurgeXCommand.this.fetchTerminatedWorkflow(wfBeanList);
        }
    };

    @VisibleForTesting
    static class SelectorTreeTraverser<T, U> {
        final T rootNode;
        final JPAFunction<T, List<U>> childrenFinder;
        final JPAFunction<List<U>, List<T>> selector;

        SelectorTreeTraverser(final T rootNode, final JPAFunction<T, List<U>> childrenFinder,
                              final JPAFunction<List<U>, List<T>> selector) {
            this.rootNode = rootNode;
            this.childrenFinder = childrenFinder;
            this.selector = selector;
        }

        List<T> findAllDescendantNodesIfSelectable() throws JPAExecutorException {
            List<T> allDescendantNodes = new ArrayList<>();
            Set<T> uniqueDescendantNodes = new HashSet<>();
            allDescendantNodes.add(rootNode);
            uniqueDescendantNodes.add(rootNode);
            int nextIndexToCheck = 0;
            while (nextIndexToCheck < allDescendantNodes.size()) {
                T id = allDescendantNodes.get(nextIndexToCheck);
                List<U> childrenNodes = childrenFinder.apply(id);
                List<T> selectedChildren = selector.apply(childrenNodes);
                if (selectedChildren.size() == childrenNodes.size()) {
                    allDescendantNodes.addAll(selectedChildren);
                    uniqueDescendantNodes.addAll(selectedChildren);
                    if (allDescendantNodes.size() != uniqueDescendantNodes.size()) {
                        throw new JPAExecutorException(ErrorCode.E0613, rootNode);
                    }
                }
                else {
                    return new ArrayList<>();
                }
                ++nextIndexToCheck;
            }
            return allDescendantNodes;
        }
    }

    public PurgeXCommand(int wfOlderThan, int coordOlderThan, int bundleOlderThan, int limit) {
        this(wfOlderThan, coordOlderThan, bundleOlderThan, limit, false);
    }

    public PurgeXCommand(int wfOlderThan, int coordOlderThan, int bundleOlderThan, int limit, boolean purgeOldCoordAction) {
        super("purge", "purge", 0);
        this.wfOlderThan = wfOlderThan;
        this.coordOlderThan = coordOlderThan;
        this.bundleOlderThan = bundleOlderThan;
        this.purgeOldCoordAction = purgeOldCoordAction;
        this.limit = limit;
        wfList = new ArrayList<String>();
        coordActionList = new ArrayList<String>();
        coordList = new ArrayList<String>();
        bundleList = new ArrayList<String>();
        wfDel = 0;
        coordDel = 0;
        coordActionDel = 0;
        bundleDel = 0;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                // Get the lists of workflows, coordinators, and bundles that can be purged (and have no parents)
                int size;
                do {
                    size = wfList.size();
                    wfList.addAll(jpaService.execute(new WorkflowJobsGetForPurgeJPAExecutor(wfOlderThan, wfList.size(), limit)));
                } while(size != wfList.size());
                if (purgeOldCoordAction) {
                    LOG.debug("Purging workflows of long running coordinators is turned on");
                    do {
                        size = coordActionList.size();
                        long olderThan = wfOlderThan;
                        List<WorkflowJobBean> jobBeans = WorkflowJobQueryExecutor.getInstance().getList(
                                WorkflowJobQuery.GET_COMPLETED_COORD_WORKFLOWS_OLDER_THAN, olderThan,
                                coordActionList.size(), limit);
                        for (WorkflowJobBean bean : jobBeans) {
                            coordActionList.add(bean.getParentId());
                            wfList.add(bean.getId());
                        }
                    } while(size != coordActionList.size());
                }
                do {
                    size = coordList.size();
                    coordList.addAll(jpaService.execute(
                            new CoordJobsGetForPurgeJPAExecutor(coordOlderThan, coordList.size(), limit)));
                } while(size != coordList.size());
                do {
                    size = bundleList.size();
                    bundleList.addAll(jpaService.execute(
                            new BundleJobsGetForPurgeJPAExecutor(bundleOlderThan, bundleList.size(), limit)));
                } while(size != bundleList.size());
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED Purge to purge Workflow Jobs older than [{0}] days, Coordinator Jobs older than [{1}] days, and Bundle"
                + " jobs older than [{2}] days.", wfOlderThan, coordOlderThan, bundleOlderThan);

        // Process parentless workflows to purge them and their children
        if (!wfList.isEmpty()) {
            try {
                processWorkflows(wfList);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }

        // Process coordinator actions of long running coordinators and purge them
        if (!coordActionList.isEmpty()) {
            try {
                purgeCoordActions(coordActionList);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }
        // Processs parentless coordinators to purge them and their children
        if (!coordList.isEmpty()) {
            try {
                processCoordinators(coordList);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }

        // Process bundles to purge them and their children
        if (!bundleList.isEmpty()) {
            try {
                processBundles(bundleList);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }

        LOG.info("ENDED Purge deleted [{0}] workflows, [{1}] coordinatorActions, [{2}] coordinators, [{3}] bundles",
                wfDel, coordActionDel, coordDel, bundleDel);
        return null;
    }

    /**
     * Process workflows to purge them and their children if all the descendants are purgeable. Skip the workflows that have
     * non-purgeable descendants.
     *
     * @param wfs List of workflows to process
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void processWorkflows(List<String> wfs) throws JPAExecutorException {
        List<String> wfsToPurge = findPurgeableWorkflows(wfs);
        purgeWorkflows(wfsToPurge);
    }

    /**
     * Get purgeable workflow list.
     *
     * @param workflows List of workflows to process
     * @return List of workflows to purge
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private List<String> findPurgeableWorkflows(List<String> workflows) throws JPAExecutorException {
        List<String> purgeableWorkflows = new ArrayList<>();
        for (String workflowId : workflows) {
            SelectorTreeTraverser<String, WorkflowJobBean> selectorTreeTraverser = new SelectorTreeTraverser<>(workflowId,
                    getSubWorkflowJobBeansFunction, fetchTerminatedWorflowFunction);
            purgeableWorkflows.addAll(selectorTreeTraverser.findAllDescendantNodesIfSelectable());
        }
        return purgeableWorkflows;
    }



    private List<WorkflowJobBean> getSubWorkflowJobBeans(String wfId) throws JPAExecutorException {
        int size;
        List<WorkflowJobBean> swfBeanList = new ArrayList<>();
        do {
            size = swfBeanList.size();
            swfBeanList.addAll(jpaService.execute(
                    new WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(wfId, swfBeanList.size(), limit)));
        } while (size != swfBeanList.size());
        return swfBeanList;
    }

    /**
     * This method will return all terminate workflow ids from wfBeanlist for purge.
     * @param wfBeanList
     * @return workflows to purge
     */
    private List<String> fetchTerminatedWorkflow(List<WorkflowJobBean> wfBeanList) {
        List<String> children = new ArrayList<String>();
        long wfOlderThanMS = System.currentTimeMillis() - (wfOlderThan * DAY_IN_MS);
        for (WorkflowJobBean wfjBean : wfBeanList) {
            if (isWorkflowPurgeable(wfjBean, wfOlderThanMS)) {
                children.add(wfjBean.getId());
            }
        }
        return children;
    }

    private boolean isWorkflowPurgeable(WorkflowJobBean wfjBean, long wfOlderThanMS) {
        final Date wfEndTime = wfjBean.getEndTime();
        final boolean isFinished = wfjBean.inTerminalState();
        if (isFinished && wfEndTime != null && wfEndTime.getTime() < wfOlderThanMS) {
            return true;
        }
        else {
            final Date lastModificationTime = wfjBean.getLastModifiedTime();
            if (isFinished && lastModificationTime != null && lastModificationTime.getTime() < wfOlderThanMS) {
                return true;
            }
        }
        return false;
    }

    /**
     * Process coordinators to purge them and their children.
     *
     * @param coords List of coordinators to process
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void processCoordinators(List<String> coords) throws JPAExecutorException {
        List<String> wfsToPurge = new ArrayList<String>();
        List<String> actionsToPurge = new ArrayList<String>();
        List<String> coordsToPurge = new ArrayList<String>();
        for (String coordId : coords) {
            // Get all of the direct workflowChildren for this coord
            List<WorkflowJobBean> wfjBeanList = new ArrayList<WorkflowJobBean>();
            int size;
            do {
                size = wfjBeanList.size();
                wfjBeanList.addAll(jpaService.execute(
                        new WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor(coordId, wfjBeanList.size(), limit)));
            } while (size != wfjBeanList.size());

            // Checking if workflow is ready to purge
            List<String> workflowChildren = fetchTerminatedWorkflow(wfjBeanList);

            // if all workflow are ready to purge add them and add the coordinator and their actions
            if(workflowChildren.size() == wfjBeanList.size()) {
                LOG.debug("Purging coordinator " + coordId);
                wfsToPurge.addAll(workflowChildren);
                coordsToPurge.add(coordId);
                // Get all of the direct actionChildren for this coord
                List<String> actionChildren = new ArrayList<String>();
                do {
                    size = actionChildren.size();
                    actionChildren.addAll(jpaService.execute(
                            new CoordActionsGetFromCoordJobIdJPAExecutor(coordId, actionChildren.size(), limit)));
                } while (size != actionChildren.size());
                actionsToPurge.addAll(actionChildren);
            }
        }
        // Process the children workflow
        processWorkflows(wfsToPurge);
        // Process the children action
        purgeCoordActions(actionsToPurge);
        // Now that all children have been purged, we can purge the coordinators
        purgeCoordinators(coordsToPurge);
    }

    /**
     * Process bundles to purge them and their children
     *
     * @param bundles List of bundles to process
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void processBundles(List<String> bundles) throws JPAExecutorException {
        List<String> coordsToPurge = new ArrayList<String>();
        List<String> bundlesToPurge = new ArrayList<String>();
        for (Iterator<String> it = bundles.iterator(); it.hasNext(); ) {
            String bundleId = it.next();
            // We only purge the bundle and its children if they are all ready to be purged
            long numChildrenNotReady = jpaService.execute(
                    new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(coordOlderThan, bundleId));
            if (numChildrenNotReady == 0) {
                bundlesToPurge.add(bundleId);
                LOG.debug("Purging bundle " + bundleId);
                // Get all of the direct children for this bundle
                List<String> children = new ArrayList<String>();
                int size;
                do {
                    size = children.size();
                    children.addAll(jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleId, children.size(), limit)));
                } while (size != children.size());
                coordsToPurge.addAll(children);
            }
        }
        // Process the children
        processCoordinators(coordsToPurge);
        // Now that all children have been purged, we can purge the bundles
        purgeBundles(bundlesToPurge);
    }

    /**
     * Purge the workflows in REVERSE order in batches of size 'limit' (this must be done in reverse order so that children are
     * purged before their parents)
     *
     * @param wfs List of workflows to purge
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void purgeWorkflows(List<String> wfs) throws JPAExecutorException {
        wfDel += wfs.size();
        //To delete sub-workflows before deleting parent workflows
        Collections.reverse(wfs);
        for (int startIndex = 0; startIndex < wfs.size(); ) {
            int endIndex = (startIndex + limit < wfs.size()) ? (startIndex + limit) : wfs.size();
            List<String> wfsForDelete = wfs.subList(startIndex, endIndex);
            LOG.debug("Deleting workflows: " + StringUtils.join(wfsForDelete, ","));
            jpaService.execute(new WorkflowJobsDeleteJPAExecutor(wfsForDelete));
            startIndex = endIndex;
        }
    }

    /**
     * Purge coordActions of long running coordinators and purge them
     *
     * @param coordActions List of coordActions to purge
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void purgeCoordActions(List<String> coordActions) throws JPAExecutorException {
        coordActionDel += coordActions.size();
        for (int startIndex = 0; startIndex < coordActions.size(); ) {
            int endIndex = (startIndex + limit < coordActions.size()) ? (startIndex + limit) : coordActions.size();
            List<String> coordActionsForDelete = coordActions.subList(startIndex, endIndex);
            LOG.debug("Deleting coordinator actions: " + StringUtils.join(coordActionsForDelete, ","));
            jpaService.execute(new CoordActionsDeleteJPAExecutor(coordActionsForDelete));
            startIndex = endIndex;
        }
    }
    /**
     * Purge the coordinators in SOME order in batches of size 'limit' (its in reverse order only for convenience)
     *
     * @param coords List of coordinators to purge
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void purgeCoordinators(List<String> coords) throws JPAExecutorException {
        coordDel += coords.size();
        for (int startIndex = 0; startIndex < coords.size(); ) {
            int endIndex = (startIndex + limit < coords.size()) ? (startIndex + limit) : coords.size();
            List<String> coordsForDelete = coords.subList(startIndex, endIndex);
            LOG.debug("Deleting coordinators: " + StringUtils.join(coordsForDelete, ","));
            jpaService.execute(new CoordJobsDeleteJPAExecutor(coordsForDelete));
            startIndex = endIndex;
        }
    }

    /**
     * Purge the bundles in SOME order in batches of size 'limit' (its in reverse order only for convenience)
     *
     * @param bundles List of bundles to purge
     * @throws JPAExecutorException If a JPA executor has a problem
     */
    private void purgeBundles(List<String> bundles) throws JPAExecutorException {
        bundleDel += bundles.size();
        for (int startIndex = 0; startIndex < bundles.size(); ) {
            int endIndex = (startIndex + limit < bundles.size()) ? (startIndex + limit) : bundles.size();
            Collection<String> bundlesForDelete = bundles.subList(startIndex, endIndex);
            LOG.debug("Deleting bundles: " + StringUtils.join(bundlesForDelete, ","));
            jpaService.execute(new BundleJobsDeleteJPAExecutor(bundlesForDelete));
            startIndex = endIndex;
        }
    }

    @Override
    public String getEntityKey() {
        return "purge_command";
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
