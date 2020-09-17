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

package org.apache.oozie.test;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.FailingHSQLDBDriverWrapper;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * To test that when a JDBC driver fails, {@link JPAService} continues working:
 * <ul>
 *     <li>use the {@link java.sql.Connection} wrapper that is used by the wrapper extending {@link org.hsqldb.jdbcDriver},
 *     and set the system property {@link FailingHSQLDBDriverWrapper#USE_FAILING_DRIVER}</li>
 *     <li>initialize {@link JPAService} and a JPA {@link javax.persistence.EntityManagerFactory}</li>
 *     <li>issue a decent number of JPA queries w/ appropriate parallelism via an {@link java.util.concurrent.Executor}</li>
 *     <li>an {@link TypedQuery#getResultList()} resulting in a {@code SELECT … FROM WF_JOBS WHERE ...} Oozie database read</li>
 *     <li>followed by a {@link TypedQuery#executeUpdate()} resulting in an {@code UPDATE WF_ACTIONS SET … WHERE …} Oozie database
 *     write</li>
 * </ul>
 */
public class TestParallelJPAOperationRetries extends MiniOozieTestCase {
    private static final XLog LOG = XLog.getLog(TestParallelJPAOperationRetries.class);
    private static final String ORIGINAL_LOG4J_FILE = System.getProperty(XLogService.LOG4J_FILE);

    private volatile Exception executorException;

    @Override
    protected void setUp() throws Exception {
        executorException = null;
        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        System.setProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER, Boolean.TRUE.toString());

        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        if (ORIGINAL_LOG4J_FILE != null) {
            System.setProperty(XLogService.LOG4J_FILE, ORIGINAL_LOG4J_FILE);
        }
    }

    public void testParallelJPAOperationsOnWorkflowBeansRetryAndSucceed() throws Exception {
        final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
        final int totalTaskCount = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(totalTaskCount);

        for (int ixTask = 1; ixTask <= totalTaskCount; ixTask++) {
            final int taskCount = ixTask;
            taskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        LOG.debug("Task #{0} started.", taskCount);

                        getResultListAndExecuteUpdateOnWorkflowBeans();

                        LOG.debug("Task #{0} finished.", taskCount);
                    }
                    catch (final SQLException e) {
                        executorException = e;
                    }
                    finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }

        countDownLatch.await();

        if (executorException != null) {
            fail(String.format("Should not get an SQLException while executing SQL operations. [e.message=%s]",
                    executorException.getMessage()));
        }

        taskExecutor.shutdown();

        try {
            taskExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            fail("Should not get an interrupt while shutting down ExecutorService.");
        }
    }

    private void getResultListAndExecuteUpdateOnWorkflowBeans() throws SQLException {
        final JPAService jpaService = Services.get().get(JPAService.class);

        final int checkAgeSecs = 86_400;
        final Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);

        final EntityManager em = jpaService.getEntityManager();
        final TypedQuery<WorkflowActionBean> q = em.createNamedQuery("GET_RUNNING_ACTIONS", WorkflowActionBean.class);
        q.setParameter("lastCheckTime", ts);
        final List<WorkflowActionBean> actions = q.getResultList();

        assertEquals("no WorkflowActionBeans should be present", 0, actions.size());

        if (!em.getTransaction().isActive()) {
            em.getTransaction().begin();
        }

        final int rowsAffected = em.createNamedQuery("UPDATE_ACTION_FOR_LAST_CHECKED_TIME")
                .setParameter("lastCheckTime", 0l)
                .setParameter("id", "666")
                .executeUpdate();
        if (em.getTransaction().isActive()) {
            em.getTransaction().commit();
        }

        assertEquals("no rows should be affected when updating WorkflowActionBeans", 0, rowsAffected);
    }
}
