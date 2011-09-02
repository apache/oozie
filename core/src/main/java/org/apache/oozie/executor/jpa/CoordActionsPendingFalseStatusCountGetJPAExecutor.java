package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of pending actions for a status for a coordinator job.
 */
public class CoordActionsPendingFalseStatusCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String coordJobId = null;
    private String status = null;

    public CoordActionsPendingFalseStatusCountGetJPAExecutor(String coordJobId, String status) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        ParamChecker.notNull(status, "status");
        this.coordJobId = coordJobId;
        this.status = status;
    }

    @Override
    public String getName() {
        return "CoordActionsPendingFalseStatusCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_PENDING_FALSE_STATUS_COUNT");
            q.setParameter("jobId", coordJobId);
            q.setParameter("status", status);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}