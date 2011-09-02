package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of pending actions for a coordinator job.
 */
public class CoordActionsPendingFalseCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String coordJobId = null;

    public CoordActionsPendingFalseCountGetJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    @Override
    public String getName() {
        return "CoordActionsPendingFalseCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_PENDING_FALSE_COUNT");

            q.setParameter("jobId", coordJobId);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}