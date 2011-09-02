package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of running actions for a coordinator job.
 */
public class CoordActionsActiveCountJPAExecutor implements JPAExecutor<Integer> {

    private String coordJobId = null;

    public CoordActionsActiveCountJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    @Override
    public String getName() {
        return "CoordActionsActiveCountJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIVE_ACTIONS_COUNT_BY_JOBID");

            q.setParameter("jobId", coordJobId);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}