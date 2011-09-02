package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of actions for a bundle job.
 */
public class BundleActionsCountForJobGetJPAExecutor implements JPAExecutor<Integer> {

    private String bundleJobId = null;

    public BundleActionsCountForJobGetJPAExecutor(String bundleJobId) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        this.bundleJobId = bundleJobId;
    }

    @Override
    public String getName() {
        return "BundleActionsCountForJobGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_COUNT_BY_JOB");

            q.setParameter("bundleId", bundleJobId);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}