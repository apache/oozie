package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of pending actions for a bundle job.
 */
public class BundleActionsPendingTrueCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String bundleJobId = null;

    public BundleActionsPendingTrueCountGetJPAExecutor(String bundleJobId) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        this.bundleJobId = bundleJobId;
    }

    @Override
    public String getName() {
        return "BundleActionsPendingTrueCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_PENDING_TRUE_COUNT");

            q.setParameter("bundleId", bundleJobId);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}