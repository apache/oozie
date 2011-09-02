package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of actions for a bundle job which are not terminate status ('PERP' OR 'RUNNING' OR 'SUSPENDED' OR
 * 'PREPSUSPENDED' OR 'PAUSED' OR 'PREPPAUSED').
 */
public class BundleActionsNotTerminateStatusCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String bundleJobId = null;

    public BundleActionsNotTerminateStatusCountGetJPAExecutor(String bundleJobId) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        this.bundleJobId = bundleJobId;
    }

    @Override
    public String getName() {
        return "BundleActionsNotTerminateStatusCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_NOT_TERMINATE_STATUS_COUNT");
            q.setParameter("bundleId", bundleJobId);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}