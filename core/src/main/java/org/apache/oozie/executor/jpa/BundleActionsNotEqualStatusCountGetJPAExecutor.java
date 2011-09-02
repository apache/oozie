package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of actions for a bundle job which are not equal a given status.
 */
public class BundleActionsNotEqualStatusCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String bundleJobId = null;
    private String status = null;

    public BundleActionsNotEqualStatusCountGetJPAExecutor(String bundleJobId, String status) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        ParamChecker.notNull(status, "status");
        this.bundleJobId = bundleJobId;
        this.status = status;
    }

    @Override
    public String getName() {
        return "BundleActionsNotEqualStatusCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_NOT_EQUAL_STATUS_COUNT");
            q.setParameter("bundleId", bundleJobId);
            q.setParameter("status", status);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

}