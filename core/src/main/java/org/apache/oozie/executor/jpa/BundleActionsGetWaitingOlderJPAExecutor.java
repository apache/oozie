package org.apache.oozie.executor.jpa;

import java.sql.Timestamp;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

public class BundleActionsGetWaitingOlderJPAExecutor implements JPAExecutor<List<BundleActionBean>>{
    private long checkAgeSecs = 0;

    public BundleActionsGetWaitingOlderJPAExecutor(final long checkAgeSecs) {
        ParamChecker.notNull(checkAgeSecs, "checkAgeSecs");
        this.checkAgeSecs = checkAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BundleActionsGetWaitingOlderJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public List<BundleActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<BundleActionBean> actions;
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN");
            Timestamp ts = new Timestamp(System.currentTimeMillis() - this.checkAgeSecs * 1000);
            q.setParameter("lastModifiedTime", ts);
            actions = q.getResultList();
            return actions;
        }
        catch (IllegalStateException e) {
            throw new JPAExecutorException(ErrorCode.E0601, e.getMessage(), e);
        }
    }
}
