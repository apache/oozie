package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorJob into a Bean and persist it.
 */
public class CoordJobUpdateJPAExecutor implements JPAExecutor<Void> {

    private CoordinatorJobBean coordJob = null;

    /**
     * @param coordJob
     */
    public CoordJobUpdateJPAExecutor(CoordinatorJobBean coordJob) {
        ParamChecker.notNull(coordJob, "CoordinatorJobBean");
        this.coordJob = coordJob;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            em.merge(coordJob);
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordinatorUpdateJobJPAExecutor";
    }

}
