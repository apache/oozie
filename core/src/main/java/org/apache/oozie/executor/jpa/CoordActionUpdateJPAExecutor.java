package org.apache.oozie.executor.jpa;

import java.util.Date;

import javax.persistence.EntityManager;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorAction into a Bean and persist it
 */
public class CoordActionUpdateJPAExecutor implements JPAExecutor<Void> {

    private CoordinatorActionBean coordAction = null;

    /**
     * Create the object for CoordActionUpdateJPAExecutor to update the CoordinatorAction into a Bean and persist it
     * 
     * @param coordAction
     */
    public CoordActionUpdateJPAExecutor(CoordinatorActionBean coordAction) {
        ParamChecker.notNull(coordAction, "coordAction");
        this.coordAction = coordAction;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            coordAction.setLastModifiedTime(new Date());
            em.merge(coordAction);
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionUpdateJPAExecutor";
    }
}