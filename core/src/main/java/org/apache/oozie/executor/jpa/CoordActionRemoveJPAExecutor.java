package org.apache.oozie.executor.jpa;

import javax.persistence.EntityManager;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorAction into a Bean and persist it.
 */
public class CoordActionRemoveJPAExecutor implements JPAExecutor<Void> {

    // private CoordinatorActionBean coordAction = null;
    private String coordActionId = null;

    /**
     * This constructs the object to Update the CoordinatorAction into a Bean and persist it.
     * 
     * @param coordAction
     */
    public CoordActionRemoveJPAExecutor(String coordActionId) {
        ParamChecker.notNull(coordActionId, "coordActionId");
        this.coordActionId = coordActionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            CoordinatorActionBean action = em.find(CoordinatorActionBean.class, coordActionId);
            if (action != null) {
                em.remove(action);
            }
            else {
                throw new CommandException(ErrorCode.E0605, coordActionId);
            }

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
        return "CoordActionRemoveJPAExecutor";
    }
}