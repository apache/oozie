package org.apache.oozie.command.coord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordChangeXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private boolean resetPauseTime = false;
    private final XLog LOG = XLog.getLog(CoordChangeXCommand.class);
    private final String changeValue;
    private CoordinatorJobBean coordJob;
    private JPAService jpaService = null;
    private final String name;

    /**
     * This command is used to update the Coordinator job with the new values Update the coordinator job bean and update
     * that to database.
     * 
     * @param id Coordinator job id.
     * @param changeValue This the changed value in the form key=value.
     */
    public CoordChangeXCommand(String id, String changeValue) {
        super("coord_change", "coord_change", 0);
        this.jobId = ParamChecker.notEmpty(id, "id");
        ParamChecker.notEmpty(changeValue, "value");
        this.changeValue = changeValue;
        this.name = "coord_change";
    }

    private void parseChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = new HashMap<String, String>();
        String[] tokens = changeValue.split(";");
        int size = tokens.length;

        if (size < 0 || size > 3) {
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
        }

        for (String token : tokens) {
            String[] pair = token.split("=");
            String key = pair[0];

            if (!key.equals(OozieClient.CHANGE_VALUE_ENDTIME) && !key.equals(OozieClient.CHANGE_VALUE_CONCURRENCY)
                    && !key.equals(OozieClient.CHANGE_VALUE_PAUSETIME)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key
                        + " must be name=value pair");
            }

            if (key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2 && pair.length != 1) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key
                        + " must be name=value pair or name=(empty string to reset pause time to null)");
            }

            if (map.containsKey(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "can not specify repeated change values on "
                        + key);
            }

            if (pair.length == 2) {
                map.put(key, pair[1]);
            }
            else {
                map.put(key, "");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            try {
                newEndTime = DateUtils.parseDateUTC(value);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_CONCURRENCY)) {
            String value = map.get(OozieClient.CHANGE_VALUE_CONCURRENCY);
            try {
                newConcurrency = Integer.parseInt(value);
            }
            catch (NumberFormatException ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid integer");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_PAUSETIME);
            if (value.equals("")) { // this is to reset pause time to null;
                resetPauseTime = true;
            }
            else {
                try {
                    newPauseTime = DateUtils.parseDateUTC(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
        parseChangeValue(this.changeValue);
    }

    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New endTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newEndTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newEndTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }
    }

    private void checkPauseTime(CoordinatorJobBean coordJob, Date newPauseTime, Date newEndTime)
            throws CommandException {
        // New pauseTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newPauseTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New pauseTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newPauseTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newPauseTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }

        // New pauseTime must be before coordinator job's end time.
        Date endTime = (newEndTime != null) ? newEndTime : coordJob.getEndTime();
        if (!newPauseTime.before(endTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "must be before coordinator job's end time ["
                    + endTime + "]");
        }
    }

    private void check(CoordinatorJobBean coordJob, Date newEndTime, Integer newConcurrency, Date newPauseTime)
            throws CommandException {
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
            throw new CommandException(ErrorCode.E1016);
        }

        if (newEndTime != null) {
            checkEndTime(coordJob, newEndTime);
        }

        if (newPauseTime != null) {
            checkPauseTime(coordJob, newPauseTime, newEndTime);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            LogUtils.setLogInfo(this.coordJob, logInfo);

            if (newEndTime != null) {
                this.coordJob.setEndTime(newEndTime);
                if (this.coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    this.coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }

            if (newConcurrency != null) {
                this.coordJob.setConcurrency(newConcurrency);
            }

            if (newPauseTime != null || resetPauseTime == true) {
                this.coordJob.setPauseTime(newPauseTime);
            }

            InstrumentUtils.incrJobCounter(name, 1, null);

            jpaService.execute(new CoordJobUpdateJPAExecutor(this.coordJob));

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);

        if (jpaService != null) {
            try {
                this.coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }
        else {
            LOG.error(ErrorCode.E0610);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        check(this.coordJob, newEndTime, newConcurrency, newPauseTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }
}
