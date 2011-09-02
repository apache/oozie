package org.apache.oozie.command.coord;

import org.apache.oozie.command.XCommand;

/**
 * Abstract coordinator command class derived from XCommand
 */
public abstract class CoordinatorXCommand<T> extends XCommand<T> {

    /**
     * Base class constructor for coordinator commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     */
    public CoordinatorXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * Base class constructor for coordinator commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     * @param dryrun true if rerun is enabled for command
     */
    public CoordinatorXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

}
