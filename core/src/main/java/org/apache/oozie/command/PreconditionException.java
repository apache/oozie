package org.apache.oozie.command;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;

public class PreconditionException extends XException{
    /**
     * Create a verifyXCommand exception for verify conditions purposes.
     *
     * @param cause the XException cause.
     */
    public PreconditionException(XException cause) {
        super(cause);
    }

    /**
     * Create a verifyXCommand exception for verify conditions purposes.
     *
     * @param errorCode error code.
     * @param params parameters for the error code message template.
     */
    public PreconditionException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
