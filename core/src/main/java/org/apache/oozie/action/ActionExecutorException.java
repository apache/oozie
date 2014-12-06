/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action;

import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * ActionExecutor exception. <p/> The exception provides information regarding the transient/no-transient/fatal nature
 * of the exception.
 */
public class ActionExecutorException extends Exception {

    /**
     * Enum that defines the type of error an {@link ActionExecutor} has produced.
     */
    public static enum ErrorType {

        /**
         * The action will be automatically retried by Oozie.
         */
        TRANSIENT,

        /**
         * The job in set in SUSPEND mode and it will wait for an admin to resume the job.
         */

        NON_TRANSIENT,

        /**
         * The action completes with an error transition.
         */
        ERROR,

        /**
         * The action fails. No transition is taken.
         */
        FAILED
    }

    private ErrorType errorType;
    private String errorCode;

    /**
     * Create an action executor exception.
     *
     * @param errorType the error type.
     * @param errorCode the error code.
     * @param message the error message.
     */
    public ActionExecutorException(ErrorType errorType, String errorCode, String message) {
        super(message);
        this.errorType = ParamChecker.notNull(errorType, "errorType");
        this.errorCode = ParamChecker.notEmpty(errorCode, "errorCode");
    }

    /**
     * Create an action executor exception.
     *
     * <p/> If the last parameter is an Exception it is used as the exception cause.
     *
     * @param errorType the error type.
     * @param errorCode the error code.
     * @param messageTemplate the error message.
     * @param params parameters used to create the exception message together with the messageTemplate. If the last
     * parameter is an Exception it is used as the exception cause.
     */
    public ActionExecutorException(ErrorType errorType, String errorCode, String messageTemplate, Object... params) {
        super(errorCode + ": " + XLog.format(messageTemplate, params), XLog.getCause(params));
        this.errorType = ParamChecker.notNull(errorType, "errorType");
        this.errorCode = ParamChecker.notEmpty(errorCode, "errorCode");
    }

    /**
     * Return the error type of the exception.
     *
     * @return the error type of the exception.
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * Return the error code of the exception.
     *
     * @return the error code of the exception.
     */
    public String getErrorCode() {
        return errorCode;
    }
}
