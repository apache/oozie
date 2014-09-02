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

package org.apache.oozie;

import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;

/**
 * Base exception for all Oozie exception. <p/> It requires error codes an captures the Log info at exception time. <p/>
 * Error codes should be modeled in subclasses as Enums.
 */
public class XException extends Exception {
    private ErrorCode errorCode;

    /**
     * Private constructor use by the public constructors.
     *
     * @param message error message.
     * @param errorCode error code.
     * @param cause exception cause.
     */
    private XException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = ParamChecker.notNull(errorCode, "errorCode");
    }

    /**
     * Create an XException from a XException.
     *
     * @param cause the XException cause.
     */
    public XException(XException cause) {
        this(cause.getMessage(), cause.getErrorCode(), cause);
    }

    /**
     * Create an EXception from an error code plus parameter to create the exception message. <p/> The value of {@link
     * ErrorCode#getTemplate} is used as a StringFormat template for the exception message. <p/> If the last parameter
     * is an Exception it is used as the exception cause.
     *
     * @param errorCode the error code for the exception.
     * @param params parameters used to create the exception message together with the error code template. If the last
     * parameter is an Exception it is used as the exception cause.
     */
    public XException(ErrorCode errorCode, Object... params) {
        this(errorCode.format(params), errorCode, XLog.getCause(params));
    }

    /**
     * Return the error code of the exception.
     *
     * @return exception error code.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

}
