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

package org.apache.oozie.client;

/**
 * Exception thrown by the {@link OozieClient}.
 */
public class OozieClientException extends Exception {
    public static final String UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION";
    public static final String IO_ERROR = "IO_ERROR";
    public static final String INVALID_FILTER = "INVALID_FILTER";
    public static final String INVALID_INPUT = "INVALID_INPUT";
    public static final String OTHER = "OTHER";
    public static final String AUTHENTICATION = "AUTHENTICATION";

    private String errorCode;

    /**
     * Create an exception.
     *
     * @param errorCode error code.
     * @param message error message.
     */
    public OozieClientException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Create an exception with a cause.
     *
     * @param errorCode error code.
     * @param cause exception cause.
     */
    public OozieClientException(String errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    /**
     * Create an exception with a cause.
     *
     * @param errorCode error code.
     * @param message error message.
     * @param cause exception cause.
     */
    public OozieClientException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Return the exception error code.
     *
     * @return the exception error code.
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Return the string representatio of the exception.
     *
     * @return the string representatio of the exception.
     */
    public String toString() {
        return errorCode + " : " + super.getMessage();
    }

}
