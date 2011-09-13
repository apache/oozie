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
package org.apache.oozie.servlet;

import org.apache.oozie.XException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.XLog;

import javax.servlet.ServletException;

/**
 * Specialized Oozie servlet exception that uses Oozie error codes. <p/> It extends ServletException so it can be
 * handled in the <code>Servlet.service</code> method of the {@link JsonRestServlet}.
 */
public class XServletException extends ServletException {
    private static final long serialVersionUID = 1L;
    private ErrorCode errorCode;
    private int httpStatusCode;

    /**
     * Create a DagXServletException that triggers a HTTP BAD_REQUEST (400).
     *
     * @param httpStatusCode HTTP error code to return.
     * @param ex cause
     */
    public XServletException(int httpStatusCode, XException ex) {
        super(ex.getMessage(), ex);
        this.errorCode = ex.getErrorCode();
        this.httpStatusCode = httpStatusCode;
    }

    /**
     * Create a XServletException that triggers a specified HTTP error code.
     *
     * @param httpStatusCode HTTP error code to return.
     * @param errorCode Oozie error code.
     * @param params paramaters to use in the error code template. If the last parameter is an Exception,
     */
    public XServletException(int httpStatusCode, ErrorCode errorCode, Object... params) {
        super(errorCode.format(params), XLog.getCause(params));
        this.errorCode = errorCode;
        this.httpStatusCode = httpStatusCode;
    }

    /**
     * Return the Oozie error code for the exception.
     *
     * @return error code for the exception.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Return the HTTP error code to return to the client.
     *
     * @return HTTP error code.
     */
    public int getHttpStatusCode() {
        return httpStatusCode;
    }

}
