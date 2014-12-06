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

package org.apache.oozie.dependency;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;

public class URIHandlerException extends XException {

    /**
     * Create an URIHandlerException Exception exception from a XException.
     *
     * @param cause the XException cause.
     */
    public URIHandlerException(XException cause) {
        super(cause);
    }

    /**
     * Create a URIHandlerException exception.
     *
     * @param errorCode error code.
     * @param params parameters for the error code message template.
     */
    public URIHandlerException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
