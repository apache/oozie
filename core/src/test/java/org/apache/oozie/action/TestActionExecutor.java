/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.client.WorkflowAction;

import java.io.IOException;
import java.rmi.RemoteException;

public class TestActionExecutor extends XTestCase {

    private static class MyActionExecutor extends ActionExecutor {

        protected MyActionExecutor() {
            super("type");
        }

        public void initActionType() {
            super.initActionType();
            registerError("java.rmi.RemoteException", ActionExecutorException.ErrorType.NON_TRANSIENT, "RMI");
            registerError("java.io.IOException", ActionExecutorException.ErrorType.TRANSIENT, "IO");
            registerError("foo.Exception", ActionExecutorException.ErrorType.TRANSIENT, "FO");
        }

        protected MyActionExecutor(int maxRetries, int retryInterval) {
            super("type", maxRetries, retryInterval);
        }

        public void start(Context context, WorkflowAction action) throws ActionExecutorException {
            assertEquals("type", getType());
            assertEquals(ActionExecutor.MAX_RETRIES, getMaxRetries());
            assertEquals(ActionExecutor.RETRY_INTERVAL, getRetryInterval());
        }

        public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public void check(Context context, WorkflowAction action) throws ActionExecutorException {
            assertEquals("type", getType());
            assertEquals(1, getMaxRetries());
            assertEquals(2, getRetryInterval());
        }

        public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public boolean isCompleted(String externalStatus) {
            return true;
        }
    }

    public void testActionExecutor() throws Exception {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor ae = new MyActionExecutor();
        ae.initActionType();
        ActionExecutor.disableInit();

        ae.start(null, null);

        ae = new MyActionExecutor(1, 2);

        ae.check(null, null);

        Exception cause = new IOException();
        try {
            throw ae.convertException(cause);
        }
        catch (ActionExecutorException ex) {
            assertEquals(cause, ex.getCause());
            assertEquals(ActionExecutorException.ErrorType.TRANSIENT, ex.getErrorType());
            assertEquals("IO", ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        cause = new RemoteException();
        try {
            throw ae.convertException(cause);
        }
        catch (ActionExecutorException ex) {
            assertEquals(cause, ex.getCause());
            assertEquals(ActionExecutorException.ErrorType.NON_TRANSIENT, ex.getErrorType());
            assertEquals("RMI", ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        cause = new RuntimeException();
        try {
            throw ae.convertException(cause);
        }
        catch (ActionExecutorException ex) {
            assertEquals(cause, ex.getCause());
            assertEquals(ActionExecutorException.ErrorType.ERROR, ex.getErrorType());
            assertEquals("RuntimeException", ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        cause = new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "x", "x");
        try {
            throw ae.convertException(cause);
        }
        catch (ActionExecutorException ex) {
            assertEquals(cause, ex);
        }
        catch (Exception ex) {
            fail();
        }

    }
}
