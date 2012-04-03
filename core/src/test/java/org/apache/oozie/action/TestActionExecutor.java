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

import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.client.WorkflowAction;

import java.io.IOException;
import java.rmi.RemoteException;

public class TestActionExecutor extends XTestCase {

    private static class MyActionExecutor extends ActionExecutor {

		private int maxRetries;

        protected MyActionExecutor() {
            super("type");
            this.maxRetries = getMaxRetries();
        }

        public void initActionType() {
            super.initActionType();
            registerError("java.rmi.RemoteException", ActionExecutorException.ErrorType.NON_TRANSIENT, "RMI");
            registerError("java.io.IOException", ActionExecutorException.ErrorType.TRANSIENT, "IO");
            registerError("foo.Exception", ActionExecutorException.ErrorType.TRANSIENT, "FO");
        }

        protected MyActionExecutor(int maxRetries, int retryInterval) {
            super("type", retryInterval);
            super.setMaxRetries(maxRetries);
            this.maxRetries = maxRetries;
        }

        public void start(Context context, WorkflowAction action) throws ActionExecutorException {
            assertEquals("type", getType());
            assertEquals(this.maxRetries, getMaxRetries());
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

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		new Services().init();
	}

	@Override
	protected void tearDown() throws Exception {
		Services.get().destroy();
		super.tearDown();
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
