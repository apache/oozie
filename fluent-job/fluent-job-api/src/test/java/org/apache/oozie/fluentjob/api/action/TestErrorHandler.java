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

package org.apache.oozie.fluentjob.api.action;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestErrorHandler {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNameIsCorrect() {
        final String name = "error-handler";
        final Builder<MapReduceAction> handlerBuilder = MapReduceActionBuilder.create().withName(name);

        final ErrorHandler errorHandler = ErrorHandler.buildAsErrorHandler(handlerBuilder);
        assertEquals(name, errorHandler.getName());
    }

    @Test
    public void testIfThereAreParentsThenThrows() {
        final String name = "error-handler";
        final Node parent = MapReduceActionBuilder.create().withName("parent").build();
        final Builder<MapReduceAction> handlerBuilder = MapReduceActionBuilder.create()
                .withName(name)
                .withParent(parent);

        expectedException.expect(IllegalStateException.class);
        ErrorHandler.buildAsErrorHandler(handlerBuilder);
    }
}