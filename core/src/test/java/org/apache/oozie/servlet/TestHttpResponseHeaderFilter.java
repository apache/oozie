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

import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

public class TestHttpResponseHeaderFilter {

    @Test
    public void testXFrameOptionAgainstClickjackingAdded() throws Exception {
        ServletRequest request = Mockito.mock(ServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        doNothing().when(response).addHeader(any(String.class), any(String.class));

        final AtomicBoolean invoked = new AtomicBoolean();

        FilterChain chain = new FilterChain() {
            @Override
            public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse) {
                invoked.set(true);
            }
        };

        Filter filter = new HttpResponseHeaderFilter();
        filter.init(null);
        filter.doFilter(request, response, chain);
        verify( response).addHeader(HttpResponseHeaderFilter.X_FRAME_OPTIONS, HttpResponseHeaderFilter.X_FRAME_OPTION_DENY);
        assertTrue("Filter is not invoked", invoked.get());
        filter.destroy();
    }


}