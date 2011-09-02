package org.apache.maven.doxia.sink;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author <a href="mailto:trygvis@inamo.no">Trygve Laugst&oslash;l</a>
 * @version $Id: PipelineSink.java 569044 2007-08-23 15:31:11Z jdcasey $
 */
public class PipelineSink
    implements InvocationHandler
{
    private List pipeline;

    public PipelineSink( List pipeline )
    {
        this.pipeline = pipeline;
    }

    public void addSink( Sink sink )
    {
        pipeline.add( sink );
    }

    public Object invoke( Object proxy, Method method, Object[] args )
        throws Throwable
    {
        for ( Iterator it = pipeline.iterator(); it.hasNext(); )
        {
            Sink sink = (Sink) it.next();

            method.invoke( sink, args );
        }

        return null;
    }

    public static Sink newInstance( List pipeline )
    {
        return (Sink) Proxy.newProxyInstance( PipelineSink.class.getClassLoader(),
                                              new Class[]{Sink.class},
                                              new PipelineSink( pipeline ) );
    }
}
