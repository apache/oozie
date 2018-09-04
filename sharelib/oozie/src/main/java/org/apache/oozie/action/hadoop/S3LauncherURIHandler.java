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

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.List;

public class S3LauncherURIHandler implements LauncherURIHandler
{
    // TODO
    @Override
    public boolean create(URI uri, Configuration conf)
        throws LauncherException
    {
        throw new UnsupportedOperationException("Creating directory at is not supported for " + uri);
    }

    // TODO
    @Override
    public boolean delete(URI uri, Configuration conf)
        throws LauncherException
    {
        throw new UnsupportedOperationException("Deleting directory at is not supported for " + uri);
    }

    @Override
    public List<Class<?>> getClassesForLauncher()
    {
        return null;
    }
}
