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
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.client.WorkflowAction;
import org.jdom.Element;

public abstract class Credentials {

    /**
     * This is the interface for all the Credentials implementation. Any new credential implementaion must implement 
     * this function. This function should modify the jobconf which will be used further to pass the credentials
     * to the tasks while running it. Creentials properties and context is also provided by that user can get all the 
     * necessary configuration.  
     * @param jobconf
     * @param props
     * @throws Exception 
     */
    public abstract void addtoJobConf(JobConf jobconf, CredentialsProperties props,Context context) throws Exception;
}
