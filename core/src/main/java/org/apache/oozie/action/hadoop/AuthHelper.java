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

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

//TODO this class can go away once we use only 20.100+
public class AuthHelper {
    private static Class<? extends AuthHelper> KLASS = null;

    @SuppressWarnings("unchecked")
    public synchronized static AuthHelper get() {
        if (KLASS == null) {
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            if (has.getClass() ==  HadoopAccessorService.class) {
                KLASS = AuthHelper.class;
            }
            else {
                try {
                    KLASS = (Class<? extends AuthHelper>) Class
                            .forName("org.apache.oozie.action.hadoop.KerberosAuthHelper");
                }
                catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ReflectionUtils.newInstance(KLASS, null);
    }

    public void set(JobClient jobClient, JobConf jobConf) throws IOException, InterruptedException {
    }

}
