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
package org.apache.oozie.coord;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.util.ParamChecker;
import org.jdom.Element;

public class CoordUtils {
    public static final String HADOOP_UGI = "hadoop.job.ugi";

    public static final String HADOOP_USER = "user.name";

    public static String getDoneFlag(Element doneFlagElement) {
        if (doneFlagElement != null) {
            return doneFlagElement.getTextTrim();
        }
        else {
            return CoordELConstants.DEFAULT_DONE_FLAG;
        }
    }

    public static Configuration getHadoopConf(Configuration jobConf) {
        Configuration conf = new Configuration();
        ParamChecker.notNull(jobConf, "Configuration to be used for hadoop setup ");
        String user = ParamChecker.notEmpty(jobConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String group = ParamChecker.notEmpty(jobConf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
        conf.set(HADOOP_USER, user);
        conf.set(HADOOP_UGI, user + "," + group);
        return conf;
    }
}
