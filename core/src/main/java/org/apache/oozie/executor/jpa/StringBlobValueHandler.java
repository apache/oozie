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

package org.apache.oozie.executor.jpa;

import org.apache.oozie.StringBlob;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.ValueMapping;

@SuppressWarnings("serial")
public class StringBlobValueHandler extends org.apache.openjpa.jdbc.meta.strats.ByteArrayValueHandler {

    private static final StringBlobValueHandler _instance = new StringBlobValueHandler();

    /**
     * Singleton instance.
     */
    public static StringBlobValueHandler getInstance() {
        return _instance;
    }

    public Object toDataStoreValue(ValueMapping vm, Object val, JDBCStore store) {
        if (val == null) {
            return null;
        }
        return ((StringBlob) val).getRawBlob();
    }

    public Object toObjectValue(ValueMapping vm, Object val) {
        if (val == null) {
            return null;
        }
        return new StringBlob((byte[]) val);
    }
}
