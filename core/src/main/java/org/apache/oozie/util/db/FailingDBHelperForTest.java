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

package org.apache.oozie.util.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;

/**
 * Helper class for simulating DB failures.
 */
public class FailingDBHelperForTest {
    static final Predicate<String> DEFAULT_DB_PREDICATE = new FailingConnectionWrapper.OozieDmlStatementPredicate();
    // predicate which defines when the failure should be injected.
    // By default the DML statements for Oozie tables witt be the targeted statements
    @VisibleForTesting
    static Predicate<String> dbPredicate = DEFAULT_DB_PREDICATE;

    /**
     * change the used predicate value
     * @param predicate
     */
    @VisibleForTesting
    public static void setDbPredicate (final Predicate predicate) {
        FailingDBHelperForTest.dbPredicate = predicate;
    }

    /**
     * reset the predicate, to the default Oozie DML predicate.
     */
    @VisibleForTesting
    public static void resetDbPredicate() {
        FailingDBHelperForTest.dbPredicate = DEFAULT_DB_PREDICATE;
    }
}
