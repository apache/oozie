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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestYarnApplicationIdComparator {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MapReduceActionExecutor.YarnApplicationIdComparator comparator;

    @Before
    public void setUp() {
        this.comparator = new MapReduceActionExecutor.YarnApplicationIdComparator();
    }

    @Test
    public void whenWrongParametersGivenExceptionIsThrown() {
        expectedException.expect(NullPointerException.class);
        comparator.compare(null, null);

        expectedException.expect(NumberFormatException.class);
        comparator.compare("application_a_b", "application_c_d");

        expectedException.expect(IndexOutOfBoundsException.class);
        comparator.compare("a_b_c", "d_e_f");
    }

    @Test
    public void whenDifferentTimestampsLeftEqualsRight() {
        assertEquals("cluster timestamps are different, the one with bigger timestamp wins",
                -1,
                comparator.compare("application_1534164756526_0001", "application_1534164756527_0002"));
    }

    @Test
    public void whenSameTimestampsGreaterSequenceWins() {
        assertEquals("cluster timestamps are the same but sequences are different, left should be greater than right",
                1,
                comparator.compare("application_1534164756526_0002", "application_1534164756526_0001"));
    }

    @Test
    public void whenSameTimestampsAndSameSequencesLeftEqualsRight() {
        assertEquals("cluster timestamps and sequences are the same, left should equal right",
                0,
                comparator.compare("application_1534164756526_0001", "application_1534164756526_0001"));
    }
}
