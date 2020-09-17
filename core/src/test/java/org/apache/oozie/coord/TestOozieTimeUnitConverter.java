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

package org.apache.oozie.coord;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestOozieTimeUnitConverter {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void whenSourceIsNullNPEIsThrown() {
        expectedException.expect(NullPointerException.class);

        new CoordELFunctions.OozieTimeUnitConverter().convertMillis(0, null);
    }

    @Test
    public void whenSourceIsNotRecognized() {
        assertMillisConverted(-1, 0, TimeUnit.END_OF_DAY);
        assertMillisConverted(-1, 0, TimeUnit.END_OF_WEEK);
        assertMillisConverted(-1, 0, TimeUnit.END_OF_MONTH);
        assertMillisConverted(-1, 0, TimeUnit.CRON);
        assertMillisConverted(-1, 0, TimeUnit.NONE);
    }

    @Test
    public void whenSourceMillisAreConvertedToMinutesCorrectly() {
        assertMillisConverted(0, 1, TimeUnit.MINUTE);
        assertMillisConverted(0, -1, TimeUnit.MINUTE);
        assertMillisConverted(1, 60_000, TimeUnit.MINUTE);
        assertMillisConverted(-1, -60_000, TimeUnit.MINUTE);
    }

    @Test
    public void whenSourceMillisAreConvertedToHoursCorrectly() {
        assertMillisConverted(0, 1, TimeUnit.HOUR);
        assertMillisConverted(0, -1, TimeUnit.HOUR);
        assertMillisConverted(1, 3_600_000, TimeUnit.HOUR);
        assertMillisConverted(-1, -3_600_000, TimeUnit.HOUR);
    }

    @Test
    public void whenSourceMillisAreConvertedToDaysCorrectly() {
        assertMillisConverted(0, 1, TimeUnit.DAY);
        assertMillisConverted(0, -1, TimeUnit.DAY);
        assertMillisConverted(1, 86_400_000, TimeUnit.DAY);
        assertMillisConverted(-1, -86_400_000, TimeUnit.DAY);
    }

    private void assertMillisConverted(final long expectedTUCount, final long millis, final TimeUnit oozieTU) {
        assertEquals(String.format("%d millis are converted to %s correctly", millis, oozieTU.name()),
                expectedTUCount,
                new CoordELFunctions.OozieTimeUnitConverter().convertMillis(millis, oozieTU));
    }
}