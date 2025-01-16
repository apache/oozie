/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *      http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.coord;

import org.apache.commons.lang3.Range;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.oozie.command.CommandException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCoordUtilsNoServices {

    static {
        // only to check log messages are printed correctly
        Logger logger = Logger.getLogger(CoordUtils.class);
        logger.setLevel(Level.TRACE);
    }

    @Test
    public void testEmptyScopeIsInvalid() {
        try {
            CoordUtils.parseScopeToRanges("");
            fail("Empty scope should be invalid");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [scope should not be empty]",
                    e.getMessage()
            );
        }

        try {
            CoordUtils.parseScopeToRanges("   ");
            fail("Empty scope should be invalid");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [scope should not be empty]",
                    e.getMessage()
            );
        }

        try {
            CoordUtils.parseScopeToRanges("1-3,-,5-9");
            fail("Range without boundaries should be invalid");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [format is wrong for action's range '-', " +
                            "an example of correct format is 1-5]",
                    e.getMessage()
            );
        }
    }

    @Test
    public void testScopeToRanges() throws CommandException {
        final List<Range<Integer>> result = CoordUtils.parseScopeToRanges(" 1-3, 5, 7-900,1100,2000 -2000");
        assertEquals(Arrays.asList(
                Range.between(1, 3), // 3 numbers: 1, 2, 3
                Range.between(5, 5), // 1 number: 5
                Range.between(7, 900), // 894 numbers: 7, 8, ...899, 900
                Range.between(1100, 1100), // 1 number: 1100
                Range.between(2000, 2000) // 1 number: 2000
        ), result);

        assertEquals("Unexpected number of elements in the ranges",
                900, CoordUtils.getElemCountOfRanges(result));
    }

    @Test
    public void testScopeRangesInvalidRange() {
        try {
            CoordUtils.parseScopeToRanges("3-1,5,7-900,11");
            fail("'3-1,5,7-900,11' is should be marked as invalid scope");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [format is wrong for action's range '3-1', " +
                            "starting action number of the range should be less than ending action number, " +
                            "an example will be 1-4]",
                    e.getMessage()
            );
        }
    }

    @Test
    public void testScopeInvalidRangeMin() {
        try {
            CoordUtils.parseScopeToRanges("-4-0");
            fail("'-4-0' is should be marked as invalid scope");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [format is wrong for action's range '-4-0', " +
                            "an example of correct format is 1-5]",
                    e.getMessage()
            );
        }
    }

    @Test
    public void testScopeInvalidRangeMax() {
        try {
            CoordUtils.parseScopeToRanges("30-A");
            fail("'30-A' is should be marked as invalid scope");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [could not parse boundaries of 30-A into an integer]",
                    e.getMessage()
            );
        }
    }

    @Test
    public void testScopeInvalidNumberAmongRanges() {
        try {
            CoordUtils.parseScopeToRanges("2-7,A,9-11");
            fail("'2-7,A,9-11' is should be marked as invalid scope");
        } catch (CommandException e) {
            assertEquals(
                    "Unexpected error message",
                    "E0302: Invalid parameter [could not parse A into an integer]",
                    e.getMessage()
            );
        }
    }

}
