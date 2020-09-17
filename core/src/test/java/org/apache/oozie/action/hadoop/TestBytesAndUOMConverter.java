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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestBytesAndUOMConverter {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void whenEmptyInputIsGivenException() {
        expectedException.expect(IllegalArgumentException.class);

        new BytesAndUOMConverter().toMegabytes(null);
    }

    @Test
    public void whenUOMIsIncorrectException() {
        expectedException.expect(IllegalArgumentException.class);

        new BytesAndUOMConverter().toMegabytes("123T");
    }

    @Test
    public void whenNoUnitIsGivenException() {
        expectedException.expect(NumberFormatException.class);

        new BytesAndUOMConverter().toMegabytes("K");
    }

    @Test
    public void whenIncorrectUnitIsGivenException() {
        expectedException.expect(NumberFormatException.class);

        new BytesAndUOMConverter().toMegabytes("1aa1K");
    }

    @Test
    public void whenNotPositiveUnitIsGivenException() {
        expectedException.expect(IllegalArgumentException.class);

        new BytesAndUOMConverter().toMegabytes("0K");
    }

    @Test
    public void whenUnitIsGivenAndNoUOMIsPresentConvertedCorrectly() {
        assertEquals("bytes count should be converted correctly",
                1L,
                new BytesAndUOMConverter().toMegabytes(
                        Integer.toString(new Double(Math.pow(2, 20)).intValue())));
    }

    @Test
    public void whenMegabytesAreGivenSameReturned() {
        assertEquals("megabytes count should remain unchanged",
                1L,
                new BytesAndUOMConverter().toMegabytes("1M"));
    }

    @Test
    public void whenKilobytesAreGivenConvertedCorrectly() {
        assertEquals("kilobytes count should be converted correctly",
                1L,
                new BytesAndUOMConverter().toMegabytes("1024K"));

        assertEquals("kilobytes count should be converted correctly",
                0L,
                new BytesAndUOMConverter().toMegabytes("1023K"));

        assertEquals("kilobytes count should be converted correctly",
                10L,
                new BytesAndUOMConverter().toMegabytes("10240K"));
    }

    @Test
    public void whenGigabytesAreGivenConvertedCorrectly() {
        assertEquals("gigabytes count should be converted correctly",
                1024L,
                new BytesAndUOMConverter().toMegabytes("1G"));
    }
}