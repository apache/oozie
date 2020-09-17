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

package org.apache.oozie.util;

import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

public class DrainerTestCase {

    private static final String HEX_CHARACTERS = "0123456789ABCDEF";
    private static final List<String> sampleStrings = new ArrayList<>();
    private static final int[] suggestedMaxLengthsToCheck = {1, 1024, 1024*1024};
    private static final int[] stringLengthsToCheck = {0, 16, 4096, 1024*1024, 20*1024*1024};

    @BeforeClass
    public static void setUpBeforeClass() {
        if (sampleStrings.isEmpty()) {
            generateSamples();
        }
    }

    private static void generateSamples() {
        for (int length : stringLengthsToCheck) {
            sampleStrings.add(generateString(length));
        }
    }

    static String generateString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<length/HEX_CHARACTERS.length(); ++i) {
            sb.append(HEX_CHARACTERS);
        }
        sb.append(HEX_CHARACTERS.substring(0,length % HEX_CHARACTERS.length()));
        return sb.toString();
    }

    static abstract class StringAndIntProcessingCallback {
        public abstract void call(String sampleString, int suggestedMaxLength) throws Exception;
    }

    static abstract class StringProcessingCallback {
        public abstract void call(String sampleString) throws Exception;
    }

    void checkSampleStringWithDifferentMaxLength(StringAndIntProcessingCallback callback) throws Exception {
        for (String sampleString : sampleStrings) {
            for (int length : suggestedMaxLengthsToCheck) {
                callback.call(sampleString, length);
            }
        }
    }

    void checkSampleStrings(StringProcessingCallback callback) throws Exception {
        for (String sampleString : sampleStrings) {
            callback.call(sampleString);
        }
    }
}
