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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.oozie.util.XLog;

import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.commons.io.FileUtils.ONE_KB;
import static org.apache.commons.io.FileUtils.ONE_MB;

/**
 * Converts {@code String}s that contain byte counts and Units of Measure (K, M, or G) to a {@code long bytesCount} in the
 * desired Unit of Measure.
 */
class BytesAndUOMConverter {
    private static final XLog LOG = XLog.getLog(BytesAndUOMConverter.class);

    /**
     * Convert {@code unitAndUOM} to {@code long bytesCount} in megabytes.
     * @param unitAndUOM a {@code String} consisting of count and Unit of Measure (K, M, or G)
     * @return {@code long bytesCount} in megabytes
     */
    long toMegabytes(final String unitAndUOM) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(unitAndUOM), "unitAndUOM should not be empty");
        Preconditions.checkArgument(unitAndUOM.toUpperCase().endsWith("K")
                || unitAndUOM.toUpperCase().endsWith("M")
                || unitAndUOM.toUpperCase().endsWith("G")
                || Character.isDigit(unitAndUOM.charAt(unitAndUOM.length() -1)),
                "unitAndUOM should end with a proper UoM or with a digit");

        try {
            final long bytesCount;

            if (unitAndUOM.toUpperCase().endsWith("K")) {
                bytesCount = getUnit(unitAndUOM) * ONE_KB;
            }
            else if (unitAndUOM.toUpperCase().endsWith("M")) {
                bytesCount = getUnit(unitAndUOM) * ONE_MB;
            }
            else if (unitAndUOM.toUpperCase().endsWith("G")) {
                bytesCount = getUnit(unitAndUOM) * ONE_GB;
            }
            else {
                bytesCount = Long.parseLong(unitAndUOM);
            }

            Preconditions.checkArgument(bytesCount > 0L, "unit should be positive");

            return bytesCount / ONE_MB;
        }
        catch (final NumberFormatException e) {
            LOG.error("Cannot parse bytes and UoM {0}, cannot convert to megabytes.");
            throw e;
        }
    }

    private long getUnit(final String unitAndUOM) {
        return Long.parseLong(unitAndUOM.substring(0, unitAndUOM.length() - 1));
    }
}
