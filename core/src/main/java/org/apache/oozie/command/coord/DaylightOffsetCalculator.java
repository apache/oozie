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

package org.apache.oozie.command.coord;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Calculates daylight offset for a given {@code target} {@link Calendar}, given a {@link TimeZone}, and both
 * {@link #startMatdTime} and {@link #endMatdTime} {@link Calendar} instances.
 */
class DaylightOffsetCalculator {
    private final Date startMatdTime;
    private final Date endMatdTime;

    DaylightOffsetCalculator(final Date startMatdTime, final Date endMatdTime) {
        this.startMatdTime = startMatdTime;
        this.endMatdTime = endMatdTime;
    }

    /**
     * It adjusts {@code target} according to daylight saving time difference between {@link #startMatdTime} and
     * {@link #endMatdTime}.
     *  <ul>
     *      <li>if {@link #startMatdTime} is in DST (e.g. PDT) and {@link #endMatdTime} is in non-DST (e.g. PST), then we need to
     *      add {@link Calendar#DST_OFFSET} to {@code target}</li>
     *      <li>if {@link #startMatdTime} is in non-DST (e.g. PST) and {@link #endMatdTime} is in DST (e.g. PDT), then we need to
     *      subtract {@link Calendar#DST_OFFSET} to {@code target}
     *      <li>otherwise, we do not adjust {@code target} and simply return
     *  </ul>
     *
     * @param tz {@link TimeZone} for {@code target}
     * @param target the {@link Calendar} to modify
     * @return adjusted {@code target} according to DST offset between {@link #startMatdTime} and {@link #endMatdTime}
     */
    Calendar calculate(final TimeZone tz, final Calendar target) {
        final Calendar targetWithDSTOffset = Calendar.getInstance();
        targetWithDSTOffset.setTimeInMillis(target.getTime().getTime() + getDSTOffset(tz, startMatdTime, endMatdTime));

        return targetWithDSTOffset;
    }

    /**
     * Calculates daylight saving time difference between {@code beginDate} and {@code endDate}.
     *  <ul>
     *      <li>if {@code beginDate} is in DST (e.g. PDT) and {@code endDate} is in non-DST (e.g. PST), then we need to return
     *      {@link Calendar#DST_OFFSET}</li>
     *      <li>if {@code beginDate} is in non-DST (e.g. PST) and {@code endDate} is in DST (e.g. PDT), then we need to return
     *      {@code -1 *} {@link Calendar#DST_OFFSET}</li>
     *      <li>otherwise, return {@code 0}</li>
     *  </ul>
     *
     * @param tz {@link TimeZone} for {@code beginDate} and {@code endDate}
     * @param beginDate the beginning {@link Date} of the reference interval
     * @param endDate the ending {@link Date} of the reference interval
     * @return DST offset between {@code beginDate} and {@code endDate}
     */
    static long getDSTOffset(final TimeZone tz, final Date beginDate, final Date endDate) {
        if (tz.inDaylightTime(beginDate) && !tz.inDaylightTime(endDate)) {
            final Calendar cal = Calendar.getInstance(tz);
            cal.setTime(beginDate);

            return cal.get(Calendar.DST_OFFSET);
        }

        if (!tz.inDaylightTime(beginDate) && tz.inDaylightTime(endDate)) {
            final Calendar cal = Calendar.getInstance(tz);
            cal.setTime(endDate);

            return -cal.get(Calendar.DST_OFFSET);
        }

        return 0;
    }
}
