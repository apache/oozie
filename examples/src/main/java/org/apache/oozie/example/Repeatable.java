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
package org.apache.oozie.example;

import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;

public class Repeatable {
	private String name;
	private Date baseline;
	private TimeZone timeZone;
	private int frequency;
	private TimeUnit timeUnit;
	public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

	/**
	 * Compute the occurrence number for the given nominal time using a TZ-DST
	 * sensitive frequency If nominal time is before baseline return -1
	 *
	 * @param nominalTime
	 *            :baseline time
	 * @param timeLimit
	 *            : Max end time
	 * @return occurrence number
	 */
	int getOccurrence(Date nominalTime, Date timeLimit) {
		int occurrence = -1;
		// ensure nominal time is greater than initial-instance
		long positiveDiff = nominalTime.getTime() - getBaseline().getTime();
		if (positiveDiff >= 0) {
			Calendar calendar = Calendar.getInstance(getTimeZone());
			calendar.setLenient(true);
			calendar.setTime(getBaseline());
			occurrence = 0;
			// starting from initial instance increment frequencies until
			// passing nominal time
			while (calendar.getTime().compareTo(nominalTime) < 0) {
				if (timeLimit != null
						&& calendar.getTime().compareTo(timeLimit) > 0) {
					return -1;
				}
				calendar.add(getTimeUnit().getCalendarUnit(), getFrequency());
				occurrence++;
			}
			// compute reminder delta between nominal time and closest greater
			// frequency tick time
			long nominalCurrentDelta = nominalTime.getTime()
					- calendar.getTime().getTime();
			// ensure that computed current is greater than initial-instance
			// the nominalCurrentDelta has to be used to cover the case when the
			// computed current
			// falls between (-1*f ... 0*f)
			positiveDiff = calendar.getTime().getTime()
					- getBaseline().getTime() + nominalCurrentDelta;
			if (positiveDiff < 0) {
				occurrence = -1;
			}
		}
		return occurrence;
	}

	/**
	 * Compute the occurrence number for the given nominal time using a TZ-DST
	 * sensitive frequency If nominal time is before baseline return -1
	 *
	 * @param nominalTime
	 *            :baseline time
	 * @return occurrence number
	 */
	public int getOccurrence(Date nominalTime) {
		return getOccurrence(nominalTime, null);
	}

	/**
	 * Compute the occurrence nominal time for the given nominal-time and
	 * occurrence-offset using a TZ-DST sensitive frequency If the computed
	 * occurrence is before baseline time returns NULL
	 *
	 * @param nominalTime
	 *            :baseline time
	 * @param occurrenceOffset
	 *            : offset
	 * @param timeLimit
	 *            : Max end time
	 * @return Date after 'occurrenceOffset' instance
	 */
	Date getOccurrenceTime(Date nominalTime, int occurrenceOffset,
			Date timeLimit) {
		Date date = null;
		int occurrence = getOccurrence(nominalTime, timeLimit);
		if (occurrence > -1) {
			occurrence += occurrenceOffset;
			occurrence = (occurrence >= 0) ? occurrence : -1;
		}
		if (occurrence > -1) {
			Calendar calendar = Calendar.getInstance(getTimeZone());
			calendar.setLenient(true);
			calendar.setTime(getBaseline());
			calendar.add(getTimeUnit().getCalendarUnit(), getFrequency()
					* occurrence);
			date = calendar.getTime();

		}
		return date;
	}

	/**
	 * Compute the occurrence nominal time for the given nominal-time and
	 * occurrence-offset using a TZ-DST sensitive frequency If the computed
	 * occurrence is before baseline time returns NULL
	 *
	 * @param nominalTime
	 *            :baseline time
	 * @param occurrenceOffset
	 *            : offset
	 * @return Date after 'occurrenceOffset' instance
	 */
	public Date getOccurrenceTime(Date nominalTime, int occurrenceOffset) {
		return getOccurrenceTime(nominalTime, occurrenceOffset, null);
	}

	/**
	 * computes the nominal time for the Nth occurrence of the Repeatable
	 *
	 * @param occurrence
	 *            : instance numbner
	 * @return TimeStamp of the Nth instance
	 */
	public Date getTime(int occurrence) {
		if (occurrence < 0) {
			throw new IllegalArgumentException("occurrence cannot be <0");
		}
		Calendar calendar = Calendar.getInstance(getTimeZone());
		calendar.setLenient(true);
		calendar.setTime(getBaseline());
		calendar.add(getTimeUnit().getCalendarUnit(), getFrequency()
				* occurrence);
		return calendar.getTime();
	}

	// Setters and getters
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getBaseline() {
		return baseline;
	}

	public void setBaseline(Date baseline) {
		this.baseline = baseline;
	}

	public TimeZone getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(TimeZone timeZone) {
		this.timeZone = timeZone;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
	}

}
