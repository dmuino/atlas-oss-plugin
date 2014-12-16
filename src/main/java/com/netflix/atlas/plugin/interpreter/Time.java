/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.atlas.plugin.interpreter;

import com.netflix.servo.Metric;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * Handle :time.
 */
class Time implements ValueExpression {
    private static final long STEP = 1000L;
    private final TimeFunction timeFunction;


    Time(TimeFunction timeFunction) {
        this.timeFunction = timeFunction;
    }

    static Time withMode(String mode) {
        return new Time(TimeFunction.valueOf(mode));
    }

    @Override
    public double apply(List<Metric> updates) {
        long alignedTime = System.currentTimeMillis() / STEP * STEP;
        DateTime now = new DateTime(alignedTime, DateTimeZone.UTC);
        return timeFunction.apply(now);
    }

    @Override
    public String getLabel() {
        return null;
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return null;
    }

    /**
     * Valid modes for :time.
     */
    enum TimeFunction {
        /**
         * TimeFunction that returns the second of minute field value.
         */
        secondOfMinute(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getSecondOfMinute();
            }
        }),
        /**
         * TimeFunction that returns the second of day field value.
         */
        secondOfDay(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getSecondOfDay();
            }
        }),
        /**
         * TimeFunction that returns the minute of hour field value.
         */
        minuteOfHour(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMinuteOfHour();
            }
        }),
        /**
         * TimeFunction that returns the minute of day field value.
         */
        minuteOfDay(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMinuteOfDay();
            }
        }),
        /**
         * TimeFunction that returns the hour of day field value.
         */
        hourOfDay(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getHourOfDay();
            }
        }),
        /**
         * TimeFunction that returns the day of week field value.
         */
        dayOfWeek(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getDayOfWeek();
            }
        }),
        /**
         * TimeFunction that returns the day of month field value.
         */
        dayOfMonth(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getDayOfMonth();
            }
        }),
        /**
         * TimeFunction that returns the day of year field value.
         */
        dayOfYear(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getDayOfYear();
            }
        }),
        /**
         * TimeFunction that returns the month of year field value.
         */
        monthOfYear(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMonthOfYear();
            }
        }),
        /**
         * TimeFunction that returns the year of century field value.
         */
        yearOfCentury(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getYearOfCentury();
            }
        }),
        /**
         * TimeFunction that returns the year of era field value.
         */
        yearOfEra(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getYearOfEra();
            }
        }),
        /**
         * TimeFunction that returns the number of seconds since the epoch.
         */
        seconds(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMillis() / Constants.MS_IN_SECS;
            }
        }),
        /**
         * TimeFunction that returns the number of minutes since the epoch.
         */
        minutes(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMillis() / Constants.MS_IN_MINS;
            }
        }),
        /**
         * TimeFunction that returns the number of hours since the epoch.
         */
        hours(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMillis() / Constants.MS_IN_HOURS;
            }
        }),
        /**
         * TimeFunction that returns the number of days since the epoch.
         */
        days(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMillis() / Constants.MS_IN_DAYS;
            }
        }),
        /**
         * TimeFunction that returns the number of weeks since the epoch.
         */
        weeks(new Apply() {
            @Override
            public long apply(DateTime dateTime) {
                return dateTime.getMillis() / Constants.MS_IN_WEEKS;
            }
        });
        private final Apply timeFunction;

        TimeFunction(Apply timeFunction) {
            this.timeFunction = timeFunction;
        }

        long apply(DateTime dateTime) {
            return timeFunction.apply(dateTime);
        }

        private interface Apply {
            long apply(DateTime dateTime);
        }

        private static class Constants {
            static final long MS_IN_SECS = 1000L;
            static final long MS_IN_MINS = 60 * MS_IN_SECS;
            static final long MS_IN_HOURS = 60 * MS_IN_MINS;
            static final long MS_IN_DAYS = 24 * MS_IN_HOURS;
            static final long MS_IN_WEEKS = 7 * MS_IN_DAYS;
        }
    }
}
