/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/**
 * bind mysql function NOW
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_now
 *
 * @returns {Date}
 */
function $NOW() {
    return new Date();
}

/**
 * bind mysql function SYSDATE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_sysdate
 *
 * @returns {Date}
 */
function $SYSDATE() {
    return new Date();
}

/**
 * bind mysql function UNIX_TIMESTAMP
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_unix-timestamp
 *
 * @returns {number}
 */
function $UNIX_TIMESTAMP() {
    return ~~(Date.now() / 1000);
}

/**
 * bind mysql function CURRENT_TIMESTAMP
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_current-timestamp
 *
 * @returns {Date}
 */
function $CURRENT_TIMESTAMP() {
    return new Date();
}

/**
 * bind mysql function CURRENT_DATE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_current-date
 *
 * @returns {*}
 */
function $CURRENT_DATE() {
    return __curdate();
}

/**
 * bind mysql function LOCALTIME
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_localtime
 *
 * @returns {Date}
 */
function $LOCALTIME() {
    return new Date();
}

/**
 * bind mysql function CURDATE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_curdate
 *
 * @returns {*}
 */
function $CURDATE() {
    return __curdate();
}

/**
 * bind mysql function CURTIME
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_curtime
 *
 * @returns {*}
 */
function $CURTIME() {
    return __curtime();
}

/**
 * bind mysql function FROM_UNIXTIME
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_from-unixtime
 *
 * @param ts
 * @returns {null|Date}
 */
function $FROM_UNIXTIME(ts) {
    if (ts === null || ts === undefined) return null;
    let n = -1;
    if (typeof ts === 'string') {
        n = parseInt(ts)
    } else if (typeof ts === 'number') {
        n = ts;
    } else if (typeof ts === 'boolean') {
        n = ts ? 1 : 0;
    }
    if (n < 0) return null;
    return new Date(1000 * n);
}

/**
 * bind mysql function MONTH
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_month
 *
 * @param d
 * @returns {*}
 */
function $MONTH(d) {
    return __month(d);
}

/**
 * bind mysql function MONTHNAME
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_monthname
 *
 * @param d
 * @returns {*}
 */
function $MONTHNAME(d) {
    return __monthname(d);
}

/**
 * bind mysql function DAY.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_day
 *
 * @param d
 * @returns {*}
 */
function $DAY(d) {
    return __day(d);
}

/**
 * bind mysql function DAYNAME
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_dayname
 *
 * @param d
 * @returns {*}
 */
function $DAYNAME(d) {
    return __dayname(d);
}

/**
 * bind mysql function DAYOFWEEK.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_dayofweek
 *
 * @param d
 * @returns {*}
 */
function $DAYOFWEEK(d) {
    return __dayofweek(d);
}

/**
 * bind mysql function WEEK.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_week
 *
 * @param d
 * @returns {*}
 */
function $WEEK(d) {
    return __week(d);
}

/**
 * bind mysql function DAYOFMONTH
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_dayofmonth
 *
 * @param d
 * @returns {*}
 */
function $DAYOFMONTH(d) {
    return __dayofmonth(d);
}

/**
 * bind mysql function DAYOFYEAR.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_dayofyear
 *
 * @param d
 * @returns {*}
 */
function $DAYOFYEAR(d) {
    return __dayofyear(d);
}

/**
 * bind mysql function QUARTER.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_quarter
 *
 * @param d
 * @returns {*}
 */
function $QUARTER(d) {
    return __quarter(d);
}

/**
 * bind mysql function HOUR.
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_hour
 *
 * @param d
 * @returns {*}
 */
function $HOUR(d) {
    return __hour(d);
}

/**
 * bind mysql function MINUTE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_minute
 *
 * @param d
 * @returns {*}
 */
function $MINUTE(d) {
    return __minute(d);
}

/**
 * bind mysql function SECOND
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_second
 *
 * @param d
 * @returns {*}
 */
function $SECOND(d) {
    return __second(d);
}

/**
 * bind mysql function DATEDIFF
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_datediff
 *
 * @param date1
 * @param date2
 * @returns {*}
 */
function $DATEDIFF(date1, date2) {
    return __datediff(date1, date2);
}

/**
 * bind mysql function ADDDATE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_adddate
 *
 * @param d
 * @param n
 * @returns {Date}
 */
function $ADDDATE(d, n) {
    return new Date(__adddate(d, n));
}

/**
 * bind mysql function SUBDATE
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_subdate
 *
 * @param d
 * @param n
 * @returns {Date}
 */
function $SUBDATE(d, n) {
    return new Date(__adddate(d, -n));
}

/**
 * bind mysql function DATE_FORMAT
 * see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format
 *
 * @param d
 * @param f
 * @returns {null|*}
 */
function $DATE_FORMAT(d, f) {
    if (d === null || d === undefined) {
        return null;
    }
    if (typeof f !== 'string') {
        return null;
    }
    return __dateformat(d, f)
}
