/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * bind mysql function CHAR_LENGTH.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_char-length
 *
 * @param s
 * @returns {*}
 */
function $CHAR_LENGTH(s) {
    return s.length;
}

/**
 * bind mysql function LENGTH.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_length
 *
 * @param s
 * @returns {number}
 */
function $LENGTH(s) {
    let b = 0;
    for (let i = 0; i < s.length; i++) {
        if (s.charCodeAt(i) > 255) {
            b += 3; // UTF8 as 3 byte
        } else {
            b++;
        }
    }
    return b;
}

/**
 * bind mysql function CONCAT.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_concat
 *
 * @returns {string}
 */
function $CONCAT() {
    let s = "";
    for (let i = 0; i < arguments.length; i++) {
        s += arguments[i]
    }
    return s;
}

/**
 * bind mysql function CONCAT_WS
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_concat-ws
 *
 * @returns {string}
 */
function $CONCAT_WS() {
    if (arguments.length < 2) {
        return "";
    }
    let s = "" + arguments[1];
    for (let i = 2; i < arguments.length; i++) {
        s += arguments[0];
        s += arguments[i];
    }
    return s;
}

/**
 * bind mysql function UPPER.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_upper
 *
 * @param s
 * @returns {string}
 */
function $UPPER(s) {
    return s.toUpperCase();
}

/**
 * bind mysql function LOWER.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lower
 *
 * @param s
 * @returns {string}
 */
function $LOWER(s) {
    return s.toLowerCase();
}

/**
 * bind mysql function LEFT.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_left
 *
 * @param s
 * @param n
 * @returns {string}
 */
function $LEFT(s, n) {
    return s.substring(0, n);
}

/**
 * bind mysql function RIGHT.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_right
 *
 * @param s
 * @param n
 * @returns {string}
 */
function $RIGHT(s, n) {
    return s.substring(s.length - n, s.length);
}

/**
 * bind mysql function REPEAT
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_repeat
 *
 * @param s
 * @param n
 * @returns {string}
 */
function $REPEAT(s, n) {
    let ret = "";
    for (let i = 0; i < n; i++) {
        ret += s;
    }
    return ret;
}

/**
 * bind mysql function SPACE.
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_space
 *
 * @param n
 * @returns {string}
 */
function $SPACE(n) {
    let ret = "";
    for (let i = 0; i < n; i++) {
        ret += " ";
    }
    return ret;
}

/**
 * bind mysql function REPLACE
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_replace
 *
 * @param s
 * @param search
 * @param replace
 * @returns {*}
 */
function $REPLACE(s, search, replace) {
    return s.replace(new RegExp(search, 'g'), replace);
}

/**
 * bind mysql function STRCMP
 * see https://dev.mysql.com/doc/refman/5.6/en/string-comparison-functions.html#function_strcmp
 *
 * @param a
 * @param b
 * @returns {number}
 */
function $STRCMP(a, b) {
    if (a === b) {
        return 0;
    }
    if (a > b) {
        return 1;
    }
    return -1;
}

/**
 * bind mysql function SUBSTRING
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_substring
 *
 * @param s
 * @param n
 * @param len
 * @returns {string}
 */
function $SUBSTRING(s, n, len) {
    if (n === 0 || len < 1) return "";
    if (n > 0) return s.substring(n - 1, n - 1 + len)
    return s.substring(s.length + n, s.length + n + len)
}

/**
 * bind mysql function REVERSE
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_reverse
 *
 * @param s
 * @returns {string}
 */
function $REVERSE(s) {
    return s.split("").reverse().join("");
}

/**
 * bind mysql function LTRIM
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_ltrim
 *
 * @param s
 * @returns {*}
 */
function $LTRIM(s) {
    return __ltrim(s);
}

/**
 * bind mysql function RTRIM
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_rtrim
 *
 * @param s
 * @returns {*}
 */
function $RTRIM(s) {
    return __rtrim(s);
}

/**
 * bind mysql function LPAD
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lpad
 *
 * @param s
 * @param l
 * @param pad
 * @returns {*}
 */
function $LPAD(s, l, pad) {
    return __lpad(s, l, pad)
}

/**
 * bind mysql function RPAD
 * see https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_rpad
 *
 * @param s
 * @param l
 * @param pad
 * @returns {*}
 */
function $RPAD(s, l, pad) {
    return __rpad(s, l, pad)
}

