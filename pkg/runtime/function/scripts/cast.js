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
 * bind mysql cast function with unsigned:
 *  - CAST(expr AS UNSIGNED)
 *  - CONVERT(expr, UNSIGNED)
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param src
 * @returns {number|*}
 */
function $CAST_UNSIGNED(src) {
    let x = parseInt(src)
    if (x >= 0) {
        return x;
    }
    return __unary('~', x)
}

/**
 * bind mysql cast function with unsigned:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param src
 * @returns {number}
 */
function $CAST_SIGNED(src) {
    return parseInt(src);
}

/**
 * bind mysql cast function with CHARSET:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param charset
 * @param src
 * @returns {*}
 */
function $CAST_CHARSET(charset, src) {
    return __cast_charset(charset, src)
}

/**
 * bind mysql cast function with NCHAR:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param length
 * @param src
 * @returns {null|string|string}
 */
function $CAST_NCHAR(length, src) {
    if (typeof src !== 'string') {
        src = __to_string(src)
    }
    if (src === null) return null;
    return length > 0 ? src.substring(0, length) : src;
}

/**
 * bind mysql cast function with CHAR:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param length
 * @param charset
 * @param src
 * @returns {null|string|string}
 */
function $CAST_CHAR(length, charset, src) {
    if (charset === '') {
        if (typeof src !== 'string') {
            src = __to_string(src)
        }
    } else {
        src = __cast_charset(charset, src)
    }

    if (src === null) return null;
    return length > 0 ? src.substring(0, length) : src;
}

/**
 * bind mysql cast function with DATE:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param src
 * @returns {null|Date}
 */
function $CAST_DATE(src) {
    if (src instanceof Date) {
        src.setHours(0, 0, 0, 0);
        return src;
    }
    if (typeof src === 'string') {
        var ts = __parse_time(src)
        if (ts === null) return null;
        var d = new Date(ts)
        d.setHours(0, 0, 0, 0);
        return d
    }
    return null;
}

/**
 * bind mysql cast function with DATETIME:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param src
 * @returns {null|Date}
 */
function $CAST_DATETIME(src) {
    if (src instanceof Date) {
        return src;
    }
    if (typeof src === 'string') {
        let ts = __parse_time(src)
        if (ts === null) return null;
        return new Date(ts)
    }
    return null;
}

/**
 * bind mysql cast function with TIME:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param src
 * @returns {null|Date}
 */
function $CAST_TIME(src) {
    if (src instanceof Date) {
        return __lpad(src.getHours(), 2, '0') + ':' + __lpad(src.getMinutes(), 2, '0') + ':' + __lpad(src.getSeconds(), 2, '0')
    }
    if (typeof src === 'string') {
        let ts = __parse_time(src)
        if (ts !== null) {
            src = new Date(ts)
            return __lpad(src.getHours(), 2, '0') + ':' + __lpad(src.getMinutes(), 2, '0') + ':' + __lpad(src.getSeconds(), 2, '0')
        }
    }
    return null;
}

/**
 * bind mysql cast function with DECIMAL:
 * see: https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
 *
 * @param m
 * @param d
 * @param src
 * @returns {*}
 */
function $CAST_DECIMAL(m, d, src) {
    if (m === 0) {
        m = 38
    }
    return __cast_decimal(m, d, src)
}
