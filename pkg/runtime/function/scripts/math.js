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
 * bind mysql function ABS
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_abs
 *
 * @param x
 * @returns {null|number}
 */
function $ABS(x) {
    if (x === null || x === undefined) return null;
    return Math.abs(x);
}

/**
 * bind mysql function CEIL
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_ceil
 *
 * @param x
 * @returns {null|number}
 */
function $CEIL(x) {
    if (x === null || x === undefined) return null;
    return Math.ceil(x);
}

/**
 * bind mysql function FLOOR
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_floor
 *
 * @param x
 * @returns {null|number}
 */
function $FLOOR(x) {
    if (x === null || x === undefined) return null;
    return Math.floor(x);
}

/**
 * bind mysql function RAND
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_rand
 *
 * @returns {number}
 */
function $RAND() {
    return Math.random();
}

/**
 * bind mysql function SIGN
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_sign
 *
 * @param x
 * @returns {null|number}
 */
function $SIGN(x) {
    if (x === null || x === undefined) return null;
    return Math.sign(x);
}

/**
 * bind mysql function PI
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_pi
 *
 * @returns {number}
 */
function $PI() {
    return Math.PI;
}

/**
 * bind mysql function TRUNCATE
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_truncate
 *
 * @param x
 * @param y
 * @returns {null|number}
 */
function $TRUNCATE(x, y) {
    if (x === null || x === undefined) return null;
    let n = parseInt(y) * 100;
    return Math.floor(x * n) / n;
}

/**
 * bind mysql function ROUND
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_round
 *
 * @param x
 * @returns {null|number}
 */
function $ROUND(x) {
    if (x === null || x === undefined) return null;
    return Math.round(x);
}

/**
 * bind mysql function POWER
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_power
 *
 * @param x
 * @param y
 * @returns {null|number}
 */
function $POWER(x, y) {
    if (x === null || x === undefined) return null;
    return Math.pow(x, y);
}

/**
 * bind mysql function SQRT
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_sqrt
 *
 * @param x
 * @returns {null|number}
 */
function $SQRT(x) {
    if (x === null || x === undefined) return null;
    return Math.sqrt(x);
}

/**
 * bind mysql function EXP
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_exp
 *
 * @param x
 * @returns {null|number}
 */
function $EXP(x) {
    if (x === null || x === undefined) return null;
    return Math.exp(x);
}

/**
 * bind mysql function MOD
 * see https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_mod
 *
 * @param x
 * @param y
 * @returns {null|number}
 */
function $MOD(x, y) {
    if (x === null || x === undefined) return null;
    return x % y;
}
