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
 * bind mysql function MD5.
 * see https://dev.mysql.com/doc/refman/5.6/en/encryption-functions.html#function_md5
 *
 * @param s
 * @returns {*}
 */
function $MD5(s) {
    return __md5(s);
}

/**
 * bind mysql function SHA.
 * see https://dev.mysql.com/doc/refman/5.6/en/encryption-functions.html#function_sha1
 *
 * @param s
 * @returns {*}
 */
function $SHA(s) {
    return __sha(s);
}

/**
 * bind mysql function SHA1.
 * see https://dev.mysql.com/doc/refman/5.6/en/encryption-functions.html#function_sha1
 *
 * @param s
 * @returns {*}
 */
function $SHA1(s) {
    return __sha1(s);
}
