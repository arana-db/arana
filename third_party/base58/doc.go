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

// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*
Package base58 provides an API for working with modified base58 and Base58Check
encodings.

# Modified Base58 Encoding

Standard base58 encoding is similar to standard base64 encoding except, as the
name implies, it uses a 58 character alphabet which results in an alphanumeric
string and allows some characters which are problematic for humans to be
excluded.  Due to this, there can be various base58 alphabets.

The modified base58 alphabet used by Bitcoin, and hence this package, omits the
0, O, I, and l characters that look the same in many fonts and are therefore
hard to humans to distinguish.

# Base58Check Encoding Scheme

The Base58Check encoding scheme is primarily used for Bitcoin addresses at the
time of this writing, however it can be used to generically encode arbitrary
byte arrays into human-readable strings along with a version byte that can be
used to differentiate the same payload.  For Bitcoin addresses, the extra
version is used to differentiate the network of otherwise identical public keys
which helps prevent using an address intended for one network on another.
*/
package base58
