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

package gxhash

// BKDRHash BKDR Hash (java str.hashCode())
// s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
func BKDRHash(str string) int32 {
	h := int32(0)
	base := int32(1)
	s := []rune(str)
	for i := len(s) - 1; i >= 0; i-- {
		h += s[i] * base
		base = base<<5 - base
	}
	return h
}
