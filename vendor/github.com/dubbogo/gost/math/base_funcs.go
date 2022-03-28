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

package gxmath

// AbsInt64 abs of int64
func AbsInt64(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

// AbsInt32 abs of int32
func AbsInt32(n int32) int32 {
	y := n >> 31
	return (n ^ y) - y
}

// AbsInt16 abs of int16
func AbsInt16(n int16) int16 {
	y := n >> 15
	return (n ^ y) - y
}

// AbsInt8 abs of int8
func AbsInt8(n int8) int8 {
	y := n >> 7
	return (n ^ y) - y
}
