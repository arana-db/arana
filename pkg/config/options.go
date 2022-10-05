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
 *
 */

package config

type option func(o *configOption)

// WithCacheable set whether to enable the read cache mechanism
func WithCacheable(open bool) option {
	return func(o *configOption) {
		o.cacheable = open
	}
}

// WithWriter Set whether to enable the write operation of the configuration center
func WithWriter(open bool) option {
	return func(o *configOption) {
		o.openWriter = open
	}
}

// WithReader Set whether to enable the read operation of the configuration center
func WithReader(open bool) option {
	return func(o *configOption) {
		o.openReader = open
	}
}

// WithWatcher Set whether to enable the watch operation of the configuration center
func WithWatcher(open bool) option {
	return func(o *configOption) {
		o.openWatcher = open
	}
}

type configOption struct {
	tenant      string
	cacheable   bool
	openWriter  bool
	openReader  bool
	openWatcher bool
}
