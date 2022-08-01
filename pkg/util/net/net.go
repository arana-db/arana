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

package net

import (
	"net"
	"os"
	"strings"
)

func FindSelfIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	openIPV6 := os.Getenv("arana.net.ipv6")

	var localhost net.IP
	var expectIP net.IP

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {

			// IsUnspecified reports whether ip is an unspecified address, either
			// the IPv4 address "0.0.0.0" or the IPv6 address "::".
			if ipnet.IP.IsUnspecified() {
				continue
			}

			// IsLoopback reports whether ip is a loopback address.
			if ipnet.IP.IsLoopback() {
				localhost = ipnet.IP
				continue
			}

			if expectIP == nil {
				if strings.Compare(openIPV6, "true") == 0 {
					expectIP = ipnet.IP.To16()
				} else {
					expectIP = ipnet.IP.To4()
				}
			}
		}
	}

	if expectIP == nil {
		expectIP = localhost
	}

	return expectIP.String(), nil
}
