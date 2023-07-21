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

package logic

var _ Item = (*Bool)(nil)

type Bool bool

func (b Bool) Compare(c Item) int {
	switch that := c.(type) {
	case Bool:
		var x, y int
		if b {
			x = 1
		}
		if that {
			y = 1
		}
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	default:
		return -1
	}
}

type BoolOperator struct{}

func (b BoolOperator) AND(first Bool, others ...Bool) (Bool, error) {
	if !first {
		return false, nil
	}
	for i := range others {
		if !others[i] {
			return false, nil
		}
	}
	return true, nil
}

func (b BoolOperator) OR(first Bool, others ...Bool) (Bool, error) {
	if first {
		return true, nil
	}
	for i := range others {
		if others[i] {
			return true, nil
		}
	}
	return false, nil
}

func (b BoolOperator) NOT(input Bool) (Bool, error) {
	return !input, nil
}
