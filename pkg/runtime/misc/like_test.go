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

package misc

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLiker(t *testing.T) {
	tests := []struct {
		pattern string
		input   []string
		except  []bool
	}{
		{
			pattern: "abc%",
			input:   []string{"abc*", "abc?", "abc_", "abc%", "zmo"},
			except:  []bool{true, true, true, true, false},
		},
		{
			pattern: "abc_",
			input:   []string{"abc*", "abc?", "abc_", "abc%", "abc"},
			except:  []bool{true, true, true, true, false},
		},
		{
			pattern: "abc?",
			input:   []string{"abc*", "abc?", "abc_", "abc%", "abc"},
			except:  []bool{false, true, false, false, false},
		},
		{
			pattern: "*?abc_",
			input:   []string{"*?abc.", "**abc.", "?*abc.", "??abc."},
			except:  []bool{true, false, false, false},
		},
		{
			pattern: "[CB]at",
			input:   []string{"[CB]at", "Cat", "cat"},
			except:  []bool{true, false, false},
		},
		{
			pattern: "[a-z]at",
			input:   []string{"[a-z]at", "cat", "Cat"},
			except:  []bool{true, false, false},
		},
		{
			pattern: "[!C]at",
			input:   []string{"[!C]at", "Bat", "Cat"},
			except:  []bool{true, false, false},
		},
		{
			pattern: "\\%",
			input:   []string{"\\\\"},
			except:  []bool{true},
		},
		{
			pattern: "/home/\\\\*",
			input:   []string{"/home/\\\\*", "/home/\\*"},
			except:  []bool{true, false},
		},
		{
			pattern: "/hom%",
			input:   []string{"/home/hello/world"},
			except:  []bool{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			for i := 0; i < len(tt.input); i++ {
				liker := NewLiker(tt.pattern)
				assert.Equal(t, tt.except[i], liker.Like(tt.input[i]))
			}
		})
	}
}
