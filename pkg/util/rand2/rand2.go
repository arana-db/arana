//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// Copyright (c) 2014 Dropbox, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors
// may be used to endorse or promote products derived from this software without
// specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// rand2 is a drop-in replacement for the "math/rand" package.  It initializes
// the global random generator with a random seed (instead of 1), and provides
// additional functionality over the standard "math/rand" package.

package rand2

import (
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// A Source that can be concurrently used by multiple goroutines.
type lockedSource struct {
	mutex sync.Mutex
	src   rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.mutex.Lock()
	val := r.src.Int63()
	r.mutex.Unlock()
	return val
}

func (r *lockedSource) Seed(seed int64) {
	r.mutex.Lock()
	r.src.Seed(seed)
	r.mutex.Unlock()
}

// This returns a thread-safe random source.
func NewSource(seed int64) rand.Source {
	return &lockedSource{
		src: rand.NewSource(seed),
	}
}

// Generates a seed based on the current time and the process ID.
func GetSeed() int64 {
	now := time.Now()
	return now.Unix() + int64(now.Nanosecond()) + 12345*int64(os.Getpid())
}

func init() {
	rand.Seed(GetSeed())
}

var (
	// See math/rand for documentation.
	New = rand.New

	// See math/rand for documentation.
	Seed = rand.Seed

	// See math/rand for documentation.
	Int63 = rand.Int63

	// See math/rand for documentation.
	Uint32 = rand.Uint32

	// See math/rand for documentation.
	Int31 = rand.Int31

	// See math/rand for documentation.
	Int = rand.Int

	// See math/rand for documentation.
	Int63n = rand.Int63n

	// See math/rand for documentation.
	Int31n = rand.Int31n

	// See math/rand for documentation.
	Intn = rand.Intn

	// See math/rand for documentation.
	Float64 = rand.Float64

	// See math/rand for documentation.
	Float32 = rand.Float32

	// See math/rand for documentation.
	Perm = rand.Perm

	// See math/rand for documentation.
	NormFloat64 = rand.NormFloat64

	// See math/rand for documentation.
	ExpFloat64 = rand.ExpFloat64

	// See math/rand for documentation.
	NewZipf = rand.NewZipf
)

// Dur returns a pseudo-random Duration in [0, max)
func Dur(max time.Duration) time.Duration {
	return time.Duration(Int63n(int64(max)))
}

// Uniformly jitters the provided duration by +/- 50%.
func Jitter(period time.Duration) time.Duration {
	return JitterFraction(period, .5)
}

// Uniformly jitters the provided duration by +/- the given fraction.  NOTE:
// fraction must be in (0, 1].
func JitterFraction(period time.Duration, fraction float64) time.Duration {
	fixed := time.Duration(float64(period) * (1 - fraction))
	return fixed + Dur(2*(period-fixed))
}

// Samples 'k' unique ints from the range [0, n)
func SampleInts(n int, k int) (res []int, err error) {
	if k < 0 {
		err = errors.New("invalid sample size k")
		return
	}

	if n < k {
		err = errors.New("sample size k larger than n")
		return
	}

	picked := make(map[int]struct{})
	for len(picked) < k {
		i := Intn(n)
		picked[i] = struct{}{}
	}

	res = make([]int, 0, k)
	for i := range picked {
		res = append(res, i)
	}

	return
}

// Samples 'k' elements from the given slice
func Sample(population []interface{}, k int) (res []interface{}, err error) {
	n := len(population)
	idxs, err := SampleInts(n, k)
	if err != nil {
		return
	}

	res = []interface{}{}
	for _, idx := range idxs {
		res = append(res, population[idx])
	}

	return
}

// Same as 'Sample' except it returns both the 'picked' sample set and the
// 'remaining' elements.
func PickN(population []interface{}, n int) (
	picked []interface{}, remaining []interface{}, err error) {

	total := len(population)
	idxs, err := SampleInts(total, n)
	if err != nil {
		return
	}
	sort.Ints(idxs)

	picked, remaining = []interface{}{}, []interface{}{}
	for x, elem := range population {
		if len(idxs) > 0 && x == idxs[0] {
			picked = append(picked, elem)
			idxs = idxs[1:]
		} else {
			remaining = append(remaining, elem)
		}
	}

	return
}

// A subset of sort.Interface used for random shuffle.
type Swapper interface {
	// Len is the number of elements in the collection.
	Len() int
	// Swap swaps the elements with indexes i and j.
	Swap(i int, j int)
}

// Randomly shuffles the collection in place.
func Shuffle(collection Swapper) {
	// Fisher-Yates shuffle.
	for i := collection.Len() - 1; i >= 0; i-- {
		collection.Swap(i, Intn(i+1))
	}
}
