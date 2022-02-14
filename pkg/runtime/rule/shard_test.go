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

package rule

import (
	"reflect"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestShardFactory(t *testing.T) {
	shardTable := []struct {
		in       ShardType
		wantName string
		wantErr  error
	}{
		{ModShard, string(ModShard), nil},
		{HashMd5Shard, string(HashMd5Shard), nil},
		{HashCrc32Shard, string(HashCrc32Shard), nil},
		{HashBKDRShard, string(HashBKDRShard), nil},
	}

	shard, err := ShardFactory("xxx", 0)
	assert.Equal(t, nil, shard)
	assert.Equal(t, "shardNum is invalid", err.Error())

	shard, err = ShardFactory("xxx", 1)
	assert.Equal(t, nil, shard)
	assert.Equal(t, "do not have this shardType", err.Error())

	for i := 0; i < len(shardTable); i++ {
		shard, err := ShardFactory(shardTable[i].in, 1)
		assert.Equal(t, shardTable[i].wantName, reflect.TypeOf(shard).Name())
		assert.Equal(t, shardTable[i].wantErr, err)
	}
}

func TestModShard(t *testing.T) {
	shardTable := []struct {
		Mod      int
		in, want int
	}{
		{7, -1, 1},
		{7, 3, 3},
		{7, 13, 6},
		{7, 14, 0},
	}
	for i := 0; i < len(shardTable); i++ {
		shard, err := ShardFactory(ModShard, shardTable[i].Mod)
		if err != nil {
			t.Error(err)
		}
		out, err := shard.Compute(shardTable[i].in)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, shardTable[i].want, out)
	}
}

func TestMd5Shard(t *testing.T) {
	shardTable := []struct {
		Mod  int
		in   string
		want int
	}{
		{7, "1", 0},
		{7, "abc", 4},
		{7, "DZ20201212", 1},
	}
	for i := 0; i < len(shardTable); i++ {
		shard, err := ShardFactory(HashMd5Shard, shardTable[i].Mod)
		if err != nil {
			t.Fatal(err)
		}
		out, err := shard.Compute(shardTable[i].in)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, shardTable[i].want, out)
	}
}

func TestCrc32Shard(t *testing.T) {
	shardTable := []struct {
		Mod  int
		in   string
		want int
	}{
		{7, "1", 2},
		{7, "abc", 5},
		{7, "DZ20201212", 3},
	}
	for i := 0; i < len(shardTable); i++ {
		shard, err := ShardFactory(HashCrc32Shard, shardTable[i].Mod)
		if err != nil {
			t.Fatal(err)
		}
		out, err := shard.Compute(shardTable[i].in)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, shardTable[i].want, out)
	}
}

func TestBKDRShard(t *testing.T) {
	shardTable := []struct {
		Mod  int
		in   string
		want int
	}{
		{7, "1", 0},
		{7, "abc", 6},
		{7, "DZ20201212", 5},
	}
	for i := 0; i < len(shardTable); i++ {
		shard, err := ShardFactory(HashBKDRShard, shardTable[i].Mod)
		if err != nil {
			t.Fatal(err)
		}
		out, err := shard.Compute(shardTable[i].in)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, shardTable[i].want, out)
	}
}
