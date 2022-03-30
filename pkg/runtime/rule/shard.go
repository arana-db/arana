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

package rule

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"strconv"
)

import (
	gxhash "github.com/dubbogo/gost/hash"
	gxmath "github.com/dubbogo/gost/math"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
)

const (
	ModShard       ShardType = "modShard"
	HashMd5Shard   ShardType = "hashMd5Shard"
	HashCrc32Shard ShardType = "hashCrc32Shard"
	HashBKDRShard  ShardType = "hashBKDRShard"
)

var shardMap = map[ShardType]ShardComputerFunc{
	ModShard:       NewModShard,
	HashMd5Shard:   NewHashMd5Shard,
	HashCrc32Shard: NewHashCrc32Shard,
	HashBKDRShard:  NewHashBKDRShard,
}

func ShardFactory(shardType ShardType, shardNum int) (shardStrategy rule.ShardComputer, err error) {
	if shardNum <= 0 {
		return nil, errors.New("shardNum is invalid")
	}
	if f, ok := shardMap[shardType]; ok {
		return f(shardNum), nil
	}
	return nil, errors.New("do not have this shardType")
}

type ShardType string
type ShardComputerFunc func(shardNum int) rule.ShardComputer

type modShard struct {
	shardNum int
}

func NewModShard(shardNum int) rule.ShardComputer {
	return modShard{shardNum}
}

func (mod modShard) Compute(value interface{}) (int, error) {
	n, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(gxmath.AbsInt64(n)) % mod.shardNum, nil
}

// FNV32, FNV32a, FNV64, FNV64a, MD5, SHA1, SHA256, SHA512, murmur3, crc32, crc64, adler
type hashCrc32Shard struct {
	shardNum int
}

func NewHashCrc32Shard(shardNum int) rule.ShardComputer {
	return hashCrc32Shard{shardNum}
}

func (h hashCrc32Shard) Compute(value interface{}) (int, error) {
	s := fmt.Sprintf("%v", value)
	return int(crc32.ChecksumIEEE([]byte(s))) % h.shardNum, nil
}

type hashMd5Shard struct {
	shardNum int
}

func NewHashMd5Shard(shardNum int) rule.ShardComputer {
	return hashMd5Shard{shardNum}
}

func (m hashMd5Shard) Compute(value interface{}) (int, error) {
	s := fmt.Sprintf("%v", value)

	h := uint64(0)
	byteHash1 := md5.Sum([]byte(s))

	shardNum := m.shardNum
	bytesNum := 1
	for ; bytesNum < len(byteHash1); bytesNum++ {
		shardNum >>= 8
		y := shardNum & 0xFF
		if y == 0 {
			break
		}
	}

	for i := 0; i < bytesNum; i++ {
		h <<= 8
		h |= uint64(byteHash1[i]) & 0xFF
	}
	return int(h % uint64(m.shardNum)), nil
}

type hashBKDRShard struct {
	shardNum int
}

func NewHashBKDRShard(shardNum int) rule.ShardComputer {
	return hashBKDRShard{shardNum}
}

func (m hashBKDRShard) Compute(value interface{}) (int, error) {
	s := fmt.Sprintf("%v", value)
	return int(gxmath.AbsInt32(gxhash.BKDRHash(s))) % m.shardNum, nil
}
