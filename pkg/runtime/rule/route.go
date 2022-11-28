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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/cmp"
)

type Matcher interface {
	Eval() (rule.Range, error)
}

type baseExpMatcher rule.VTable

func (bem *baseExpMatcher) vtab() *rule.VTable {
	return (*rule.VTable)(bem)
}

func (bem *baseExpMatcher) innerEval(c *cmp.Comparative) (rule.Range, error) {
	k := c.Key()

	// 非sharding键
	dbMetadata, tbMetadata, ok := bem.vtab().GetShardMetadata(k)
	if !ok {
		return nil, nil
	}

	value, err := c.Value()
	if err != nil {
		return nil, errors.Wrap(err, "eval failed:")
	}

	var md *rule.ShardMetadata

	if tbMetadata != nil {
		md = tbMetadata
	} else {
		md = dbMetadata
	}

	switch c.Comparison() {
	case cmp.Ceq:
		return Single(value), nil
	case cmp.Cgt:
		after, err := md.Stepper.After(value)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return md.Stepper.Ascend(after, md.Steps)
	case cmp.Cgte:
		return md.Stepper.Ascend(value, md.Steps)
	case cmp.Clt:
		before, err := md.Stepper.Before(value)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return md.Stepper.Descend(before, md.Steps)
	case cmp.Clte:
		return md.Stepper.Descend(value, md.Steps)
	case cmp.Cne:
		return nil, nil
	default:
		return nil, errors.Errorf("unsupported comparison %s", c.Comparison())
	}
}

type cmpExpMatcher struct {
	*baseExpMatcher
	c *cmp.Comparative
}

func (c *cmpExpMatcher) Eval() (rule.Range, error) {
	if c.c == nil {
		return nil, nil
	}
	return c.innerEval(c.c)
}

func Route(vt *rule.VTable, c *cmp.Comparative) (Matcher, error) {
	mat := &cmpExpMatcher{
		baseExpMatcher: (*baseExpMatcher)(vt),
		c:              c,
	}
	return mat, nil
}

func MatchTables(vt *rule.VTable, column string, it rule.Range) (*rule.Shards, error) {
	if it == nil {
		return nil, nil
	}
	var values []interface{}
	for it.HasNext() {
		values = append(values, it.Next())
	}

	if len(values) < 1 {
		return rule.NewShards(), nil
	}

	ret := rule.NewShards()
	for _, value := range values {
		dbIdx, tbIdx, err := vt.Shard(column, value)
		if err != nil {
			return nil, err
		}
		ret.Add(dbIdx, tbIdx)
	}

	return ret, nil
}
