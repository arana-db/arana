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

package dataset

import (
	"github.com/arana-db/arana/pkg/proto"
)

type pipeOption []func(proto.Dataset) proto.Dataset

func Filter(predicate PredicateFunc) Option {
	return func(option *pipeOption) {
		*option = append(*option, func(prev proto.Dataset) proto.Dataset {
			return FilterDataset{
				Dataset:   prev,
				Predicate: predicate,
			}
		})
	}
}

func Map(generateFields FieldsFunc, transform TransformFunc) Option {
	return func(option *pipeOption) {
		*option = append(*option, func(dataset proto.Dataset) proto.Dataset {
			return &TransformDataset{
				Dataset:      dataset,
				FieldsGetter: generateFields,
				Transform:    transform,
			}
		})
	}
}

type Option func(*pipeOption)

func Pipe(root proto.Dataset, options ...Option) proto.Dataset {
	var o pipeOption
	for _, it := range options {
		it(&o)
	}

	next := root
	for _, it := range o {
		next = it(next)
	}

	return next
}
