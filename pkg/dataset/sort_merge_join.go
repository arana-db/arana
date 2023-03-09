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

import "github.com/arana-db/arana/pkg/proto"

var _ proto.Dataset = (*SortMergeJoin)(nil)

type SortMergeJoin struct {
	fields       []proto.Field
	joinFields   []proto.Field
	leftDataset  proto.Dataset
	rightDataset proto.Dataset
}

func (s *SortMergeJoin) Close() error {
	return nil
}

func (s *SortMergeJoin) Fields() ([]proto.Field, error) {
	return s.fields, nil
}

func (s *SortMergeJoin) Next() (proto.Row, error) {
	// all data is order
	// left or right dataset is nil, direct ret
	// get left dataset data , get right dataset data
	//	init a cursor with left data
	// 		if left data join key equal right return data
	//		if left data less than right data , lost left data
	//		if left data great than right data, lost right data, cursor to right data

	return nil, nil
}
