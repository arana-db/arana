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

import (
	"fmt"
	"github.com/pkg/errors"
)

func GetStoreOperate() (StoreOperate, error) {
	if storeOperate != nil {
		return storeOperate, nil
	}

	return nil, errors.New("StoreOperate not init")
}

func initStoreOperate(name string, options map[string]interface{}) error {
	s, exist := slots[name]
	if !exist {
		return fmt.Errorf("StoreOperate solt=[%s] not exist", name)
	}

	storeOperate = s
	return storeOperate.Init(options)
}

func Init(options Options) error {
	initPath(options.RootPath)

	if err := initStoreOperate(options.StoreName, options.Options); err != nil {
		return err
	}

	return nil
}
