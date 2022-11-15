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

package store

import (
	"context"
	"sync"
	"time"
)

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

const defaultTTL = 30

// EtcdV3 is the receiver type for the Store interface
type EtcdV3 struct {
	timeout time.Duration
	client  *clientv3.Client
	cfg     clientv3.Config
	leaseID clientv3.LeaseID

	done           chan struct{}
	startKeepAlive chan struct{}

	mu  sync.RWMutex
	ttl int64
}

// NewEtcdV3 creates a new Etcd client given a list
// of endpoints and an optional tls config
func NewEtcdV3(addrs []string, conf *Options) (Store, error) {
	s := &EtcdV3{
		done:           make(chan struct{}),
		startKeepAlive: make(chan struct{}),
		ttl:            defaultTTL,
	}

	cfg := clientv3.Config{
		Endpoints: addrs,
	}
	if conf != nil {
		cfg.DialTimeout = conf.DialTimeout
		cfg.DialKeepAliveTime = 10 * time.Second
		cfg.DialKeepAliveTimeout = 2 * cfg.DialTimeout
		cfg.TLS = conf.TLS
		cfg.Username = conf.Username
		cfg.Password = conf.Password
	}
	if s.timeout == 0 {
		s.timeout = 10 * time.Second
	}
	s.cfg = cfg

	err := s.init()
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-s.startKeepAlive:
		case <-s.done:
			return
		}

		var ch <-chan *clientv3.LeaseKeepAliveResponse
		var kaerr error
	rekeepalive:
		cli := s.client
		for {
			if s.leaseID != 0 {
				ch, kaerr = cli.KeepAlive(context.Background(), s.leaseID)
			}
			if kaerr == nil {
				break
			}
			time.Sleep(time.Second)
		}

		for {
			select {
			case <-s.done:
				return
			case resp := <-ch:
				if resp == nil { // connection is closed
					cli.Close()
					for {
						select {
						case <-s.done:
							return
						default:
							err = s.init()
							if err != nil {
								time.Sleep(time.Second)
								continue
							}
							s.mu.RLock()
							err = s.grant(context.Background(), s.ttl)
							s.mu.RUnlock()
							if err != nil {
								s.client.Close()
								time.Sleep(time.Second)
								continue
							}
							goto rekeepalive
						}
					}

				}
			}
		}
	}()

	return s, nil
}

func (s *EtcdV3) init() error {
	cli, err := clientv3.New(s.cfg)
	if err != nil {
		return err
	}

	s.client = cli
	return nil
}

func (s *EtcdV3) grant(ctx context.Context, ttl int64) error {
	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	resp, err := s.client.Grant(newCtx, ttl)
	cancel()
	if err == nil {
		s.leaseID = resp.ID
	}
	return err
}

// Put a value at the specified key
func (s *EtcdV3) Put(ctx context.Context, key string, value []byte, ttl int64) error {
	if ttl == 0 {
		ttl = defaultTTL
	}
	s.mu.Lock()
	s.ttl = ttl
	s.mu.Unlock()

	// init leaseID
	if s.leaseID == 0 {
		err := s.grant(ctx, ttl)
		if err != nil {
			return err
		}
		close(s.startKeepAlive)
	}

	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	_, err := s.client.Put(newCtx, key, string(value), clientv3.WithLease(s.leaseID))
	cancel()
	return err
}

// Get a value given its key
func (s *EtcdV3) Get(ctx context.Context, key string) ([]byte, error) {
	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	resp, err := s.client.Get(newCtx, key)
	cancel()
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrKeyNotFound
	}
	return resp.Kvs[0].Value, nil
}

// Delete the value at the specified key
func (s *EtcdV3) Delete(ctx context.Context, key string) error {
	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	_, err := s.client.Delete(newCtx, key)
	cancel()
	return err
}

// Exists verifies if a Key exists in the store
func (s *EtcdV3) Exists(ctx context.Context, key string) (bool, error) {
	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	resp, err := s.client.Get(newCtx, key)
	cancel()
	if err != nil {
		return false, err
	}

	return len(resp.Kvs) != 0, nil
}

// Watch for changes on a key
func (s *EtcdV3) Watch(ctx context.Context, key string, stopCh <-chan struct{}) (<-chan []byte, error) {
	watchCh := make(chan []byte)

	go func() {
		defer close(watchCh)

		pair, err := s.Get(ctx, key)
		if err != nil {
			return
		}
		watchCh <- pair

		rch := s.client.Watch(ctx, key)
		for {
			select {
			case <-s.done:
				return
			case wresp := <-rch:
				for _, event := range wresp.Events {
					watchCh <- event.Kv.Value
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on child nodes under
// a given directory
func (s *EtcdV3) WatchTree(ctx context.Context, directory string, stopCh <-chan struct{}) (<-chan [][]byte, error) {
	watchCh := make(chan [][]byte)

	go func() {
		defer close(watchCh)

		valList, err := s.List(ctx, directory)
		if err != nil && err != ErrKeyNotFound {
			return
		}

		watchCh <- valList

		rch := s.client.Watch(ctx, directory, clientv3.WithPrefix())
		for {
			select {
			case <-s.done:
				return
			case <-rch:
				valList, err := s.List(ctx, directory)
				if err != nil && err != ErrKeyNotFound {
					continue
				}
				watchCh <- valList
			}
		}
	}()

	return watchCh, nil
}

// List the content of a given prefix
func (s *EtcdV3) List(ctx context.Context, directory string) ([][]byte, error) {
	newCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	resp, err := s.client.Get(newCtx, directory, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	kvpairs := make([][]byte, 0, len(resp.Kvs))
	if len(resp.Kvs) == 0 {
		return nil, ErrKeyNotFound
	}

	for _, kv := range resp.Kvs {
		kvpairs = append(kvpairs, kv.Value)
	}
	return kvpairs, nil
}

// Close closes the client connection
func (s *EtcdV3) Close() {
	defer func() {
		if err := recover(); err != nil {
			// close of closed channel panic occur
			log.Errorf("close client error:%v", err)
		}
	}()
	close(s.done)
	s.client.Close()
}
