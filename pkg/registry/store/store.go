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
	"crypto/tls"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrBackendNotSupported is thrown when the backend k/v store is not supported by libkv
	ErrBackendNotSupported = errors.New("backend storage not supported yet, please choose one of")
	// ErrCallNotSupported is thrown when a method is not implemented/supported by the current backend
	ErrCallNotSupported = errors.New("the current call is not supported with this backend")
	// ErrNotReachable is thrown when the API cannot be reached for issuing common store operations
	ErrNotReachable = errors.New("api not reachable")
	// ErrCannotLock is thrown when there is an error acquiring a lock on a key
	ErrCannotLock = errors.New("error acquiring the lock")
	// ErrKeyModified is thrown during an atomic operation if the index does not match the one in the store
	ErrKeyModified = errors.New("unable to complete atomic operation, key modified")
	// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
	ErrKeyNotFound = errors.New("key not found in store")
	// ErrPreviousNotSpecified is thrown when the previous value is not specified for an atomic operation
	ErrPreviousNotSpecified = errors.New("previous K/V pair should be provided for the Atomic operation")
	// ErrKeyExists is thrown when the previous value exists in the case of an AtomicPut
	ErrKeyExists = errors.New("previous K/V pair exists, cannot complete Atomic operation")
)

// Options contains the options for a storage client
type Options struct {
	ClientTLS         *ClientTLSConfig
	TLS               *tls.Config
	DialTimeout       time.Duration
	Bucket            string
	PersistConnection bool
	Username          string
	Password          string
}

// ClientTLSConfig contains data for a Client TLS configuration in the form
// the etcd client wants it.  Eventually we'll adapt it for ZK and Consul.
type ClientTLSConfig struct {
	CertFile   string
	KeyFile    string
	CACertFile string
}

// Store represents the backend K/V storage
// Each store should support every call listed
// here. Or it couldn't be implemented as a K/V
// backend for libkv
type Store interface {
	// Put a value at the specified key
	Put(ctx context.Context, key string, value []byte, ttl int64) error

	// Get a value given its key
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete the value at the specified key
	Delete(ctx context.Context, key string) error

	// Exists if a Key exists in the store
	Exists(ctx context.Context, key string) (bool, error)

	// Watch for changes on a key
	Watch(ctx context.Context, key string, stopCh <-chan struct{}) (<-chan []byte, error)

	// WatchTree watches for changes on child nodes under
	// a given directory
	WatchTree(ctx context.Context, directory string, stopCh <-chan struct{}) (<-chan [][]byte, error)

	// List the content of a given prefix
	List(ctx context.Context, directory string) ([][]byte, error)

	// Close the store connection
	Close()
}

// Initialize creates a new Store object, initializing the client
type Initialize func(addrs []string, options *Options) (Store, error)

var initializers = make(map[string]Initialize)

// NewStore creates an instance of store
func NewStore(backend string, addrs []string, options *Options) (Store, error) {
	if init, exists := initializers[backend]; exists {
		return init(addrs, options)
	}

	return nil, fmt.Errorf("%s %s", ErrBackendNotSupported.Error(), backend)
}

// AddStore adds a new store backend
func AddStore(store string, init Initialize) {
	initializers[store] = init
}
