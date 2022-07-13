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

package mysql

import (
	"crypto/rsa"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBackendConnection_auth(t *testing.T) {
	type fields struct {
		conf *Config
	}
	type args struct {
		authData []byte
		plugin   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "caching_sha2_password"}, nil, nil},
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_old_password"}, nil, nil},
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_clear_password"}, nil, nil},
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_native_password"}, nil, nil},
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "sha256_password"}, nil, nil},
		{"a", fields{&Config{Passwd: "123456"}}, args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "default"}, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				conf: tt.fields.conf,
			}
			got, err := conn.auth(tt.args.authData, tt.args.plugin)
			if !tt.wantErr(t, err, fmt.Sprintf("auth(%v, %v)", tt.args.authData, tt.args.plugin)) {
				return
			}
			assert.Equalf(t, tt.want, got, "auth(%v, %v)", tt.args.authData, tt.args.plugin)
		})
	}
}

func TestBackendConnection_handleAuthResult(t *testing.T) {
	type fields struct {
		c             *Conn
		conf          *Config
		capabilities  uint32
		serverVersion string
		characterSet  uint8
	}
	type args struct {
		oldAuthData []byte
		plugin      string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				c:             tt.fields.c,
				conf:          tt.fields.conf,
				capabilities:  tt.fields.capabilities,
				serverVersion: tt.fields.serverVersion,
				characterSet:  tt.fields.characterSet,
			}
			tt.wantErr(t, conn.handleAuthResult(tt.args.oldAuthData, tt.args.plugin), fmt.Sprintf("handleAuthResult(%v, %v)", tt.args.oldAuthData, tt.args.plugin))
		})
	}
}

func TestBackendConnection_readAuthResult(t *testing.T) {
	type fields struct {
		c             *Conn
		conf          *Config
		capabilities  uint32
		serverVersion string
		characterSet  uint8
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		want1   string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				c:             tt.fields.c,
				conf:          tt.fields.conf,
				capabilities:  tt.fields.capabilities,
				serverVersion: tt.fields.serverVersion,
				characterSet:  tt.fields.characterSet,
			}
			got, got1, err := conn.readAuthResult()
			if !tt.wantErr(t, err, fmt.Sprintf("readAuthResult()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "readAuthResult()")
			assert.Equalf(t, tt.want1, got1, "readAuthResult()")
		})
	}
}

func TestBackendConnection_readResultOK(t *testing.T) {
	type fields struct {
		c             *Conn
		conf          *Config
		capabilities  uint32
		serverVersion string
		characterSet  uint8
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				c:             tt.fields.c,
				conf:          tt.fields.conf,
				capabilities:  tt.fields.capabilities,
				serverVersion: tt.fields.serverVersion,
				characterSet:  tt.fields.characterSet,
			}
			tt.wantErr(t, conn.readResultOK(), fmt.Sprintf("readResultOK()"))
		})
	}
}

func TestBackendConnection_sendEncryptedPassword(t *testing.T) {
	type fields struct {
		c             *Conn
		conf          *Config
		capabilities  uint32
		serverVersion string
		characterSet  uint8
	}
	type args struct {
		seed []byte
		pub  *rsa.PublicKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				c:             tt.fields.c,
				conf:          tt.fields.conf,
				capabilities:  tt.fields.capabilities,
				serverVersion: tt.fields.serverVersion,
				characterSet:  tt.fields.characterSet,
			}
			tt.wantErr(t, conn.sendEncryptedPassword(tt.args.seed, tt.args.pub), fmt.Sprintf("sendEncryptedPassword(%v, %v)", tt.args.seed, tt.args.pub))
		})
	}
}

func TestBackendConnection_writeAuthSwitchPacket(t *testing.T) {
	type fields struct {
		c             *Conn
		conf          *Config
		capabilities  uint32
		serverVersion string
		characterSet  uint8
	}
	type args struct {
		authData []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &BackendConnection{
				c:             tt.fields.c,
				conf:          tt.fields.conf,
				capabilities:  tt.fields.capabilities,
				serverVersion: tt.fields.serverVersion,
				characterSet:  tt.fields.characterSet,
			}
			tt.wantErr(t, conn.writeAuthSwitchPacket(tt.args.authData), fmt.Sprintf("writeAuthSwitchPacket(%v)", tt.args.authData))
		})
	}
}

func TestDeregisterServerPubKey(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DeregisterServerPubKey(tt.args.name)
		})
	}
}

func TestRegisterServerPubKey(t *testing.T) {
	type args struct {
		name   string
		pubKey *rsa.PublicKey
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterServerPubKey(tt.args.name, tt.args.pubKey)
		})
	}
}

func Test_encryptPassword(t *testing.T) {
	type args struct {
		password string
		seed     []byte
		pub      *rsa.PublicKey
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encryptPassword(tt.args.password, tt.args.seed, tt.args.pub)
			if !tt.wantErr(t, err, fmt.Sprintf("encryptPassword(%v, %v, %v)", tt.args.password, tt.args.seed, tt.args.pub)) {
				return
			}
			assert.Equalf(t, tt.want, got, "encryptPassword(%v, %v, %v)", tt.args.password, tt.args.seed, tt.args.pub)
		})
	}
}

func Test_getServerPubKey(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name       string
		args       args
		wantPubKey *rsa.PublicKey
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantPubKey, getServerPubKey(tt.args.name), "getServerPubKey(%v)", tt.args.name)
		})
	}
}

func Test_myRnd_NextByte(t *testing.T) {
	type fields struct {
		seed1 uint32
		seed2 uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &myRnd{
				seed1: tt.fields.seed1,
				seed2: tt.fields.seed2,
			}
			assert.Equalf(t, tt.want, r.NextByte(), "NextByte()")
		})
	}
}

func Test_newMyRnd(t *testing.T) {
	type args struct {
		seed1 uint32
		seed2 uint32
	}
	tests := []struct {
		name string
		args args
		want *myRnd
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, newMyRnd(tt.args.seed1, tt.args.seed2), "newMyRnd(%v, %v)", tt.args.seed1, tt.args.seed2)
		})
	}
}

func Test_pwHash(t *testing.T) {
	type args struct {
		password []byte
	}
	tests := []struct {
		name       string
		args       args
		wantResult [2]uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantResult, pwHash(tt.args.password), "pwHash(%v)", tt.args.password)
		})
	}
}

func Test_scrambleOldPassword(t *testing.T) {
	type args struct {
		scramble []byte
		password string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, scrambleOldPassword(tt.args.scramble, tt.args.password), "scrambleOldPassword(%v, %v)", tt.args.scramble, tt.args.password)
		})
	}
}

func Test_scramblePassword(t *testing.T) {
	type args struct {
		scramble []byte
		password string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, scramblePassword(tt.args.scramble, tt.args.password), "scramblePassword(%v, %v)", tt.args.scramble, tt.args.password)
		})
	}
}

func Test_scrambleSHA256Password(t *testing.T) {
	type args struct {
		scramble []byte
		password string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, scrambleSHA256Password(tt.args.scramble, tt.args.password), "scrambleSHA256Password(%v, %v)", tt.args.scramble, tt.args.password)
		})
	}
}
