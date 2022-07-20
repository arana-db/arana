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
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
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
		{
			"caching_sha2_password",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "caching_sha2_password"},
			[]byte{0xcd, 0xb8, 0xda, 0xf1, 0xb7, 0x4f, 0xd, 0x91, 0x96, 0xde, 0x1b, 0x8f, 0xd8, 0xf5, 0x91, 0x1d, 0xda, 0x6c, 0x27, 0xef, 0xc6, 0x8b, 0x4a, 0xde, 0x56, 0xc9, 0x54, 0xb1, 0xe3, 0x84, 0xf3, 0x7d},
			assert.NoError,
		},
		{
			"mysql_old_password",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_old_password"},
			[]byte{0x58, 0x5f, 0x40, 0x52, 0x56, 0x42, 0x59, 0x4a, 0x0},
			assert.NoError,
		},
		{
			"mysql_clear_password",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_clear_password"},
			[]byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x0},
			assert.NoError,
		},
		{
			"mysql_native_password",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "mysql_native_password"},
			[]byte{0x48, 0x9c, 0x9e, 0x5e, 0x9, 0x2d, 0x5a, 0x82, 0x80, 0xbc, 0xb3, 0x4f, 0xf1, 0xb0, 0xec, 0x19, 0xce, 0x71, 0xb9, 0x5},
			assert.NoError,
		},
		{
			"sha256_password",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "sha256_password"},
			[]byte{1},
			assert.NoError,
		},
		{
			"default",
			fields{&Config{Passwd: "123456"}},
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "default"},
			nil,
			assert.Error,
		},
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
	type args struct {
		oldAuthData []byte
		plugin      string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"caching_sha2_password",
			args{[]byte("9761AD4D3BFD97B86287FC7A4A136D38"), "caching_sha2_password"},
			assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := createBackendConnection()
			tt.wantErr(t, conn.handleAuthResult(tt.args.oldAuthData, tt.args.plugin), fmt.Sprintf("handleAuthResult(%v, %v)", tt.args.oldAuthData, tt.args.plugin))
		})
	}
}

func TestBackendConnection_readAuthResult(t *testing.T) {
	tests := []struct {
		name    string
		want    []byte
		want1   string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
		{"readAuthResult", nil, "", assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := createBackendConnection()
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
	tests := []struct {
		name    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
		{"readResultOK", assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := createBackendConnection()
			tt.wantErr(t, conn.readResultOK(), fmt.Sprintf("readResultOK()"))
		})
	}
}

func TestBackendConnection_sendEncryptedPassword(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	type args struct {
		seed []byte
		pub  *rsa.PublicKey
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{"sendEncryptedPassword", args{[]byte("arana"), &key.PublicKey}, assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := createBackendConnection()
			tt.wantErr(t, conn.sendEncryptedPassword(tt.args.seed, tt.args.pub), fmt.Sprintf("sendEncryptedPassword(%v, %v)", tt.args.seed, tt.args.pub))
		})
	}
}

func TestBackendConnection_writeAuthSwitchPacket(t *testing.T) {
	type args struct {
		authData []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
		{"writeAuthSwitchPacket", args{[]byte("123456")}, assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := createBackendConnection()
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
		{"DeregisterServerPubKey", args{"arana"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DeregisterServerPubKey(tt.args.name)
		})
	}
}

func TestRegisterServerPubKey(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	type args struct {
		name   string
		pubKey *rsa.PublicKey
	}
	tests := []struct {
		name string
		args args
	}{
		{"RegisterServerPubKey", args{"arana", &key.PublicKey}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterServerPubKey(tt.args.name, tt.args.pubKey)
		})
	}
}

func Test_encryptPassword(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 1024)
	type args struct {
		password string
		seed     []byte
		pub      *rsa.PublicKey
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"encryptPassword",
			args{"123456", []byte{0x61, 0x72, 0x61, 0x6e, 0x61}, &key.PublicKey},
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encryptPassword(tt.args.password, tt.args.seed, tt.args.pub)
			if !tt.wantErr(t, err, fmt.Sprintf("encryptPassword(%v, %v, %v)", tt.args.password, tt.args.seed, tt.args.pub)) {
				return
			}
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
		{"getServerPubKey", args{"key"}, nil},
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
		{"NextByte", fields{uint32(11), uint32(22)}, byte(0)},
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
		{"newMyRnd", args{uint32(1), uint32(2)}, &myRnd{uint32(1), uint32(2)}},
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
		{"pwHash", args{[]byte("123456")}, [2]uint32{0x565491d7, 0x4013245}},
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
		{
			"scrambleOldPassword",
			args{[]byte{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39}, "123456"},
			[]byte{0x4d, 0x44, 0x5b, 0x4b, 0x56, 0x5e, 0x41, 0x5e},
		},
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
		{
			"scramblePassword",
			args{[]byte{0x61, 0x72, 0x61, 0x6e, 0x61}, "123456"},
			[]byte{0xb3, 0x7e, 0x9f, 0x58, 0xbf, 0xc3, 0x99, 0x64, 0xee, 0xb7, 0x57, 0x4e, 0x3f, 0xcc, 0x8d, 0xed, 0x6d, 0x9e, 0x94, 0x3b},
		},
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
		{
			"scrambleSHA256Password",
			args{[]byte{0x61, 0x72, 0x61, 0x6e, 0x61}, "123456"},
			[]byte{0x50, 0xa9, 0x42, 0x87, 0xc9, 0x8b, 0x2f, 0xe6, 0xdc, 0xfc, 0x71, 0x9, 0xad, 0x2a, 0xf1, 0xb9, 0x1e, 0x13, 0x27, 0x33, 0x6b, 0xac, 0x88, 0x97, 0xd7, 0xde, 0x25, 0x51, 0x40, 0xb0, 0x51, 0xa0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, scrambleSHA256Password(tt.args.scramble, tt.args.password), "scrambleSHA256Password(%v, %v)", tt.args.scramble, tt.args.password)
		})
	}
}

func createBackendConnection() *BackendConnection {
	dsn := "admin:123456@tcp(127.0.0.1:3306)/pass?allowAllFiles=true&allowCleartextPasswords=true"
	cfg, _ := ParseDSN(dsn)
	conn := &BackendConnection{conf: cfg}
	conn.c = newConn(new(mockConn))
	buf := make([]byte, 100)
	buf[0] = 96
	buf[4] = 3
	buf[5] = 'd'
	buf[6] = 'e'
	buf[7] = 'f'
	buf[8] = 8
	buf[9] = 't'
	buf[10] = 'e'
	buf[11] = 's'
	buf[12] = 't'
	buf[13] = 'b'
	buf[14] = 'a'
	buf[15] = 's'
	buf[16] = 'e'
	buf[17] = 9
	buf[18] = 't'
	buf[19] = 'e'
	buf[20] = 's'
	buf[21] = 't'
	buf[22] = 't'
	buf[23] = 'a'
	buf[24] = 'b'
	buf[25] = 'l'
	buf[26] = 'e'
	buf[28] = 4
	buf[29] = 'n'
	buf[30] = 'a'
	buf[31] = 'm'
	buf[32] = 'e'
	buf[37] = 255
	buf[41] = 15
	conn.c.conn.(*mockConn).data = buf
	return conn
}
