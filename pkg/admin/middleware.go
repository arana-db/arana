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

package admin

import (
	"reflect"
)

import (
	"github.com/gin-gonic/gin"
)

// ErrorHandler is middleware that enables you to configure error handling from a centralized place via its fluent API.
// Copyright: https://github.com/josephwoodward/gin-errorhandling
func ErrorHandler(errMap ...*errorMapping) gin.HandlerFunc {
	return func(context *gin.Context) {
		context.Next()

		lastErr := context.Errors.Last()
		if lastErr == nil {
			return
		}

		for _, e := range errMap {
			for _, e2 := range e.fromErrors {
				if lastErr.Err == e2 {
					e.toResponse(context, lastErr.Err)
				} else if isType(lastErr.Err, e2) {
					e.toResponse(context, lastErr.Err)
				}
			}
		}
	}
}

func isType(a, b interface{}) bool {
	return reflect.TypeOf(a) == reflect.TypeOf(b)
}

type errorMapping struct {
	fromErrors   []error
	toStatusCode int
	toResponse   func(ctx *gin.Context, err error)
}

// ToStatusCode specifies the status code returned to a caller when the error is handled.
func (r *errorMapping) ToStatusCode(statusCode int) *errorMapping {
	r.toStatusCode = statusCode
	r.toResponse = func(ctx *gin.Context, err error) {
		ctx.Status(statusCode)
	}
	return r
}

// ToResponse provides more control over the returned response when an error is matched.
func (r *errorMapping) ToResponse(response func(ctx *gin.Context, err error)) *errorMapping {
	r.toResponse = response
	return r
}

// Map enables you to map errors to a given response status code or response body.
func Map(err ...error) *errorMapping {
	return &errorMapping{
		fromErrors: err,
	}
}
