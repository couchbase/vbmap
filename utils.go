// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
)

func Shuffle(a []int) {
	for i := range a {
		j := i + rand.Intn(len(a)-i)
		a[i], a[j] = a[j], a[i]
	}
}

func SpreadSum(sum int, n int) (result []int) {
	result = make([]int, n)

	quot := sum / n
	rem := sum % n

	for i := range result {
		result[i] = quot
		if rem != 0 {
			rem--
			result[i]++
		}
	}

	return
}

func B2i(b bool) int {
	if b {
		return 1
	}

	return 0
}

func Min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func Max(a, b int) int {
	if a >= b {
		return a
	}

	return b
}

func ppStructFields(indent string, v interface{}) string {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Struct {
		panic(fmt.Sprintf("called on non-struct %v", v))
	}

	buffer := &bytes.Buffer{}

	for i := 0; i < value.NumField(); i++ {
		fmt.Fprintf(
			buffer,
			"%s%s: %v\n",
			indent,
			value.Type().Field(i).Name,
			value.Field(i))
	}

	return buffer.String()
}
