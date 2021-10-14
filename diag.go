// @author Couchbase <info@couchbase.com>
// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"io"
	"log"
)

var diag logger

type logger struct {
	*log.Logger
	verbose bool
}

func (l *logger) SetSink(w io.Writer) {
	l.Logger = log.New(w, "", 0)
}

func (l *logger) SetVerbose(verbose bool) {
	l.verbose = verbose
}

func (l *logger) Verbosef(fmt string, args ...interface{}) {
	if l.verbose {
		l.Printf(fmt, args...)
	}
}
