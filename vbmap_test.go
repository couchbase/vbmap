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
	"flag"
	"math/rand"
	"reflect"
	"runtime/debug"
	"testing"
	"testing/quick"
	"time"
)

var (
	testMaxFlow = flag.Bool("maxflow", true, "run maxflow tests")
	testGlpk    = flag.Bool("glpk", false, "run glpk tests")
)

var (
	testSearchParams = SearchParams{
		NumRRetries:          25,
		StrictReplicaBalance: false,
		RelaxNumSlaves:       false,
		DotPath:              "",
	}
)

func testBuildRI(params VbmapParams, gen RIGenerator) (RI, error) {
	return gen.Generate(params, testSearchParams)
}

func testBuildR(params VbmapParams, gen RIGenerator) (RI, R, error) {
	ri, err := tryBuildRI(&params, testSearchParams, gen)
	if err != nil {
		return RI{}, R{}, err
	}

	r, err := BuildR(params, ri, testSearchParams)
	if err != nil {
		return RI{}, R{}, err
	}

	return ri, r, nil
}

type testingWriter struct {
	t *testing.T
}

func allGenerators() []RIGenerator {
	result := []RIGenerator{}

	if *testMaxFlow {
		result = append(result, makeMaxFlowRIGenerator())
	}

	if *testGlpk {
		result = append(result, makeGlpkRIGenerator())
	}

	return result
}

func (w testingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf("%s", string(p))
	return len(p), nil
}

func setup(t *testing.T) {
	diag.SetSink(testingWriter{t})
	t.Parallel()
}

func trivialTags(nodes int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	for n := 0; n < nodes; n++ {
		tags[Node(n)] = Tag(n)
	}

	return
}

func TestRReplicaBalance(t *testing.T) {
	setup(t)

	for nodes := 1; nodes <= 100; nodes++ {
		tags := trivialTags(nodes)

		for replicas := 1; replicas <= 3; replicas++ {
			seed = time.Now().UTC().UnixNano()
			rand.Seed(seed)

			t.Log("=======================================")
			t.Logf("Generating R for %d node, %d replicas (seed %d)",
				nodes, replicas, seed)

			params := VbmapParams{
				Tags:        tags,
				NumNodes:    nodes,
				NumSlaves:   10,
				NumVBuckets: 1024,
				NumReplicas: replicas,
			}

			normalizeParams(&params)

			for _, gen := range allGenerators() {
				_, _, err := testBuildR(params, gen)
				if err != nil {
					t.Error("Could not find a solution")
				}
			}
		}
	}
}

func (VbmapParams) generate(rand *rand.Rand, _ int) VbmapParams {
	nodes := rand.Int()%100 + 1
	replicas := rand.Int() % 4

	params := VbmapParams{
		Tags:        trivialTags(nodes),
		NumNodes:    nodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: replicas,
	}
	normalizeParams(&params)

	return params
}

func (p VbmapParams) Generate(rand *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(p.generate(rand, size))
}

func checkRIProperties(gen RIGenerator, params VbmapParams) bool {
	ri, err := testBuildRI(params, gen)
	if err != nil {
		return false
	}

	if len(ri.Matrix) != params.NumNodes {
		return false
	}

	if len(ri.Matrix[0]) != params.NumNodes {
		return false
	}

	colSums := make([]int, params.NumNodes)
	rowSums := make([]int, params.NumNodes)

	for i, row := range ri.Matrix {
		for j, elem := range row {
			colSums[j] += elem
			rowSums[i] += elem
		}
	}

	for i := range colSums {
		if colSums[i] != params.NumSlaves {
			return false
		}

		if rowSums[i] != params.NumSlaves {
			return false
		}
	}

	return true
}

func TestRIProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators() {
		check := func(params VbmapParams) bool {
			return checkRIProperties(gen, params)
		}

		err := quickCheck(check, &quick.Config{MaxCount: 250}, t)
		if err != nil {
			t.Error(err)
		}
	}
}

func checkRProperties(gen RIGenerator, params VbmapParams, seed int64) bool {
	rand.Seed(seed)

	ri, r, err := testBuildR(params, gen)
	if err != nil {
		return false
	}

	if params.NumReplicas == 0 {
		// no replicas? R should obviously be empty
		for _, row := range r.Matrix {
			for _, elem := range row {
				if elem != 0 {
					return false
				}
			}
		}
	} else {
		// check that we follow RI topology
		for i, row := range ri.Matrix {
			for j, elem := range row {
				if elem == 0 && r.Matrix[i][j] != 0 ||
					elem != 0 && r.Matrix[i][j] == 0 {
					return false
				}
			}
		}

		totalVBuckets := 0

		// check active vbuckets balance
		for _, sum := range r.RowSums {
			if sum%params.NumReplicas != 0 {
				return false
			}

			vbuckets := sum / params.NumReplicas
			expected := params.NumVBuckets / params.NumNodes

			if vbuckets != expected && vbuckets != expected+1 {
				return false
			}

			totalVBuckets += vbuckets
		}

		if totalVBuckets != params.NumVBuckets {
			return false
		}
	}

	return true
}

func TestRProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators() {

		check := func(params VbmapParams, seed int64) bool {
			return checkRProperties(gen, params, seed)
		}

		err := quickCheck(check, &quick.Config{MaxCount: 250}, t)
		if err != nil {
			t.Error(err)
		}
	}
}

type nodePair struct {
	x, y Node
}

func checkVbmapProperties(gen RIGenerator, params VbmapParams, seed int64) bool {
	rand.Seed(seed)

	ri, r, err := testBuildR(params, gen)
	if err != nil {
		return false
	}

	vbmap := buildVbmap(r)

	if len(vbmap) != params.NumVBuckets {
		return false
	}

	activeVBuckets := make([]int, params.NumNodes)
	replicaVBuckets := make([]int, params.NumNodes)

	replications := make(map[nodePair]int)

	for _, chain := range vbmap {
		if len(chain) != params.NumReplicas+1 {
			return false
		}

		master := chain[0]
		activeVBuckets[int(master)]++

		for _, replica := range chain[1:] {
			// all the replications correspond to ones
			// defined by R
			if ri.Matrix[int(master)][int(replica)] == 0 {
				return false
			}

			replicaVBuckets[int(replica)]++

			pair := nodePair{master, replica}
			if _, present := replications[pair]; !present {
				replications[pair] = 0
			}

			replications[pair]++
		}
	}

	// number of replications should correspond to one defined by R
	for i, row := range r.Matrix {
		for j, elem := range row {
			if elem != 0 {
				pair := nodePair{Node(i), Node(j)}

				count, present := replications[pair]
				if !present || count != elem {
					return false
				}
			}
		}
	}

	// number of active vbuckets should correspond to one defined
	// by matrix R
	for n, sum := range r.RowSums {
		if params.NumReplicas != 0 {
			// if we have at least one replica then number
			// of active vbuckets is defined by matrix R
			if sum/params.NumReplicas != activeVBuckets[n] {
				return false
			}
		} else {
			// otherwise matrix R is empty and we just
			// need to check that active vbuckets are
			// distributed evenly
			expected := params.NumVBuckets / params.NumNodes
			if activeVBuckets[n] != expected &&
				activeVBuckets[n] != expected+1 {
				return false
			}
		}
	}

	// number of replica vbuckets should correspond to one defined
	// by R
	for n, sum := range r.ColSums {
		if sum != replicaVBuckets[n] {
			return false
		}
	}

	return true
}

func TestVbmapProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators() {

		check := func(params VbmapParams, seed int64) bool {
			return checkVbmapProperties(gen, params, seed)
		}

		err := quickCheck(check, &quick.Config{MaxCount: 250}, t)
		if err != nil {
			t.Error(err)
		}

	}
}

func equalTags(numNodes int, numTags int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	tagSize := numNodes / numTags

	if tagSize*numTags != numNodes {
		panic("numNodes % numTags != 0")
	}

	node := 0
	tag := 0
	for numTags > 0 {
		for i := 0; i < tagSize; i++ {
			tags[Node(node)] = Tag(tag)
			node++
		}

		tag++
		numTags--
	}

	return
}

type equalTagsVbmapParams struct {
	VbmapParams
}

func (p equalTagsVbmapParams) Generate(
	rand *rand.Rand, size int) reflect.Value {

	params := p.VbmapParams.generate(rand, size)

	// number of tags is in range [numReplicas+1, numNodes]
	numTags := rand.Int() % (params.NumNodes - params.NumReplicas)
	numTags += params.NumReplicas + 1

	if params.NumNodes%numTags != 0 {
		params.NumNodes /= numTags
		params.NumNodes *= numTags
	}

	params.Tags = equalTags(params.NumNodes, numTags)
	normalizeParams(&params)

	return reflect.ValueOf(equalTagsVbmapParams{params})
}

func checkRIPropertiesTagAware(gen RIGenerator, params VbmapParams) bool {
	ri, err := testBuildRI(params, gen)
	if err != nil {
		switch err {
		case ErrorNoSolution:
			diag.Printf("Couldn't find a solution for params %s", params)

			// cross-check using glpk generator
			gen := makeGlpkRIGenerator()

			// if glpk can find a solution then report an error
			_, err := testBuildRI(params, gen)
			if err == nil {
				return false
			}

			return true
		default:
			return false
		}
	}

	for i, row := range ri.Matrix {
		for j, elem := range row {
			if elem != 0 {
				if params.Tags[Node(i)] == params.Tags[Node(j)] {
					return false
				}
			}
		}
	}

	return true
}

func TestRIPropertiesTagAware(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators() {
		check := func(params equalTagsVbmapParams) bool {
			return checkRIPropertiesTagAware(gen, params.VbmapParams)
		}

		err := quickCheck(check, &quick.Config{MaxCount: 250}, t)
		if err != nil {
			t.Error(err)
		}

	}
}

func checkVbmapTagAware(gen RIGenerator, params VbmapParams) bool {
	_, r, err := testBuildR(params, gen)
	if err != nil {
		if err == ErrorNoSolution {
			diag.Printf("Couldn't find a solution for params %s", params)
			return true
		}

		return false
	}

	for i, row := range r.Matrix {
		counts := make(map[Tag]int)

		for j, vbs := range row {
			if vbs == 0 {
				continue
			}

			tag := params.Tags[Node(j)]
			count, _ := counts[tag]
			counts[tag] = count + vbs
		}

		var vbuckets int
		if params.NumReplicas != 0 {
			vbuckets = r.RowSums[i] / params.NumReplicas
		}

		for _, count := range counts {
			if count > vbuckets {
				diag.Printf("Can't generate fully rack aware "+
					"vbucket map for params %s", params)
				return true
			}
		}
	}

	vbmap := buildVbmap(r)
	for _, chain := range vbmap {
		tags := make(map[Tag]bool)

		for _, node := range chain {
			tag := params.Tags[node]
			tags[tag] = true
		}

		if len(chain) != len(tags) {
			return false
		}
	}

	return true
}

func TestVbmapTagAware(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators() {
		check := func(params equalTagsVbmapParams) bool {
			return checkVbmapTagAware(gen, params.VbmapParams)
		}

		err := quickCheck(check, &quick.Config{MaxCount: 250}, t)
		if err != nil {
			t.Error(err)
		}

	}
}

// Calls quick.Check() with the function and the config passed. Converts
// panics into test failures and logs the stacktrace. This way it's possible
// to see what input caused the panic.
func quickCheck(fn interface{}, config *quick.Config, t *testing.T) error {
	wrapper := func(args []reflect.Value) (results []reflect.Value) {
		defer func() {
			if err := recover(); err != nil {
				t.Logf("%v\n%s", err, debug.Stack())
				results = []reflect.Value{
					reflect.ValueOf(false),
				}
			}
		}()

		results = reflect.ValueOf(fn).Call(args)
		return
	}
	check := reflect.MakeFunc(reflect.TypeOf(fn), wrapper).Interface()
	return quick.Check(check, config)
}
