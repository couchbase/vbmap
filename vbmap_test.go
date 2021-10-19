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
	"fmt"
	"math/rand"
	"reflect"
	"runtime/debug"
	"testing"
	"testing/quick"
)

var (
	testMaxFlow  = flag.Bool("maxflow", true, "run maxflow tests")
	testGlpk     = flag.Bool("glpk", false, "run glpk tests")
	testMaxCount = flag.Int("max-count", 250, "testing/quick MaxCount")
)

var (
	balancedSearchParams = SearchParams{
		NumRRetries:          25,
		StrictReplicaBalance: false,
		RelaxNumSlaves:       true,
		DotPath:              "",
	}
	unbalancedSearchParams = SearchParams{
		NumRRetries:          25,
		StrictReplicaBalance: false,
		RelaxSlaveBalance:    true,
		RelaxReplicaBalance:  true,
		RelaxNumSlaves:       true,
		BalanceSlaves:        true,
		BalanceReplicas:      true,
		DotPath:              "",
	}
)

type vbmapParams interface {
	getParams() VbmapParams
	getSearchParams() SearchParams
	mustBalance() bool
}

func testBuildRI(
	params *VbmapParams,
	searchParams SearchParams,
	gen RIGenerator) (RI, error) {

	return tryBuildRI(params, searchParams, gen)
}

func testBuildR(
	params *VbmapParams,
	searchParams SearchParams,
	gen RIGenerator) (RI, R, error) {

	ri, err := tryBuildRI(params, searchParams, gen)
	if err != nil {
		return RI{}, R{}, err
	}

	r, err := BuildR(*params, ri, searchParams)
	if err != nil {
		return RI{}, R{}, err
	}

	return ri, r, nil
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

func trivialTags(nodes int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	for n := 0; n < nodes; n++ {
		tags[Node(n)] = Tag(n)
	}

	return
}

func genVbmapParams(rand *rand.Rand) VbmapParams {
	nodes := rand.Int()%100 + 1
	replicas := rand.Int() % 4

	params := VbmapParams{
		NumNodes:    nodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: replicas,
	}
	normalizeParams(&params)
	return params
}

type trivialTagsVbmapParams VbmapParams

func (p trivialTagsVbmapParams) getParams() VbmapParams {
	return VbmapParams(p)
}

func (trivialTagsVbmapParams) getSearchParams() SearchParams {
	return balancedSearchParams
}

func (trivialTagsVbmapParams) mustBalance() bool {
	return true
}

func (trivialTagsVbmapParams) Generate(rand *rand.Rand, _ int) reflect.Value {
	params := genVbmapParams(rand)
	params.Tags = trivialTags(params.NumNodes)
	return reflect.ValueOf(trivialTagsVbmapParams(params))
}

type equalTagsVbmapParams VbmapParams

func (p equalTagsVbmapParams) getParams() VbmapParams {
	return VbmapParams(p)
}

func (equalTagsVbmapParams) getSearchParams() SearchParams {
	return balancedSearchParams
}

func (equalTagsVbmapParams) mustBalance() bool {
	return true
}

func (equalTagsVbmapParams) Generate(rand *rand.Rand, _ int) reflect.Value {
	params := genVbmapParams(rand)

	// number of tags is in range [numReplicas+1, numNodes]
	numTags := rand.Int() % (params.NumNodes - params.NumReplicas)
	numTags += params.NumReplicas + 1

	if params.NumNodes%numTags != 0 {
		params.NumNodes /= numTags
		params.NumNodes *= numTags
	}

	params.Tags = equalTags(params.NumNodes, numTags)
	normalizeParams(&params)

	return reflect.ValueOf(equalTagsVbmapParams(params))
}

type randomTagsVbmapParams VbmapParams

func (p randomTagsVbmapParams) getParams() VbmapParams {
	return VbmapParams(p)
}

func (randomTagsVbmapParams) getSearchParams() SearchParams {
	return unbalancedSearchParams
}

func (randomTagsVbmapParams) mustBalance() bool {
	return false
}

func (randomTagsVbmapParams) Generate(rand *rand.Rand, _ int) reflect.Value {
	params := genVbmapParams(rand)

	numTags := rand.Int() % (params.NumNodes - params.NumReplicas)
	numTags += params.NumReplicas + 1

	histo := make([]int, numTags)
	for i := range histo {
		histo[i] = 1
	}

	for i := 0; i < params.NumNodes-numTags; i++ {
		histo[rand.Int()%numTags]++
	}

	tags := make(map[Node]Tag)
	node := 0
	for i, count := range histo {
		tag := Tag(i)
		for j := 0; j < count; j++ {
			tags[Node(node)] = tag
			node++
		}
	}

	params.Tags = tags
	return reflect.ValueOf(randomTagsVbmapParams(params))
}

func TestRReplicaBalance(t *testing.T) {
	t.Parallel()

	for nodes := 1; nodes <= 100; nodes++ {
		tags := trivialTags(nodes)

		for replicas := 1; replicas <= 3; replicas++ {
			params := VbmapParams{
				Tags:        tags,
				NumNodes:    nodes,
				NumSlaves:   10,
				NumVBuckets: 1024,
				NumReplicas: replicas,
			}

			normalizeParams(&params)

			for _, gen := range allGenerators() {
				_, _, err :=
					testBuildR(
						&params,
						balancedSearchParams,
						gen)
				if err != nil {
					t.Error("Could not find a solution "+
						"for %d nodes, %d replicas",
						nodes, replicas)
				}
			}
		}
	}
}

func checkRIProperties(gen RIGenerator, p vbmapParams) bool {
	params := p.getParams()
	numSlaves := params.NumSlaves

	ri, err := testBuildRI(&params, p.getSearchParams(), gen)
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
		count := 0
		for j, elem := range row {
			colSums[j] += elem
			rowSums[i] += elem

			if elem != 0 {
				count++
			}
		}

		if count > numSlaves {
			return false
		}
	}

	mustBalance := p.mustBalance()
	for i := range colSums {
		if mustBalance && colSums[i] != params.NumSlaves {
			return false
		}

		if rowSums[i] != params.NumSlaves {
			return false
		}
	}

	return true
}

func TestRIProperties(t *testing.T) {
	qc := newQc(t)
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRIProperties)
}

func checkRProperties(gen RIGenerator, p vbmapParams) bool {
	params := p.getParams()
	mustBalance := p.mustBalance()

	ri, r, err := testBuildR(&params, p.getSearchParams(), gen)
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

		return true
	}

	// check that we follow RI topology
	for i, row := range ri.Matrix {
		for j, elem := range row {
			if elem == 0 && r.Matrix[i][j] != 0 {
				return false
			}

			if mustBalance &&
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

	totalReplicas := 0
	targetReplicasLo := params.NumVBuckets / params.NumNodes
	targetReplicasHi := targetReplicasLo
	if targetReplicasLo*params.NumNodes != params.NumVBuckets {
		targetReplicasHi++
	}

	targetReplicasLo *= params.NumReplicas
	targetReplicasHi *= params.NumReplicas

	for _, sum := range r.ColSums {
		totalReplicas += sum

		if mustBalance &&
			(sum < targetReplicasLo || sum > targetReplicasHi) {
			return false
		}
	}

	if totalReplicas != params.NumVBuckets*params.NumReplicas {
		return false
	}

	return true
}

func TestRProperties(t *testing.T) {
	qc := newQc(t)
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRProperties)
}

type nodePair struct {
	x, y Node
}

func checkVbmapProperties(gen RIGenerator, p vbmapParams) bool {
	params := p.getParams()

	ri, r, err := testBuildR(&params, p.getSearchParams(), gen)
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
	qc := newQc(t)
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkVbmapProperties)
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

func checkRIPropertiesTagAware(gen RIGenerator, p vbmapParams) bool {
	params := p.getParams()

	ri, err := testBuildRI(&params, p.getSearchParams(), gen)
	if err != nil {
		return false
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
	qc := newQc(t)
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRIProperties)
}

func checkVbmapTagAware(gen RIGenerator, p vbmapParams) bool {
	params := p.getParams()

	_, r, err := testBuildR(&params, p.getSearchParams(), gen)
	if err != nil {
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
				return false
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
	qc := newQc(t)
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRIProperties)
}

// Calls quick.Check() with the function and the config passed. Converts
// panics into test failures and logs the stacktrace. This way it's possible
// to see what input caused the panic.
func quickCheck(fn interface{}, t *testing.T) error {
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
	config := quick.Config{MaxCount: *testMaxCount}
	check := reflect.MakeFunc(reflect.TypeOf(fn), wrapper).Interface()
	return quick.Check(check, &config)
}

type qc struct {
	t  *testing.T
	ps []vbmapParams
}

func newQc(t *testing.T) *qc {
	return &qc{t, nil}
}

func (q *qc) testOn(p vbmapParams) {
	q.ps = append(q.ps, p)
}

func (q qc) run(fn interface{}) {
	q.t.Parallel()

	for _, gen := range allGenerators() {
		for _, p := range q.ps {
			check := makeChecker(gen, p, fn)
			pName := reflect.TypeOf(p).Name()
			name := fmt.Sprintf("%s+%s", gen, pName)

			q.t.Run(name,
				func(t *testing.T) {
					t.Parallel()
					err := quickCheck(check, t)
					if err != nil {
						t.Error(err)
					}
				})
		}
	}
}

// Makes a function that can be passed to quickCheck(). The values are
// generated for the specific type as specified by `params`. The callback
// function `check` will receive the value via `vbmapParams` interface
// though. This allows writing loops over different generators for
// `VbmapParams`.
func makeChecker(
	gen RIGenerator, p vbmapParams, check interface{}) interface{} {

	checkType := reflect.TypeOf(check)
	in := make([]reflect.Type, checkType.NumIn()-1)

	in[0] = reflect.TypeOf(p)
	for i := 1; i < checkType.NumIn()-1; i++ {
		in[i] = checkType.In(i + 1)
	}

	out := []reflect.Type(nil)
	for i := 0; i < checkType.NumOut(); i++ {
		out = append(out, checkType.Out(i))
	}

	fnType := reflect.FuncOf(in, out, false)

	fn := func(args0 []reflect.Value) []reflect.Value {
		args := []reflect.Value(nil)
		args = append(args, reflect.ValueOf(gen))
		args = append(args, args0...)
		return reflect.ValueOf(check).Call(args)
	}

	return reflect.MakeFunc(fnType, fn).Interface()
}
