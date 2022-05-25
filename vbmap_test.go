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
	testGreedy   = flag.Bool("greedy", true, "run greedy tests")
	testMaxCount = flag.Int("max-count", 50, "testing/quick MaxCount")
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

func defaultGenerators() []RIGenerator {
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
	replicas := rand.Int()%3 + 1

	params := VbmapParams{
		NumNodes:    nodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: replicas,
		Tags:        trivialTags(nodes),
	}
	normalizeParams(&params, false)
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
	return reflect.ValueOf(trivialTagsVbmapParams(genVbmapParams(rand)))
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

	numTags := rand.Int()%params.NumNodes + 1

	if params.NumNodes%numTags != 0 {
		params.NumNodes /= numTags
		params.NumNodes *= numTags
	}

	params.Tags = equalTags(params.NumNodes, numTags)
	normalizeParams(&params, false)

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

	numTags := rand.Int()%params.NumNodes + 1

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
	normalizeParams(&params, false)
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

			normalizeParams(&params, false)

			for _, gen := range defaultGenerators() {
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

func checkRIProperties(gen RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()
	numSlaves := params.NumSlaves

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

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
	qc.addDefaultGenerators()
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRIProperties)
}

func doCheckGreedyRIProperties(prevRI RIMap, params VbmapParams) (res bool) {

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

	ri := generateRI(&params, prevRI)

	numNodes := params.NumNodes
	numSlaves := params.NumSlaves
	nodeTagMap := params.Tags
	tagsNodesMap := params.Tags.TagsNodesMap()

	// Check all the active nodes are present in the ri.

	if len(ri) != numNodes {
		return false
	}

	for _, active := range params.Nodes() {
		if _, ok := ri[active]; !ok {
			return false
		}
	}

	// Check none of the slave nodes are in the same tag as the active.

	for active, slaveMap := range ri {
		activeTag := nodeTagMap[active]
		for slave, _ := range slaveMap {
			if activeTag == nodeTagMap[slave] {
				return false
			}
		}
	}

	// Ensure maximal number of slaves are picked for each node.

	for active, slaveMap := range ri {
		activeTag := nodeTagMap[active]
		maxSlaveNodes := Min(numNodes-len(tagsNodesMap[activeTag]), numSlaves)
		if len(slaveMap) != maxSlaveNodes {
			return false
		}
	}

	// Ensure a slave has been picked from each possible tag.

	maxSlaveTags := params.Tags.TagsCount() - 1

	// If the number of tags are larger than the numSlaves, a slave can not
	// be picked from each of the tags; avoid the below check.

	if maxSlaveTags <= numSlaves {
		for _, slaveMap := range ri {
			tagsSeen := make(map[Tag]bool)
			for slave, _ := range slaveMap {
				tagsSeen[nodeTagMap[slave]] = true
			}
			if len(tagsSeen) != maxSlaveTags {
				return false
			}
		}
	}

	return true
}

func tagsCopy(src TagMap) TagMap {
	dst := make(TagMap)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func generateNumReplicasIncParams(params VbmapParams) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	newParams.NumReplicas += 1
	normalizeParams(&newParams, true)
	return newParams
}

func generateNumReplicasDecParams(params VbmapParams) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	if newParams.NumReplicas > 1 {
		newParams.NumReplicas -= 1
	}
	normalizeParams(&newParams, true)
	return newParams
}

func generateTagNodesIncParams(params VbmapParams) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	nodeId := Node(newParams.NumNodes)

	for _, tag := range params.Tags.TagsList() {
		newParams.Tags[nodeId] = tag
		nodeId++
	}

	newParams.NumNodes = len(newParams.Tags)
	normalizeParams(&newParams, true)

	return newParams
}

func generateTagNodesDecParams(params VbmapParams) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	tagsNodesMap := params.Tags.TagsNodesMap()

	for _, tag := range params.Tags.TagsList() {
		tagNodes := tagsNodesMap[tag]
		if len(tagNodes) > 1 {
			firstNode := tagNodes[0]
			delete(newParams.Tags, firstNode)
		}
	}

	newParams.NumNodes = len(newParams.Tags)
	normalizeParams(&newParams, true)

	return newParams
}

func generateTagIncParams(params VbmapParams, numNewNodes int) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	nodeId := newParams.NumNodes

	newTag := newParams.Tags.TagsCount()
	for n := 0; n < numNewNodes; n++ {
		newParams.Tags[Node(nodeId)] = Tag(newTag)
		nodeId++
	}

	newParams.NumNodes = len(newParams.Tags)
	normalizeParams(&newParams, true)

	return newParams
}

func generateTagDecParams(params VbmapParams) VbmapParams {
	newParams := params
	newParams.Tags = tagsCopy(params.Tags)

	maxTag := Tag(newParams.Tags.TagsCount() - 1)

	for _, node := range newParams.Tags.TagsNodesMap()[maxTag] {
		delete(newParams.Tags, node)
	}

	newParams.NumNodes = len(newParams.Tags)
	normalizeParams(&newParams, true)

	return newParams
}

func checkGreedyRIProperties(_ RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()
	normalizeParams(&params, true)

	// Check RI properties for a brand new RI generated without any prevRI.

	if !doCheckGreedyRIProperties(nil, params) {
		return false
	}

	prevRI := generateRI(&params, nil)

	// Modify the numReplicas and generateRI.

	if !doCheckGreedyRIProperties(
		prevRI, generateNumReplicasIncParams(params)) {
		return false
	}

	if !doCheckGreedyRIProperties(
		prevRI, generateNumReplicasDecParams(params)) {
		return false
	}

	// Modify the numNodes and the corresponding nodes in each Tag.

	if !doCheckGreedyRIProperties(
		prevRI, generateTagNodesIncParams(params)) {
		return false
	}

	if !doCheckGreedyRIProperties(
		prevRI, generateTagNodesDecParams(params)) {
		return false
	}

	// Add new nodes with a new Tag

	for numNewNodes := 0; numNewNodes < 3; numNewNodes++ {
		if !doCheckGreedyRIProperties(
			prevRI, generateTagIncParams(params, numNewNodes)) {
			return false
		}
	}

	// Remove a Tag and all of its nodes.

	if !doCheckGreedyRIProperties(
		prevRI, generateTagDecParams(params)) {
		return false
	}

	return true
}

func TestGreedyRIProperties(t *testing.T) {
	qc := newQc(t)
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.addGreedyGenerator()
	qc.run(checkGreedyRIProperties)
}

func doCheckGreedyVbmapProperties(
	prevVbmap Vbmap, params VbmapParams) (res bool) {

	defer func() {
		if !res {
			panic("docheckGreedyVbmapProperties Failed")
		}
	}()

	if params.Tags.TagsCount() <= 1 {
		return true
	}

	nodesTagMap := params.Tags

	vbmap, err := generateVbmapGreedy(params, prevVbmap)

	if err != nil {
		return false
	}

	if len(vbmap) != params.NumVBuckets {
		return false
	}

	if len(vbmap[0]) != params.NumReplicas+1 {
		return false
	}

	// chain-level checks.

	for _, chain := range vbmap {
		// Check the active and replicas are all on different nodes for each
		// chain.
		if usedSameNodeTwice(chain) {
			return false
		}

		// Check active and replicas are in different tags for each chain.
		activeTag := nodesTagMap[chain[0]]
		for _, r := range chain[1:] {
			replicaTag := nodesTagMap[r]
			if replicaTag == activeTag {
				return false
			}
		}
	}

	return true
}

func checkGreedyVbmapProperties(_ RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()
	normalizeParams(&params, true)

	if !doCheckGreedyVbmapProperties(nil, params) {
		return false
	}

	prevVbmap, err := generateVbmapGreedy(params, nil)

	if err != nil {
		return false
	}

	// Modify the numReplicas and generateRI.

	if !doCheckGreedyVbmapProperties(
		prevVbmap, generateNumReplicasIncParams(params)) {
		return false
	}

	if !doCheckGreedyVbmapProperties(
		prevVbmap, generateNumReplicasDecParams(params)) {
		return false
	}

	// Modify the numNodes and the corresponding nodes in each Tag.

	if !doCheckGreedyVbmapProperties(
		prevVbmap, generateTagNodesIncParams(params)) {
		return false
	}

	if !doCheckGreedyVbmapProperties(
		prevVbmap, generateTagNodesDecParams(params)) {
		return false
	}

	// Add new nodes with a new Tag

	for numNewNodes := 0; numNewNodes < 3; numNewNodes++ {
		if !doCheckGreedyVbmapProperties(
			prevVbmap, generateTagIncParams(params, numNewNodes)) {
			return false
		}
	}

	// Remove a Tag and all of its nodes.

	if !doCheckGreedyVbmapProperties(
		prevVbmap, generateTagDecParams(params)) {
		return false
	}

	return true
}

func TestGreedyVbmapProperties(t *testing.T) {
	qc := newQc(t)
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.addGreedyGenerator()
	qc.run(checkGreedyVbmapProperties)
}

func checkRProperties(gen RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()
	mustBalance := p.mustBalance()

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

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
	qc.addDefaultGenerators()
	qc.testOn(trivialTagsVbmapParams{})
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRProperties)
}

type nodePair struct {
	x, y Node
}

func checkVbmapProperties(gen RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

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
	qc.addDefaultGenerators()
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

func checkRIPropertiesTagAware(gen RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

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
	qc.addDefaultGenerators()
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkRIPropertiesTagAware)
}

func checkVbmapTagAware(gen RIGenerator, p vbmapParams) (res bool) {
	params := p.getParams()

	defer func() {
		if !res {
			panic("Check Failed")
		}
	}()

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
	qc.addDefaultGenerators()
	qc.testOn(equalTagsVbmapParams{})
	qc.testOn(randomTagsVbmapParams{})
	qc.run(checkVbmapTagAware)
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
	t          *testing.T
	ps         []vbmapParams
	generators []RIGenerator
}

func newQc(t *testing.T) *qc {
	return &qc{t, nil, nil}
}

func (q *qc) addDefaultGenerators() {
	q.generators = append(q.generators, defaultGenerators()...)
}

func (q *qc) addGreedyGenerator() {
	if *testGreedy {
		q.generators = append(q.generators, makeGreedyRIGenerator())
	}
}

func (q *qc) testOn(p vbmapParams) {
	q.ps = append(q.ps, p)
}

func (q qc) run(fn interface{}) {
	q.t.Parallel()

	for _, gen := range q.generators {
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
