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
	"math"
	"math/rand"
	"time"
)

type Vbmap [][]Node

func (vbmap Vbmap) String() string {
	buffer := &bytes.Buffer{}

	for i, nodes := range vbmap {
		fmt.Fprintf(buffer, "%4d: ", i)
		for _, n := range nodes {
			fmt.Fprintf(buffer, "%3d ", n)
		}
		fmt.Fprintf(buffer, "\n")
	}

	return buffer.String()
}

func makeVbmap(params VbmapParams) (vbmap Vbmap) {
	vbmap = make([][]Node, params.NumVBuckets)
	for v := 0; v < params.NumVBuckets; v++ {
		vbmap[v] = make([]Node, params.NumReplicas+1)
	}

	return
}

type chainCost struct {
	rawCost       int
	tagViolations int
}

var (
	inf = chainCost{math.MaxInt, math.MaxInt}
)

func (c chainCost) cmp(other chainCost) int {
	switch {
	case c.tagViolations > other.tagViolations:
		return 1
	case c.tagViolations < other.tagViolations:
		return -1
	default:
		switch {
		case c.rawCost > other.rawCost:
			return 1
		case c.rawCost < other.rawCost:
			return -1
		}
	}

	return 0
}

func (c chainCost) less(other chainCost) bool {
	return c.cmp(other) == -1
}

func (c chainCost) plus(other chainCost) (result chainCost) {
	if c == inf || other == inf {
		return inf
	}

	result.tagViolations = c.tagViolations + other.tagViolations
	result.rawCost = c.rawCost + other.rawCost

	return
}

func (c chainCost) div(denom int) (result chainCost) {
	if c == inf {
		return inf
	}

	result.tagViolations = c.tagViolations
	result.rawCost = c.rawCost / denom

	return
}

func (c chainCost) String() string {
	if c == inf {
		return "inf"
	}

	return fmt.Sprintf("{%d %d}", c.rawCost, c.tagViolations)
}

type slavePair struct {
	x, y     Node
	distance int
}

type tagPair struct {
	x, y     Tag
	distance int
}

type pairStats struct {
	slaveStats map[slavePair]int
	tagStats   map[tagPair]int
}

func makePairStats() (stats *pairStats) {
	stats = new(pairStats)
	stats.slaveStats = make(map[slavePair]int)
	stats.tagStats = make(map[tagPair]int)

	return
}

func (s *pairStats) getSlaveStat(x, y Node, distance int) int {
	pair := slavePair{x, y, distance}
	stat, _ := s.slaveStats[pair]

	return stat
}

func (s *pairStats) getTagStat(x, y Tag, distance int) int {
	tagPair := tagPair{x, y, distance}
	stat, _ := s.tagStats[tagPair]

	return stat
}

func (s *pairStats) notePair(x, y Node, xTag, yTag Tag, distance int) {
	pair := slavePair{x, y, distance}

	count, _ := s.slaveStats[pair]
	s.slaveStats[pair] = count + 1

	tagPair := tagPair{xTag, yTag, distance}
	tagCount, _ := s.tagStats[tagPair]
	s.tagStats[tagPair] = tagCount + 1
}

type selectionCtx struct {
	params VbmapParams

	master   Node
	vbuckets int
	slaves   []Node

	slaveCounts map[Node]int
	tagCounts   map[Tag]int

	stats *pairStats
}

func makeSelectionCtx(params VbmapParams, master Node,
	vbuckets int, stats *pairStats) (ctx *selectionCtx) {

	ctx = &selectionCtx{}

	ctx.params = params
	ctx.slaveCounts = make(map[Node]int)
	ctx.tagCounts = make(map[Tag]int)
	ctx.master = master
	ctx.vbuckets = vbuckets
	ctx.stats = stats

	return
}

func (ctx *selectionCtx) getSlaveStat(x, y Node, distance int) int {
	return ctx.stats.getSlaveStat(x, y, distance)
}

func (ctx *selectionCtx) getTagStat(x, y Node, distance int) int {
	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	return ctx.stats.getTagStat(xTag, yTag, distance)
}

func (ctx *selectionCtx) addSlave(node Node, count int) {
	if _, present := ctx.slaveCounts[node]; present {
		panic("duplicated slave")
	}

	ctx.slaves = append(ctx.slaves, node)
	ctx.slaveCounts[node] = count

	tag := ctx.params.Tags[node]
	tagCount, _ := ctx.tagCounts[tag]
	ctx.tagCounts[tag] = tagCount + count
}

func (ctx *selectionCtx) notePair(x, y Node, distance int) {
	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	ctx.stats.notePair(x, y, xTag, yTag, distance)
}

func (ctx *selectionCtx) noteChain(chain []Node) {
	for i, node := range chain {
		tag := ctx.params.Tags[node]

		nodeCount := ctx.slaveCounts[node]
		tagCount := ctx.tagCounts[tag]

		if nodeCount == 0 || tagCount == 0 {
			panic("chain refers to tag or node with zero count")
		}

		ctx.slaveCounts[node] = nodeCount - 1
		ctx.tagCounts[tag] = tagCount - 1

		ctx.notePair(ctx.master, node, i)

		for j, otherNode := range chain[i+1:] {
			ctx.notePair(node, otherNode, j)
		}
	}

	ctx.vbuckets--
}

func (ctx *selectionCtx) hasSlaves() bool {
	return len(ctx.slaveCounts) != 0
}

func (ctx *selectionCtx) pairCost(x, y Node, distance int) chainCost {
	stat := ctx.getSlaveStat(x, y, distance)
	tagStat := ctx.getTagStat(x, y, distance)

	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	viol := B2i(xTag == yTag)
	raw := stat*100 + tagStat*30

	return chainCost{raw, viol}
}

func (ctx *selectionCtx) requiredTags() (result []Tag) {
	result = make([]Tag, 0)
	for tag, count := range ctx.tagCounts {
		if count >= ctx.vbuckets {
			result = append(result, tag)
		}
	}

	return
}

func (ctx *selectionCtx) requiredNodes() (result []Node) {
	result = make([]Node, 0)
	for node, count := range ctx.slaveCounts {
		if count > ctx.vbuckets {
			panic("node has count greater than number of vbuckets left")
		} else if count == ctx.vbuckets {
			result = append(result, node)
		}
	}

	return
}

func (ctx *selectionCtx) availableSlaves() (result []Node) {
	result = make([]Node, 0)
	for _, node := range ctx.slaves {
		if ctx.slaveCounts[node] != 0 {
			result = append(result, node)
		}
	}

	return
}

func (*selectionCtx) restoreChain(parent [][]int,
	nodes []Node, t, i int) (chain []Node) {

	chain = make([]Node, t+1)

	for t >= 0 {
		chain[t] = nodes[i]
		i = parent[t][i]
		t--
	}

	return
}

func (ctx *selectionCtx) isFeasibleChain(requiredTags []Tag,
	requiredNodes []Node, chain []Node) bool {

	seenNodes := make(map[Node]bool)
	seenTags := make(map[Tag]bool)

	for _, node := range chain {
		if node == ctx.master {
			return false
		}

		if _, present := seenNodes[node]; present {
			return false
		}

		seenNodes[node] = true

		tag := ctx.params.Tags[node]
		seenTags[tag] = true
	}

	reqTagsCount := 0
	for _, tag := range requiredTags {
		if _, present := seenTags[tag]; present {
			reqTagsCount++
		}
	}

	reqNodesCount := 0
	for _, node := range requiredNodes {
		if _, present := seenNodes[node]; present {
			reqNodesCount++
		}
	}

	n := len(chain)
	if n-ctx.params.NumReplicas+len(requiredNodes) > reqNodesCount ||
		n-ctx.params.NumReplicas+len(requiredTags) > reqTagsCount {
		return false
	}

	for i, node := range chain {
		if node == ctx.master {
			return false
		}

		for _, other := range chain[i+1:] {
			if node == other {
				return false
			}
		}
	}

	return true
}

func (ctx *selectionCtx) nextBestChain() (result []Node) {
	requiredTags := ctx.requiredTags()
	requiredNodes := ctx.requiredNodes()

	candidates := ctx.availableSlaves()
	candidates = append(candidates, ctx.master)
	numCandidates := len(candidates)

	isFeasible := func(chain []Node) bool {
		return ctx.isFeasibleChain(requiredTags, requiredNodes, chain)
	}

	cost := make([][]chainCost, ctx.params.NumReplicas)
	parent := make([][]int, ctx.params.NumReplicas)

	for i := range cost {
		cost[i] = make([]chainCost, numCandidates)
		parent[i] = make([]int, numCandidates)
	}

	for i, node := range candidates {
		if isFeasible([]Node{node}) {
			cost[0][i] = ctx.pairCost(ctx.master, node, 0)
		} else {
			cost[0][i] = inf
		}
	}

	for t := 1; t < ctx.params.NumReplicas; t++ {
		for i, node := range candidates {
			min := inf
			var minCount int

			for j := range candidates {
				c := cost[t-1][j]

				if c == inf {
					continue
				}

				chain := ctx.restoreChain(parent, candidates, t-1, j)
				chain = append(chain, candidates[i])
				if !isFeasible(chain) {
					continue
				}

				for d := 0; d < t; d++ {
					other := chain[t-d-1]
					c = c.plus(ctx.pairCost(other, node, d))
				}
				c = c.plus(ctx.pairCost(ctx.master, node, t))

				if c.less(min) {
					min = c
					minCount = 1

					parent[t][i] = j
				} else if c == min {
					minCount++

					if rand.Intn(minCount) == 0 {
						parent[t][i] = j
					}
				}
			}

			cost[t][i] = min
		}
	}

	t := ctx.params.NumReplicas - 1
	min := inf
	iMin := -1

	for i := range candidates {
		c := cost[t][i]
		if c.less(min) {
			min = c
			iMin = i
		}
	}

	if iMin == -1 {
		panic("cannot happen")
	}

	result = ctx.restoreChain(parent, candidates, t, iMin)

	return
}

// Construct vbucket map from a matrix R.
func buildVbmap(r R) (vbmap Vbmap) {
	params := r.params
	vbmap = makeVbmap(params)

	// determines how many active vbuckets each node has
	var nodeVbs []int
	if params.NumReplicas == 0 || params.NumSlaves == 0 {
		// If there's only one copy of every vbucket, then matrix R is
		// just a null matrix. So we just spread the vbuckets evenly
		// among the nodes and we're almost done.
		nodeVbs = SpreadSum(params.NumVBuckets, params.NumNodes)
	} else {
		// Otherwise matrix R defines the amount of active vbuckets
		// each node has.
		nodeVbs = make([]int, params.NumNodes)
		for i, sum := range r.RowSums {
			vbs := sum / params.NumReplicas
			if sum%params.NumReplicas != 0 {
				panic("row sum is not multiple of NumReplicas")
			}

			nodeVbs[i] = vbs
		}
	}

	stats := makePairStats()

	vbucket := 0
	for i, row := range r.Matrix {
		vbs := nodeVbs[i]
		ctx := makeSelectionCtx(params, Node(i), vbs, stats)

		for s, count := range row {
			if count != 0 {
				ctx.addSlave(Node(s), count)
			}
		}

		if !ctx.hasSlaves() {
			// Row matrix contained only zeros. This usually means
			// that replica count is zero. Other possibility is
			// that the number of vbuckets is less than number of
			// nodes and some of the nodes end up with no vbuckets
			// at all. In any case, we just mark the node as a
			// master for its vbuckets (if any).

			for vbs > 0 {
				vbmap[vbucket][0] = Node(i)
				vbs--
				vbucket++
			}

			continue
		}

		for vbs > 0 {
			vbmap[vbucket][0] = Node(i)

			chain := ctx.nextBestChain()
			ctx.noteChain(chain)

			copy(vbmap[vbucket][1:], chain)

			vbs--
			vbucket++
		}
	}

	return
}

func getMaxStrictSlaves(params VbmapParams) int {
	result := params.NumSlaves
	for _, tagNodes := range params.Tags.TagsNodesMap() {
		result = Min(params.NumNodes-len(tagNodes), result)
	}

	return result
}

func getTagMaxSlaves(params VbmapParams) int {
	result := 0
	for _, tagNodes := range params.Tags.TagsNodesMap() {
		result = Max(params.NumNodes-len(tagNodes), result)
	}

	return result
}

func getMaxSlaves(params VbmapParams) int {
	tagMaxSlaves := getTagMaxSlaves(params)
	if params.NumSlaves < tagMaxSlaves {
		return params.NumSlaves
	}

	maxTagSize := 0
	for _, tagNodes := range params.Tags.TagsNodesMap() {
		maxTagSize = Max(maxTagSize, len(tagNodes))
	}

	roundUp := (params.NumSlaves-1)/params.NumReplicas + 1
	roundUp *= params.NumReplicas

	return Max(maxTagSize*params.NumReplicas, roundUp)
}

func doBuildRI(
	params *VbmapParams,
	searchParams SearchParams,
	gen RIGenerator,
	banner string,
	minSlaves, maxSlaves int) (RI, error) {

	var ri RI
	var err error

	for numSlaves := maxSlaves; numSlaves >= minSlaves; numSlaves-- {
		diag.Printf(
			"Trying to generate %s RI with NumSlaves=%d",
			banner, numSlaves)

		params.NumSlaves = numSlaves

		ri, err = gen.Generate(*params, searchParams)
		if err != ErrorNoSolution {
			return ri, err
		}
	}

	return ri, ErrorNoSolution
}

func tryBuildRI(
	params *VbmapParams,
	searchParams SearchParams,
	gen RIGenerator) (RI, error) {

	var ri RI
	var err error

	minSlaves := params.NumSlaves
	maxStrictSlaves := params.NumSlaves
	maxSlaves := params.NumSlaves
	if searchParams.RelaxNumSlaves && params.NumSlaves != 0 {
		maxStrictSlaves = getMaxStrictSlaves(*params)
		maxSlaves = getMaxSlaves(*params)

		minSlaves = maxStrictSlaves / params.NumReplicas
		minSlaves *= params.NumReplicas
	}

	strict := searchParams
	strict.RelaxSlaveBalance = false
	strict.RelaxReplicaBalance = false

	ri, err = doBuildRI(
		params, strict, gen, "strict", minSlaves, maxStrictSlaves)
	if err == nil || err != ErrorNoSolution {
		return ri, err
	}

	if searchParams.RelaxSlaveBalance || searchParams.RelaxReplicaBalance {
		return doBuildRI(
			params, searchParams,
			gen, "non-strict", minSlaves, maxSlaves)
	}

	return ri, ErrorNoSolution
}

func PrintVbStats(vbmap Vbmap, params VbmapParams, banner string) {
	activeVbs := make(map[Node]int)
	replicaVbs := make(map[Node]int)

	for _, node := range params.Nodes() {
		activeVbs[node] = 0
		replicaVbs[node] = 0
	}

	for _, vbChain := range vbmap {
		activeVbs[vbChain[0]]++
		for _, replica := range vbChain[1:] {
			replicaVbs[replica]++
		}
	}

	diag.Printf("Per-node Vb Stats (%s):", banner)
	diag.Printf("   Active Vbs:   %v", activeVbs)
	diag.Printf("   Replica Vbs:  %v", replicaVbs)
	return
}

// Generate vbucket map given a generator for matrix RI and vbucket map
// parameters.
func VbmapGenerate(params VbmapParams, gen RIGenerator,
	searchParams SearchParams, _ Vbmap) (vbmap Vbmap, err error) {

	start := time.Now()

	ri, err := tryBuildRI(&params, searchParams, gen)
	if err != nil {
		return nil, err
	}

	diag.Printf("Generated topology:\n%s", ri.String())

	r, err := BuildR(params, ri, searchParams)
	if err != nil {
		return nil, err
	}

	dt := time.Since(start)
	diag.Printf("Generated matrix R in %s (wall clock)", dt)
	diag.Printf("Final map R:\n%s", r.String())

	vbmapStart := time.Now()

	vbmap = buildVbmap(r)

	PrintVbStats(vbmap, params, "max-flow")

	dt = time.Since(vbmapStart)
	diag.Printf("Built vbucket map from R in %s (wall clock)", dt)

	dt = time.Since(start)
	diag.Printf("Spent %s overall on vbucket map generation (wall clock)", dt)

	return
}
