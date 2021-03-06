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
	"fmt"
	"math"
	"math/big"
)

// Matrix R with some meta-information.
type R struct {
	Matrix  [][]int // actual matrix
	RowSums []int   // row sums for the matrix
	ColSums []int   // column sums for the matrix

	params VbmapParams // corresponding vbucket map params
}

func (r R) String() string {
	return matrixToString(r.Matrix, r.params)
}

func newRat(num, denom int) *big.Rat {
	return big.NewRat(int64(num), int64(denom))
}

func newRatFromInt(n int) *big.Rat {
	return newRat(n, 1)
}

func ratFloor(r *big.Rat) int {
	num := int(r.Num().Int64())
	denom := int(r.Denom().Int64())

	return num / denom
}

func ratCeil(r *big.Rat) int {
	if r.IsInt() {
		return ratFloor(r)
	}

	return ratFloor(r) + 1
}

func replicasPerSlave(params VbmapParams, strict bool) (*big.Rat, *big.Rat) {
	if params.NumSlaves == 0 {
		return newRatFromInt(0), newRatFromInt(0)
	}

	total := params.NumVBuckets * params.NumReplicas
	perNode := newRat(total, params.NumNodes)

	low := &big.Rat{}
	low.Quo(perNode, newRatFromInt(params.NumSlaves))
	high := low

	if !strict {
		activeVbsPerNode := newRat(params.NumVBuckets, params.NumNodes)
		activeVbsPerNodeLow := ratFloor(activeVbsPerNode)
		activeVbsPerNodeHigh := ratCeil(activeVbsPerNode)

		low = newRatFromInt(activeVbsPerNodeLow)
		low.Mul(low, newRatFromInt(params.NumReplicas))
		low.Quo(low, newRatFromInt(params.NumSlaves))

		high = newRatFromInt(activeVbsPerNodeHigh)
		high.Mul(high, newRatFromInt(params.NumReplicas))
		high.Quo(high, newRatFromInt(params.NumSlaves))
	}

	return low, high
}

func buildRFlowGraph(
	params VbmapParams, ri RI, activeVbs []int, strict bool) (g *Graph) {

	replicasPerSlaveLo, replicasPerSlaveHi :=
		replicasPerSlave(params, strict)

	graphName := fmt.Sprintf("Flow graph for R (%s)", params)
	g = NewGraph(graphName)

	for i, row := range ri.Matrix {
		node := Node(i)
		nodeSrcV := NodeSourceVertex(node)
		nodeSinkV := NodeSinkVertex(node)

		numVbsReplicated := activeVbs[i] * params.NumReplicas
		g.AddEdge(Source, nodeSrcV, numVbsReplicated, numVbsReplicated)

		replications := newRatFromInt(ri.NumInboundReplications(node))

		numReplicas := &big.Rat{}
		numReplicas.Mul(replications, replicasPerSlaveLo)
		numReplicasLo := ratFloor(numReplicas)

		numReplicas.Mul(replications, replicasPerSlaveHi)
		numReplicas.Mul(replications, replicasPerSlaveHi)
		numReplicasHi := ratCeil(numReplicas)

		g.AddEdge(nodeSinkV, Sink, numReplicasHi, numReplicasLo)

		var maxVbsPerTag int
		if params.NumReplicas > 0 {
			maxVbsPerTag = numVbsReplicated / params.NumReplicas
		}
		for j, elem := range row {
			if elem == 0 {
				continue
			}

			dstNode := Node(j)
			dstNodeTag := params.Tags[dstNode]

			dstNodeSinkV := NodeSinkVertex(dstNode)
			tagV := TagNodeVertex{dstNodeTag, node}

			if !g.HasVertex(tagV) {
				g.AddEdge(nodeSrcV, tagV, maxVbsPerTag, 0)
			}

			vbsPerSlave := newRat(
				numVbsReplicated, params.NumSlaves)
			vbsPerSlaveLo := ratFloor(vbsPerSlave)
			vbsPerSlaveHi := ratCeil(vbsPerSlave)

			g.AddEdge(
				tagV, dstNodeSinkV,
				vbsPerSlaveHi*elem,
				vbsPerSlaveLo*elem)
		}
	}

	return
}

func graphToR(g *Graph, params VbmapParams) (r *R) {
	matrix := make([][]int, params.NumNodes)

	for i := range matrix {
		matrix[i] = make([]int, params.NumNodes)
	}

	for _, dst := range params.Nodes() {
		dstVertex := NodeSinkVertex(dst)

		for _, edge := range g.EdgesToVertex(dstVertex) {
			src := edge.Src.(TagNodeVertex).Node

			matrix[int(src)][int(dst)] = edge.Flow()
		}
	}

	return makeRFromMatrix(params, matrix)
}

func makeRFromMatrix(params VbmapParams, matrix [][]int) (result *R) {
	result = &R{}
	result.params = params
	result.Matrix = matrix
	result.ColSums = make([]int, params.NumNodes)
	result.RowSums = make([]int, params.NumNodes)

	for i, row := range result.Matrix {
		rowSum := 0
		for j, elem := range row {
			rowSum += elem
			result.ColSums[j] += elem
		}
		result.RowSums[i] = rowSum
	}

	return
}

// Build balanced matrix R from RI.
func BuildR(params VbmapParams, ri RI, searchParams SearchParams) (R, error) {
	var g *Graph
	var feasible bool
	activeVbsPerNode := SpreadSum(params.NumVBuckets, params.NumNodes)

	for i := 0; i < searchParams.NumRRetries; i++ {
		Shuffle(activeVbsPerNode)
		feasible, g = findRFlow(params, ri, activeVbsPerNode, true)
		if feasible {
			diag.Printf("Found feasible R after %d attempts", i+1)
			break
		}
	}

	if !feasible && !searchParams.StrictReplicaBalance {
		feasible, g = findRFlow(params, ri, activeVbsPerNode, false)
		if !feasible {
			panic("Couldn't generate non-strict R. " +
				"This should not happen")
		}

		diag.Printf("Found feasible R with non-strict replica balance")
	}

	if feasible {
		var balancedReplicas bool
		if searchParams.BalanceReplicas &&
			!ri.IsBalanced(ReplicasBalanced) {

			g = balanceReplicas(params, g)
			balancedReplicas = true
		}

		if balancedReplicas ||
			searchParams.BalanceSlaves &&
				!ri.IsBalanced(SlavesBalanced) {

			g = balanceSlaves(ri, params, g)
		}
	}

	dotPath := searchParams.DotPath
	if dotPath != "" {
		err := g.Dot(dotPath, false)
		if err != nil {
			diag.Printf("Couldn't create dot file %s: %s",
				dotPath, err.Error())
		}
	}

	if !feasible {
		return R{}, ErrorNoSolution
	}

	return *graphToR(g, params), nil
}

func findRFlow(
	params VbmapParams, ri RI, vbs []int, strict bool) (bool, *Graph) {

	g := buildRFlowGraph(params, ri, vbs, strict)
	feasible, _ := g.FindFeasibleFlow()

	return feasible, g
}

func balanceSlaves(ri RI, params VbmapParams, rg *Graph) *Graph {
	graphName := fmt.Sprintf(
		"Flow graph for balancing slaves in R (%s)", params)
	g := NewGraph(graphName)

	numSlaves := params.NumSlaves
	for _, node := range params.Nodes() {
		numSlaves = Max(numSlaves, ri.NumInboundReplications(node))
	}

	capacity := params.NumVBuckets * params.NumReplicas
	capacity /= params.NumNodes * numSlaves

	for _, edge := range rg.EdgesFromVertex(Source) {
		g.AddEdge(Source, edge.Dst, edge.Capacity(), edge.Capacity())
	}

	for _, edge := range rg.EdgesToVertex(Sink) {
		g.AddEdge(edge.Src, Sink, edge.Flow(), 0)
	}

	for _, node := range params.Nodes() {
		nodeSrc := NodeSourceVertex(node)
		nodeSink := NodeSinkVertex(node)

		for _, edge := range rg.EdgesFromVertex(nodeSrc) {
			g.AddEdge(nodeSrc, edge.Dst, edge.Capacity(), 0)
		}

		for _, edge := range rg.EdgesToVertex(nodeSink) {
			g.AddEdge(edge.Src, nodeSink, capacity, 0)
		}
	}

	for {
		feasible, _ := g.FindFeasibleFlow()
		if feasible {
			break
		}

		relaxed := relaxSlaves(g, params)
		if !relaxed {
			panic("no feasible flow; should not happen")
		}
	}

	return g
}

func relaxSlaves(g *Graph, params VbmapParams) bool {
	relaxed := false
	for _, node := range params.Nodes() {
		v := NodeSourceVertex(node)

		edges := []*GraphEdge(nil)
		for _, tagEdge := range g.EdgesFromVertex(v) {
			nodeEdges := g.EdgesFromVertex(tagEdge.Dst)
			edges = append(edges, nodeEdges...)
		}

		relaxEdges := edgesToRelax(edges)
		if len(relaxEdges) > 0 {
			relaxed = true
			for _, edge := range relaxEdges {
				edge.IncreaseCapacity(1)
			}
		}
	}

	return relaxed
}

func edgesToRelax(edges []*GraphEdge) []*GraphEdge {
	minFlow := math.MaxInt
	relaxEdges := []*GraphEdge(nil)

	for _, edge := range edges {
		flow := edge.Flow()
		if edge.IsSaturated() {
			if flow < minFlow {
				minFlow = flow
				relaxEdges = []*GraphEdge{edge}
			} else if flow == minFlow {
				relaxEdges = append(relaxEdges, edge)
			}
		}
	}

	return relaxEdges
}

func balanceReplicas(params VbmapParams, rg *Graph) *Graph {
	graphName := fmt.Sprintf(
		"Flow graph for balancing replicas in R (%s)", params)
	g := NewGraph(graphName)

	for _, edge := range rg.EdgesFromVertex(Source) {
		g.AddEdge(Source, edge.Dst, edge.Capacity(), edge.Capacity())
	}

	minFlow := math.MaxInt
	for _, edge := range rg.EdgesToVertex(Sink) {
		minFlow = Min(minFlow, edge.Flow())
	}

	for _, edge := range rg.EdgesToVertex(Sink) {
		g.AddEdge(edge.Src, Sink, minFlow, 0)
	}

	for _, node := range params.Nodes() {
		nodeSrc := NodeSourceVertex(node)
		nodeSink := NodeSinkVertex(node)

		for _, edge := range rg.EdgesFromVertex(nodeSrc) {
			g.AddEdge(nodeSrc, edge.Dst, edge.Capacity(), 0)
		}

		for _, edge := range rg.EdgesToVertex(nodeSink) {
			g.AddEdge(edge.Src, nodeSink, math.MaxInt, 0)
		}
	}

	for {
		feasible, _ := g.FindFeasibleFlow()
		if feasible {
			break
		}

		if !relaxReplicas(g) {
			panic("no feasible flow; should not happen")
		}
	}

	return g
}

func relaxReplicas(g *Graph) bool {
	relaxed := false

	for _, edge := range g.EdgesToVertex(Sink) {
		if edge.IsSaturated() {
			relaxed = true
			edge.IncreaseCapacity(1)
		}
	}

	return relaxed
}
