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
	"io"
	"io/ioutil"
	"math"
)

type GraphVertex interface {
	fmt.Stringer
}

type SimpleVertex string

func (v SimpleVertex) String() string {
	return string(v)
}

type TagVertex Tag

func (v TagVertex) String() string {
	return fmt.Sprintf("tag_%d", int(v))
}

type NodeSourceVertex Node

func (v NodeSourceVertex) String() string {
	return fmt.Sprintf("node_%d_source", int(v))
}

type NodeSinkVertex Node

func (v NodeSinkVertex) String() string {
	return fmt.Sprintf("node_%d_sink", int(v))
}

type TagNodeVertex struct {
	Tag  Tag
	Node Node
}

func (v TagNodeVertex) String() string {
	return fmt.Sprintf("tag_%d_node_%d", int(v.Tag), int(v.Node))
}

const (
	Source SimpleVertex = "source"
	Sink   SimpleVertex = "sink"

	supplySource SimpleVertex = "supply"
	demandSink   SimpleVertex = "demand"
)

type edgeType int

const (
	edgeNormal  edgeType = iota
	edgeReverse edgeType = iota
	edgeDemand  edgeType = iota
)

type GraphEdge struct {
	Src GraphVertex
	Dst GraphVertex

	Demand      int
	ReverseEdge *GraphEdge

	// actual capacity adjusted for demand
	capacity int
	// flow according to adjusted capacity
	flow int

	etype edgeType

	demandEdge *GraphEdge
	supplyEdge *GraphEdge
}

func (edge GraphEdge) Capacity() int {
	return edge.Demand + edge.capacity
}

func (edge GraphEdge) Flow() int {
	if edge.Demand == 0 {
		return edge.flow
	}

	// Note that this value will only be correct if there's a feasible
	// flow in the grah. If there's not, the value may be nonsensical.
	demandFlow := Min(edge.demandEdge.flow, edge.supplyEdge.flow)
	demandFlow = Min(edge.Demand, demandFlow)
	return edge.flow + demandFlow
}

func (edge GraphEdge) String() string {
	return fmt.Sprintf("%s->%s", edge.Src, edge.Dst)
}

func (edge GraphEdge) residual() int {
	return edge.capacity - edge.flow
}

func (edge GraphEdge) mustREdge() *GraphEdge {
	if edge.ReverseEdge == nil {
		panic(fmt.Sprintf("Edge %s does not have a reverse edge", edge))
	}

	return edge.ReverseEdge
}

func (edge GraphEdge) IsReverseEdge() bool {
	return edge.etype == edgeReverse
}

func (edge *GraphEdge) pushFlow(flow int) {
	residual := edge.residual()
	if flow > residual {
		panic(fmt.Sprintf("Trying to push flow %d "+
			"via edge %s with residual capacity %d",
			flow, edge, residual))
	}

	edge.flow += flow
	if edge.ReverseEdge != nil {
		edge.ReverseEdge.flow -= flow
	}
}

func (edge GraphEdge) IsSaturated() bool {
	return edge.residual() == 0
}

func (edge *GraphEdge) SetCapacity(capacity int) {
	if capacity < edge.Capacity() {
		panic("new capacity is less than the old one")
	}

	edge.capacity = capacity - edge.Demand
}

func (edge *GraphEdge) IncreaseCapacity(by int) {
	edge.SetCapacity(edge.Capacity() + by)
}

type augPath []*GraphEdge

func (path *augPath) addEdge(edge *GraphEdge) {
	*path = append(*path, edge)
}

func (path *augPath) removeLastEdge() (edge *GraphEdge) {
	n := len(*path)
	if n == 0 {
		panic("Removing edge from empty path")
	}

	edge = (*path)[n-1]
	*path = (*path)[0 : n-1]

	return
}

func (path augPath) capacity() (result int) {
	if len(path) == 0 {
		panic("capacity called on empty path")
	}

	result = path[0].residual()
	for _, edge := range path {
		residual := edge.residual()
		if residual < result {
			result = residual
		}
	}

	return
}

func (path *augPath) truncate(i int) {
	if i >= len(*path) {
		panic("index out of range in truncate")
	}

	*path = (*path)[0:i]
}

type graphVertexData struct {
	allEdges  []*GraphEdge
	firstEdge int
}

func makeGraphVertexData() *graphVertexData {
	return &graphVertexData{allEdges: []*GraphEdge{}, firstEdge: 0}
}

func (v graphVertexData) edges() []*GraphEdge {
	return v.allEdges[v.firstEdge:]
}

func (v *graphVertexData) addEdge(edge *GraphEdge) {
	v.allEdges = append(v.allEdges, edge)
}

func (v *graphVertexData) forgetFirstEdge() {
	v.firstEdge++
}

func (v *graphVertexData) reset() {
	v.firstEdge = 0
}

func (v *graphVertexData) flow() (result int) {
	for _, edge := range v.edges() {
		if edge.etype == edgeNormal {
			result += edge.Flow()
		}
	}

	return
}

type graphStats struct {
	numVertices int
	numEdges    int
}

func (stats graphStats) String() string {
	return fmt.Sprintf("Graph stats:\n\tVertices: %d\n\tEdges: %d\n",
		stats.numVertices, stats.numEdges)
}

func (stats *graphStats) noteVertexAdded() {
	stats.numVertices++
}

func (stats *graphStats) noteEdgeAdded() {
	stats.numEdges++
}

type maxflowStats struct {
	iteration   int
	numAdvances int
	numRetreats int
	numAugments int
	numEdges    int
}

func (stats maxflowStats) String() string {
	return fmt.Sprintf("\tCurrent iteration: %d\n"+
		"\tNumber of advances: %d\n\tNumber of retreats: %d\n"+
		"\tNumber of augments: %d\n"+
		"\tTotal number of edges processed: %d\n",
		stats.iteration, stats.numAdvances,
		stats.numRetreats, stats.numAugments, stats.numEdges)
}

func (stats *maxflowStats) reset() {
	stats.iteration = 0
	stats.numAdvances = 0
	stats.numRetreats = 0
	stats.numAugments = 0
	stats.numEdges = 0
}

func (stats *maxflowStats) nextIteration() {
	iter := stats.iteration
	stats.reset()
	stats.iteration = iter + 1
}

func (stats *maxflowStats) noteAdvance() {
	stats.numAdvances++
}

func (stats *maxflowStats) noteRetreat() {
	stats.numRetreats++
}

func (stats *maxflowStats) noteAugment() {
	stats.numAugments++
}

func (stats *maxflowStats) noteEdgeProcessed() {
	stats.numEdges++
}

type Graph struct {
	name      string
	vertices  map[GraphVertex]*graphVertexData
	distances map[GraphVertex]int

	graphStats
	maxflowStats
}

func NewGraph(name string) (g *Graph) {
	g = &Graph{}
	g.name = name
	g.vertices = make(map[GraphVertex]*graphVertexData)
	g.distances = make(map[GraphVertex]int)

	g.addEdge(Sink, Source, math.MaxInt, 0, edgeDemand)

	return
}

type edgePredicate func(*GraphEdge) bool

func (g *Graph) bfsGeneric(source GraphVertex, pred edgePredicate) int {
	queue := []GraphVertex{source}
	seen := make(map[GraphVertex]bool)

	for v := range g.vertices {
		g.distances[v] = -1
	}

	seen[source] = true
	g.distances[source] = 0

	var d int
	for len(queue) != 0 {
		v := queue[0]
		d = g.distances[v]

		queue = queue[1:]

		for _, edge := range g.vertices[v].edges() {
			if !pred(edge) {
				continue
			}

			_, present := seen[edge.Dst]
			if !present {
				dst := edge.Dst

				queue = append(queue, dst)
				seen[dst] = true
				g.distances[dst] = d + 1
			}
		}
	}

	return d
}

func (g *Graph) bfsUnsaturated(source, sink GraphVertex) bool {
	_ = g.bfsGeneric(source, func(edge *GraphEdge) bool {
		return !edge.IsSaturated()
	})

	return g.distances[sink] != -1
}

func (g *Graph) bfsNetwork(source GraphVertex) int {
	return g.bfsGeneric(source, func(edge *GraphEdge) bool {
		return edge.etype == edgeNormal
	})
}

func (g *Graph) dfsPath(from, to GraphVertex, path *augPath) bool {
	if from == to {
		return true
	}

	d := g.distances[from]

	fromData := g.vertices[from]

	for _, edge := range fromData.edges() {
		g.noteEdgeProcessed()

		dst := edge.Dst

		if g.distances[dst] == d+1 && !edge.IsSaturated() {
			g.noteAdvance()

			path.addEdge(edge)
			if g.dfsPath(dst, to, path) {
				return true
			}

			path.removeLastEdge()
		}

		fromData.forgetFirstEdge()
	}

	g.noteRetreat()
	return false
}

func (g *Graph) augmentFlow(source, sink GraphVertex) bool {
	for _, vertexData := range g.vertices {
		vertexData.reset()
	}

	if !g.bfsUnsaturated(source, sink) {
		return false
	}

	path := augPath(nil)
	v := source

	for {
		pathFound := g.dfsPath(v, sink, &path)
		if pathFound {
			capacity := path.capacity()
			firstSaturatedEdge := -1

			for i, edge := range path {
				edge.pushFlow(capacity)
				if firstSaturatedEdge == -1 && edge.IsSaturated() {
					firstSaturatedEdge = i
				}
			}

			g.noteAugment()

			if firstSaturatedEdge == -1 {
				panic("No saturated edge on augmenting path")
			}

			v = path[firstSaturatedEdge].Src
			path.truncate(firstSaturatedEdge)
		} else {
			if v == source {
				break
			}

			g.distances[v] = -1
			edge := path.removeLastEdge()
			v = edge.Src
		}
	}

	return true
}

func (g *Graph) addVertex(vertex GraphVertex) {
	_, present := g.vertices[vertex]
	if !present {
		g.noteVertexAdded()
		g.vertices[vertex] = makeGraphVertexData()
	}
}

func (g *Graph) addEdge(src, dst GraphVertex,
	capacity, demand int, etype edgeType) *GraphEdge {

	g.addVertex(src)
	g.addVertex(dst)

	edge := &GraphEdge{Src: src, Dst: dst, Demand: demand,
		capacity: capacity, etype: etype}

	g.noteEdgeAdded()
	g.vertices[src].addEdge(edge)

	return edge
}

func (g *Graph) addDemandEdge(src, dst GraphVertex, demand int) *GraphEdge {
	var edge *GraphEdge
	for _, e := range g.edgesFromVertex(src, edgeDemand) {
		if e.Dst == dst {
			edge = e
			break
		}
	}

	if edge == nil {
		edge = g.addEdge(src, dst, 0, 0, edgeDemand)
	}

	edge.capacity += demand
	return edge
}

func (g *Graph) AddEdge(src, dst GraphVertex, capacity, demand int) {
	g.checkNoEdge(src, dst)

	capacity -= demand

	edge := g.addEdge(src, dst, capacity, demand, edgeNormal)
	redge := g.addEdge(dst, src, 0, 0, edgeReverse)

	edge.ReverseEdge = redge
	redge.ReverseEdge = edge

	if demand != 0 {
		demandEdge := g.addDemandEdge(src, demandSink, demand)
		supplyEdge := g.addDemandEdge(supplySource, dst, demand)

		edge.demandEdge = demandEdge
		edge.supplyEdge = supplyEdge
	}
}

func (g *Graph) checkNoEdge(src, dst GraphVertex) {
	for _, edge := range g.EdgesFromVertex(src) {
		if edge.Dst == dst {
			panic("edge from %v to %v already exists")
		}
	}
}

func (g *Graph) edges() (result []*GraphEdge) {
	for _, vertexData := range g.vertices {
		for _, edge := range vertexData.edges() {
			result = append(result, edge)
		}
	}

	return
}

func (g *Graph) hasFeasibleFlow() (result bool, violation int) {
	_, haveDemands := g.vertices[supplySource]
	if !haveDemands {
		return true, 0
	}

	for _, edge := range g.vertices[supplySource].edges() {
		violation += edge.residual()
	}

	return violation == 0, violation
}

func (g *Graph) FindFeasibleFlow() (bool, int) {
	if feasible, _ := g.hasFeasibleFlow(); feasible {
		return true, 0
	}

	g.doMaximizeFlow(supplySource, demandSink, "FindFeasibleFlow stats")
	return g.hasFeasibleFlow()
}

func (g *Graph) MaximizeFlow() bool {
	if feasible, _ := g.FindFeasibleFlow(); !feasible {
		return false
	}

	g.doMaximizeFlow(Source, Sink, "MaximizeFlow stats")
	return true
}

func (g *Graph) doMaximizeFlow(source, sink GraphVertex, statsHeader string) {
	g.maxflowStats.reset()

	for {
		augmented := g.augmentFlow(source, sink)
		if !augmented {
			break
		}

		diag.Verbosef("%s:\n%s", statsHeader, g.maxflowStats)
		g.maxflowStats.nextIteration()
	}
}

func (g *Graph) HasVertex(v GraphVertex) bool {
	_, ok := g.vertices[v]
	return ok
}

func (g *Graph) EdgesFromVertex(v GraphVertex) (edges []*GraphEdge) {
	return g.edgesFromVertex(v, edgeNormal)
}

func (g *Graph) edgesFromVertex(v GraphVertex, etype edgeType) []*GraphEdge {
	if !g.HasVertex(v) {
		return nil
	}

	var edges []*GraphEdge
	for _, edge := range g.vertices[v].edges() {
		if edge.etype == etype {
			edges = append(edges, edge)
		}
	}

	return edges
}

func (g *Graph) EdgesToVertex(v GraphVertex) (edges []*GraphEdge) {
	if !g.HasVertex(v) {
		return nil
	}

	for _, edge := range g.vertices[v].edges() {
		if edge.etype == edgeReverse {
			edges = append(edges, edge.mustREdge())
		}
	}

	return
}

func (g *Graph) Vertices() (vertices []GraphVertex) {
	for v := range g.vertices {
		vertices = append(vertices, v)
	}

	return
}

func (g *Graph) Dot(path string, verbose bool) (err error) {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(buffer, "digraph G {\n")
	fmt.Fprintf(buffer, "rankdir=LR;\n")
	fmt.Fprintf(buffer, "labelloc=t; labeljust=l; ")

	feasible, violation := g.hasFeasibleFlow()

	label := fmt.Sprintf(`%s\nflow = %d\nfeasible = %v, violation = %d`,
		g.name, g.vertices[Source].flow(), feasible, violation)
	fmt.Fprintf(buffer, "label=\"%s\";\n", label)

	dist := g.bfsNetwork(Source)
	groups := make([][]GraphVertex, dist+1)

	for v := range g.vertices {
		d := g.distances[v]

		if d != -1 {
			groups[d] = append(groups[d], v)
		}
	}

	groupVertices(buffer, groups[0], "source")
	groupVertices(buffer, groups[dist], "sink")

	for _, group := range groups[1:dist] {
		groupVertices(buffer, group, "same")
	}

	for _, edge := range g.edges() {
		var style string

		if edge.etype != edgeNormal && !verbose {
			continue
		}

		switch edge.etype {
		case edgeNormal:
			style = "solid"
		case edgeReverse:
			style = "dashed"
		case edgeDemand:
			style = "dotted"
		}

		color := "red"
		if edge.Flow() < edge.Capacity() {
			color = "darkgreen"
		}

		labelcolor := "black"
		if edge.etype == edgeNormal && edge.Flow() < edge.Demand {
			labelcolor = "red"
		}

		capacity := edge.Capacity()
		capacityString := fmt.Sprintf("%d", capacity)

		if capacity == math.MaxInt {
			capacityString = "∞"
		}

		fmt.Fprintf(buffer,
			"%s -> %s [label=\"%d (%d..%s)\", decorate=true,"+
				" style=%s, color=%s, fontcolor=%s];\n",
			edge.Src, edge.Dst, edge.Flow(),
			edge.Demand, capacityString, style, color, labelcolor)
	}

	fmt.Fprintf(buffer, "}\n")

	return ioutil.WriteFile(path, buffer.Bytes(), 0644)
}

func groupVertices(w io.Writer, vertices []GraphVertex, rank string) {
	fmt.Fprintf(w, "{\n")
	fmt.Fprintf(w, "rank=%s;\n", rank)

	for _, v := range vertices {
		fmt.Fprintf(w, "%s;\n", v)
	}

	fmt.Fprintf(w, "}\n")
}
