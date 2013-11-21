package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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

	etype       edgeType
	demandEdges []*GraphEdge
}

func (edge GraphEdge) Capacity() int {
	return edge.Demand + edge.capacity
}

func (edge GraphEdge) Flow() int {
	if edge.demandEdges == nil {
		return edge.flow
	} else {
		demandFlow := MaxInt
		for _, demandEdge := range edge.demandEdges {
			if demandEdge.flow < demandFlow {
				demandFlow = demandEdge.flow
			}
		}

		return edge.flow + demandFlow
	}
}

func (edge GraphEdge) String() string {
	return fmt.Sprintf("%s->%s", edge.Src, edge.Dst)
}

func (edge GraphEdge) residual() int {
	return edge.capacity - edge.flow
}

func (edge GraphEdge) MustREdge() *GraphEdge {
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

func (edge GraphEdge) isSaturated() bool {
	return edge.residual() == 0
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
	v.firstEdge += 1
}

func (v *graphVertexData) reset() {
	v.firstEdge = 0
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
	stats.numVertices += 1
}

func (stats *graphStats) noteEdgeAdded() {
	stats.numEdges += 1
}

type maxflowStats struct {
	iteration   int
	numAdvances int
	numRetreats int
	numAugments int
	numEdges    int
}

func (stats maxflowStats) String() string {
	return fmt.Sprintf("Max flow stats:\n\tCurrent iteration: %d\n"+
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
	stats.numAdvances += 1
}

func (stats *maxflowStats) noteRetreat() {
	stats.numRetreats += 1
}

func (stats *maxflowStats) noteAugment() {
	stats.numAugments += 1
}

func (stats *maxflowStats) noteEdgeProcessed() {
	stats.numEdges += 1
}

type Graph struct {
	name      string
	vertices  map[GraphVertex]*graphVertexData
	distances map[GraphVertex]int
	flow      int

	graphStats
	maxflowStats
}

func NewGraph(name string) (g *Graph) {
	g = &Graph{}
	g.name = name
	g.vertices = make(map[GraphVertex]*graphVertexData)
	g.distances = make(map[GraphVertex]int)

	g.addEdge(supplySource, Source, MaxInt, 0, edgeNormal)
	g.addEdge(Sink, demandSink, MaxInt, 0, edgeNormal)

	return
}

type edgePredicate func(*GraphEdge) bool

func (g *Graph) bfsGeneric(pred edgePredicate) int {
	queue := []GraphVertex{supplySource}
	seen := make(map[GraphVertex]bool)

	for v, _ := range g.vertices {
		g.distances[v] = -1
	}

	seen[supplySource] = true
	g.distances[supplySource] = 0

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

func (g *Graph) bfsUnsaturated() bool {
	_ = g.bfsGeneric(func(edge *GraphEdge) bool {
		return !edge.isSaturated()
	})

	return g.distances[demandSink] != -1
}

func (g *Graph) bfsNetwork() int {
	return g.bfsGeneric(func(edge *GraphEdge) bool {
		return edge.etype == edgeNormal
	})
}

func (g *Graph) dfsPath(from GraphVertex, path *augPath) bool {
	if from == demandSink {
		return true
	}

	d := g.distances[from]

	fromData := g.vertices[from]

	for _, edge := range fromData.edges() {
		g.noteEdgeProcessed()

		dst := edge.Dst

		if g.distances[dst] == d+1 && !edge.isSaturated() {
			g.noteAdvance()

			path.addEdge(edge)
			if g.dfsPath(dst, path) {
				return true
			}

			path.removeLastEdge()
		}

		fromData.forgetFirstEdge()
	}

	g.noteRetreat()
	return false
}

func (g *Graph) augmentFlow() bool {
	for _, vertexData := range g.vertices {
		vertexData.reset()
	}

	if !g.bfsUnsaturated() {
		return false
	}

	path := augPath(nil)
	v := GraphVertex(supplySource)

	for {
		pathFound := g.dfsPath(v, &path)
		if pathFound {
			capacity := path.capacity()
			g.flow += capacity
			firstSaturatedEdge := -1

			for i, edge := range path {
				edge.pushFlow(capacity)
				if firstSaturatedEdge == -1 && edge.isSaturated() {
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
			if v == supplySource {
				break
			} else {
				g.distances[v] = -1
				edge := path.removeLastEdge()
				v = edge.Src
			}
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

func (g *Graph) AddEdge(src, dst GraphVertex, capacity, demand int) {
	capacity -= demand

	edge := g.addEdge(src, dst, capacity, demand, edgeNormal)
	redge := g.addEdge(dst, src, 0, 0, edgeReverse)

	edge.ReverseEdge = redge
	redge.ReverseEdge = edge

	if demand != 0 {
		demandEdge1 := g.addEdge(src, demandSink, demand, 0, edgeDemand)
		demandEdge2 := g.addEdge(supplySource, dst, demand, 0, edgeDemand)

		edge.demandEdges = []*GraphEdge{demandEdge1, demandEdge2}
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

func (g *Graph) MaximizeFlow() bool {
	g.maxflowStats.reset()

	for {
		if augmented := g.augmentFlow(); !augmented {
			break
		}

		diag.Print(g.maxflowStats.String())
		g.maxflowStats.nextIteration()
	}

	return g.isFeasible()
}

func (g *Graph) isFeasible() bool {
	for _, edge := range g.vertices[supplySource].edges() {
		if edge.etype == edgeDemand && !edge.isSaturated() {
			return false
		}
	}

	return true
}

func (g *Graph) EdgesFromVertex(v GraphVertex) (edges []*GraphEdge) {
	for _, edge := range g.vertices[v].edges() {
		if edge.etype == edgeDemand {
			continue
		}

		edges = append(edges, edge)
	}

	return
}

func (g *Graph) Dot(path string) (err error) {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(buffer, "digraph G {\n")
	fmt.Fprintf(buffer, "rankdir=LR;\n")
	fmt.Fprintf(buffer, "labelloc=t; labeljust=l; ")

	label := fmt.Sprintf(`%s\nflow = %d\nfeasible = %v`,
		g.name, g.flow, g.isFeasible())
	fmt.Fprintf(buffer, "label=\"%s\";\n", label)

	dist := g.bfsNetwork()
	groups := make([][]GraphVertex, dist+1)

	for v, _ := range g.vertices {
		d := g.distances[v]
		groups[d] = append(groups[d], v)
	}

	groupVertices(buffer, groups[0], "source")
	groupVertices(buffer, groups[dist], "sink")

	for _, group := range groups[1:dist] {
		groupVertices(buffer, group, "same")
	}

	for _, edge := range g.edges() {
		var style string

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

		if capacity == MaxInt {
			capacityString = "∞"
		}

		fmt.Fprintf(buffer,
			"%s -> %s [label=\"%d (%d..%s)\", decorate,"+
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
