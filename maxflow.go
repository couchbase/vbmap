package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

type MaxFlowRIGenerator struct {
	DontAcceptRIGeneratorParams
}

func makeMaxFlowRIGenerator() *MaxFlowRIGenerator {
	return &MaxFlowRIGenerator{}
}

func (_ MaxFlowRIGenerator) String() string {
	return "maxflow"
}

func (_ MaxFlowRIGenerator) Generate(params VbmapParams) (RI RI, err error) {
	g := buildFlowGraph(params)
	g.maximizeFlow()

	err = g.dot("flow_graph.dot")
	if err != nil {
		panic(err)
	}

	return nil, fmt.Errorf("unimplemented")
}

func buildFlowGraph(params VbmapParams) (g *graph) {
	g = makeGraph(params)
	tags := params.Tags.TagsList()
	tagsNodes := params.Tags.TagsNodesMap()
	maxReplicationsPerTag := params.NumSlaves / params.NumReplicas

	for _, node := range params.Nodes() {
		nodeTag := params.Tags[node]
		nodeSrcV := nodeSourceVertex(node)
		nodeSinkV := nodeSinkVertex(node)

		g.addSimpleEdge(source, nodeSrcV, params.NumSlaves)
		g.addSimpleEdge(nodeSinkV, sink, params.NumSlaves)

		for _, tag := range tags {
			if tag == nodeTag {
				continue
			}

			tagNodesCount := len(tagsNodes[tag])
			tagCapacity := min(tagNodesCount, maxReplicationsPerTag)

			tagV := tagVertex(tag)
			g.addEdge(nodeSrcV, tagV, tagCapacity)
		}
	}

	for tag, tagNodes := range tagsNodes {
		tagV := tagVertex(tag)
		tagNodesCount := len(tagNodes)
		tagEdgeCapacity := params.NumNodes - tagNodesCount

		for _, tagNode := range tagNodes {
			tagNodeV := nodeSinkVertex(tagNode)

			g.addEdge(tagV, tagNodeV, tagEdgeCapacity)
		}
	}

	return
}

type graphVertex interface {
	fmt.Stringer
}

type simpleVertex string

func (v simpleVertex) String() string {
	return string(v)
}

const (
	source simpleVertex = "source"
	sink   simpleVertex = "sink"
)

type tagVertex Tag

func (v tagVertex) String() string {
	return fmt.Sprintf("tag_%d", int(v))
}

type nodeSourceVertex Node

func (v nodeSourceVertex) String() string {
	return fmt.Sprintf("node_%d_source", int(v))
}

type nodeSinkVertex Node

func (v nodeSinkVertex) String() string {
	return fmt.Sprintf("node_%d_sink", int(v))
}

type graphEdge struct {
	src graphVertex
	dst graphVertex

	capacity    int
	flow        int
	reverseEdge *graphEdge
}

func (edge graphEdge) String() string {
	return fmt.Sprintf("%s->%s", edge.src, edge.dst)
}

func (edge graphEdge) residual() int {
	return edge.capacity - edge.flow
}

func (edge graphEdge) isForwardEdge() bool {
	return edge.capacity != 0
}

func (edge *graphEdge) pushFlow(flow int) {
	residual := edge.residual()
	if flow > residual {
		panic(fmt.Sprintf("Trying to push flow %d "+
			"via edge %s with residual capacity %d",
			flow, edge, residual))
	}

	edge.flow += flow
	if edge.reverseEdge != nil {
		edge.reverseEdge.flow -= flow
	}
}

type augPath struct {
	edges []*graphEdge
	flow  int
}

func makePath() (path *augPath) {
	path = &augPath{edges: nil, flow: 0}
	return
}

func (path *augPath) addEdge(edge *graphEdge) {
	residual := edge.residual()

	if path.edges == nil || path.flow > residual {
		path.flow = residual
	}

	path.edges = append(path.edges, edge)
}

type graph struct {
	params   VbmapParams
	vertices map[graphVertex][]*graphEdge
	flow     int
}

func makeGraph(params VbmapParams) (g *graph) {
	g = &graph{}
	g.vertices = make(map[graphVertex][]*graphEdge)
	g.params = params
	return
}

func (g graph) bfsPath(from graphVertex, to graphVertex) (path *augPath) {
	queue := []graphVertex{from}
	parentEdge := make(map[graphVertex]*graphEdge)
	seen := make(map[graphVertex]bool)

	seen[from] = true
	done := false

	for len(queue) != 0 && !done {
		v := queue[0]
		queue = queue[1:]

		for _, edge := range g.vertices[v] {
			_, present := seen[edge.dst]
			if !present && edge.residual() > 0 {
				queue = append(queue, edge.dst)
				seen[edge.dst] = true
				parentEdge[edge.dst] = edge

				if edge.dst == to {
					done = true
					break
				}
			}
		}
	}

	if done {
		edges := make([]*graphEdge, 0)
		path = makePath()

		v := to
		for v != from {
			edge := parentEdge[v]
			edges = append(edges, edge)
			v = edge.src
		}

		for i := len(edges) - 1; i >= 0; i-- {
			path.addEdge(edges[i])
		}
	}

	return
}

func (g *graph) addVertex(vertex graphVertex) {
	_, present := g.vertices[vertex]
	if !present {
		g.vertices[vertex] = nil
	}
}

func (g *graph) addSimpleEdge(src graphVertex, dst graphVertex, capacity int) {
	g.addVertex(src)
	g.addVertex(dst)

	edge := &graphEdge{src: src, dst: dst,
		capacity: capacity, flow: 0, reverseEdge: nil}

	g.vertices[src] = append(g.vertices[src], edge)
}

func (g *graph) addEdge(src graphVertex, dst graphVertex, capacity int) {
	g.addVertex(src)
	g.addVertex(dst)

	edge := &graphEdge{src: src, dst: dst, capacity: capacity, flow: 0}
	redge := &graphEdge{src: dst, dst: src, capacity: 0, flow: 0}

	edge.reverseEdge = redge
	redge.reverseEdge = edge

	g.vertices[src] = append(g.vertices[src], edge)
	g.vertices[dst] = append(g.vertices[dst], redge)
}

func (g graph) edges() (result []*graphEdge) {
	for _, vertexEdges := range g.vertices {
		for _, edge := range vertexEdges {
			result = append(result, edge)
		}
	}

	return
}

func (g *graph) pushFlow(path augPath) {
	g.flow += path.flow

	for _, edge := range path.edges {
		edge.pushFlow(path.flow)
	}
}

func (g *graph) maximizeFlow() {
	for {
		path := g.bfsPath(source, sink)
		if path == nil {
			break
		}

		g.pushFlow(*path)
	}
}

func (g graph) dot(path string) (err error) {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(buffer, "digraph G {\n")
	fmt.Fprintf(buffer, "rankdir=LR;\n")
	fmt.Fprintf(buffer, "labelloc=t; labeljust=l; ")
	fmt.Fprintf(buffer, "label=\"flow = %d\";\n", g.flow)

	groupVertices(buffer, []graphVertex{source}, "source")
	groupVertices(buffer, []graphVertex{sink}, "sink")

	nodeSrcVertices := make([]graphVertex, 0)
	nodeSinkVertices := make([]graphVertex, 0)
	tagVertices := make([]graphVertex, 0)

	for _, node := range g.params.Nodes() {
		nodeSrcVertices = append(nodeSrcVertices, nodeSourceVertex(node))
		nodeSinkVertices = append(nodeSinkVertices, nodeSinkVertex(node))
	}

	for _, tag := range g.params.Tags.TagsList() {
		tagVertices = append(tagVertices, tagVertex(tag))
	}

	groupVertices(buffer, nodeSrcVertices, "same")
	groupVertices(buffer, tagVertices, "same")
	groupVertices(buffer, nodeSinkVertices, "same")

	for _, edge := range g.edges() {
		style := "solid"
		if !edge.isForwardEdge() {
			style = "dashed"
		}

		color := "red"
		if edge.residual() > 0 {
			color = "darkgreen"
		}

		fmt.Fprintf(buffer,
			"%s -> %s [label=\"%d (cap %d)\", decorate,"+
				" style=%s, color=%s];\n",
			edge.src, edge.dst, edge.flow,
			edge.capacity, style, color)
	}

	fmt.Fprintf(buffer, "}\n")

	return ioutil.WriteFile(path, buffer.Bytes(), 0644)
}

func groupVertices(w io.Writer, vertices []graphVertex, rank string) {
	fmt.Fprintf(w, "{\n")
	fmt.Fprintf(w, "rank=%s;\n", rank)

	for _, v := range vertices {
		fmt.Fprintf(w, "%s;\n", v)
	}

	fmt.Fprintf(w, "}\n")
}
