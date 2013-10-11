package main

import (
	"bytes"
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
	return nil, nil
}

type graphVertex string

type graphEdge struct {
	src graphVertex
	dst graphVertex

	capacity    int
	flow        int
	reverseEdge *graphEdge
}

func (edge graphEdge) residual() int {
	return edge.capacity - edge.flow
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
	if path.edges == nil || path.flow > edge.capacity {
		path.flow = edge.capacity
	}

	path.edges = append(path.edges, edge)
}

type graph struct {
	vertices map[graphVertex][]*graphEdge
}

func makeGraph() (g *graph) {
	g = &graph{}
	g.vertices = make(map[graphVertex][]*graphEdge)
	return
}

func (g graph) bfsPath(from graphVertex, to graphVertex) (path *augPath) {
	queue := []graphVertex{from}
	parentEdge := make(map[graphVertex]*graphEdge)
	seen := make(map[graphVertex]bool)

	seen[from] = true
	done := false

	for queue != nil && !done {
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

func (g graph) dot(path string) (err error) {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(buffer, "digraph G {\n")

	for _, edge := range g.edges() {
		fmt.Fprintf(buffer,
			"%s -> %s [label=\"capacity=%d, flow=%d\"];\n",
			edge.src, edge.dst, edge.capacity, edge.flow)
	}

	fmt.Fprintf(buffer, "}\n")

	return ioutil.WriteFile(path, buffer.Bytes(), 0644)
}
