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

type graphNode string

type graphEdge struct {
	src graphNode
	dst graphNode

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
	nodes map[graphNode][]*graphEdge
}

func makeGraph() (g *graph) {
	g = &graph{}
	g.nodes = make(map[graphNode][]*graphEdge)
	return
}

func (g graph) bfsPath(from graphNode, to graphNode) (path *augPath) {
	queue := []graphNode{from}
	parentEdge := make(map[graphNode]*graphEdge)
	seen := make(map[graphNode]bool)

	seen[from] = true
	done := false

	for queue != nil && !done {
		v := queue[0]
		queue = queue[1:]

		for _, edge := range g.nodes[v] {
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

func (g *graph) addNode(node graphNode) {
	_, present := g.nodes[node]
	if !present {
		g.nodes[node] = nil
	}
}

func (g *graph) addEdge(src graphNode, dst graphNode, capacity int) {
	g.addNode(src)
	g.addNode(dst)

	edge := &graphEdge{src: src, dst: dst, capacity: capacity, flow: 0}
	redge := &graphEdge{src: dst, dst: src, capacity: 0, flow: 0}

	edge.reverseEdge = redge
	redge.reverseEdge = edge

	g.vertices[src] = append(g.vertices[src], edge)
	g.vertices[dst] = append(g.vertices[dst], redge)
}

func (g graph) edges() (result []*graphEdge) {
	for _, nodeEdges := range g.nodes {
		for _, edge := range nodeEdges {
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
