package main

type MaxFlowRIGenerator struct {
	DontAcceptRIGeneratorParams
}

func makeMaxFlowRIGenerator() *MaxFlowRIGenerator {
	return &MaxFlowRIGenerator{}
}

func (_ MaxFlowRIGenerator) String() string {
	return "maxflow"
}

func (_ MaxFlowRIGenerator) Generate() (RI RI, err error) {
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

type graph struct {
	nodes map[graphNode][]*graphEdge
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
