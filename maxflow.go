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

const (
	source = iota
	sink
	tagsStart
)

type graph struct {
	nodes [][]graphEdge
}

type graphEdge struct {
	src         int
	dst         int
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

func (g graph) bfsPath() (path *augPath) {
	queue := []int{sink}
	parentEdge := make(map[int]*graphEdge)
	seen := make(map[int]bool)

	seen[sink] = true
	done := false

	for queue != nil && !done {
		v := queue[0]
		queue = queue[1:]

		for _, edge := range g.nodes[v] {
			_, present := seen[edge.dst]
			if !present && edge.residual() > 0 {
				queue = append(queue, edge.dst)
				seen[edge.dst] = true
				parentEdge[edge.dst] = &edge

				if edge.dst == sink {
					done = true
					break
				}
			}
		}
	}

	if done {
		edges := make([]*graphEdge, 0)
		path = makePath()

		v := sink
		for v != source {
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
