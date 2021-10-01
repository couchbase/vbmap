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
	"math/rand"
	"sort"
)

type MaxFlowRIGenerator struct {
	dotPath    string
	dotVerbose bool
}

func makeMaxFlowRIGenerator() *MaxFlowRIGenerator {
	return &MaxFlowRIGenerator{dotPath: "", dotVerbose: false}
}

func (_ MaxFlowRIGenerator) String() string {
	return "maxflow"
}

func (gen *MaxFlowRIGenerator) SetParams(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "dot":
			gen.dotPath = v
		case "dot-verbose":
			gen.dotVerbose = true
		default:
			return fmt.Errorf("unsupported parameter '%s'", k)
		}
	}

	return nil
}

func (gen MaxFlowRIGenerator) Generate(params VbmapParams,
	searchParams SearchParams) (ri RI, err error) {

	g := buildFlowGraph(params)

	diag.Print("Constructed flow graph.\n")
	diag.Print(g.graphStats)

	feasible, _ := g.FindFeasibleFlow()
	if gen.dotPath != "" {
		err := g.Dot(gen.dotPath, gen.dotVerbose)
		if err != nil {
			diag.Printf("Couldn't create dot file %s: %s",
				gen.dotPath, err.Error())
		}
	}

	if !feasible {
		err = ErrorNoSolution
		return
	}

	ri = graphToRI(g, params)
	return
}

func buildFlowGraph(params VbmapParams) (g *Graph) {
	graphName := fmt.Sprintf("Flow graph for RI (%s)", params)
	g = NewGraph(graphName)

	tags := params.Tags.TagsList()
	tagsNodes := params.Tags.TagsNodesMap()

	maxReplicationsPerTag := 0
	if params.NumReplicas != 0 {
		maxReplicationsPerTag = params.NumSlaves / params.NumReplicas
	}

	nodes := params.Nodes()
	for _, nodeIx := range rand.Perm(len(nodes)) {
		node := nodes[nodeIx]
		nodeTag := params.Tags[node]
		nodeSrcV := NodeSourceVertex(node)

		g.AddEdge(Source, nodeSrcV, params.NumSlaves, params.NumSlaves)

		for _, tagIx := range rand.Perm(len(tags)) {
			tag := tags[tagIx]

			if tag == nodeTag {
				continue
			}

			tagNodesCount := len(tagsNodes[tag])
			tagCapacity := Min(tagNodesCount, maxReplicationsPerTag)

			tagV := TagVertex(tag)
			g.AddEdge(nodeSrcV, tagV, tagCapacity, 0)
		}
	}

	for _, tagIx := range rand.Perm(len(tags)) {
		tag := tags[tagIx]
		tagNodes := tagsNodes[tag]
		tagV := TagVertex(tag)

		g.AddEdge(tagV, Sink, params.NumSlaves*len(tagNodes), 0)
	}

	return
}

type nodeCount struct {
	node  Node
	count int
}

func graphToRI(g *Graph, params VbmapParams) (ri RI) {
	ri.Matrix = make([][]int, params.NumNodes)
	tags := params.Tags.TagsNodesMap()

	for i := range params.Nodes() {
		ri.Matrix[i] = make([]int, params.NumNodes)
	}

	for _, tag := range params.Tags.TagsList() {
		tagV := TagVertex(tag)
		tagNodes := tags[tag]

		reps := make([]nodeCount, 0)

		for _, edge := range g.EdgesToVertex(tagV) {
			srcNode := Node(edge.Src.(NodeSourceVertex))

			count := nodeCount{srcNode, edge.Flow()}
			reps = append(reps, count)
		}

		sort.Slice(
			reps,
			func(i, j int) bool {
				return reps[i].node < reps[j].node
			})
		slaveIx := 0

		for _, pair := range reps {
			count := pair.count
			srcNode := int(pair.node)

			for count > 0 {
				dstNode := tagNodes[slaveIx]

				if ri.Matrix[srcNode][dstNode] > 0 {
					panic(fmt.Sprintf("Forced to use the "+
						"same slave %d twice (tag %v)",
						dstNode, tag))
				}

				ri.Matrix[srcNode][dstNode] = 1
				count -= 1

				slaveIx = (slaveIx + 1) % len(tagNodes)
			}
		}
	}

	return
}
