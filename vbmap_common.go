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
	"errors"
	"fmt"
	"sort"
)

var (
	ErrorNoSolution = errors.New("Couldn't find a solution")
)

type Node int
type NodeSlice []Node

func (s NodeSlice) Len() int           { return len(s) }
func (s NodeSlice) Less(i, j int) bool { return int(s[i]) < int(s[j]) }
func (s NodeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Tag int
type TagSlice []Tag
type TagMap map[Node]Tag

func (s TagSlice) Len() int           { return len(s) }
func (s TagSlice) Less(i, j int) bool { return int(s[i]) < int(s[j]) }
func (s TagSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type VbmapParams struct {
	Tags TagMap

	NumNodes    int
	NumSlaves   int
	NumVBuckets int
	NumReplicas int
}

type SearchParams struct {
	NumRRetries int

	StrictReplicaBalance bool
	RelaxSlaveBalance    bool
	RelaxReplicaBalance  bool
	RelaxNumSlaves       bool

	DotPath string
}

func (params VbmapParams) Nodes() (nodes []Node) {
	for n := 0; n < params.NumNodes; n++ {
		nodes = append(nodes, Node(n))
	}

	return
}

func (params VbmapParams) String() string {
	return fmt.Sprintf("VbmapParams{Tags: %s, NumNodes: %d, "+
		"NumSlaves: %d, NumVBuckets: %d, NumReplicas: %d}",
		params.Tags, params.NumNodes, params.NumSlaves,
		params.NumVBuckets, params.NumReplicas)
}

func (tags TagMap) String() string {
	return fmt.Sprintf("%v", map[Node]Tag(tags))
}

func (tags TagMap) TagsList() (result []Tag) {
	seen := make(map[Tag]bool)

	for _, t := range tags {
		if _, present := seen[t]; !present {
			result = append(result, t)
			seen[t] = true
		}
	}

	sort.Sort(TagSlice(result))

	return
}

func (tags TagMap) TagsCount() int {
	return len(tags.TagsList())
}

func (tags TagMap) TagsNodesMap() (m map[Tag][]Node) {
	m = make(map[Tag][]Node)
	for _, tag := range tags.TagsList() {
		m[tag] = nil
	}

	for node, tag := range tags {
		m[tag] = append(m[tag], node)
		sort.Sort(NodeSlice(m[tag]))
	}

	return
}

type RIBalance uint64

const (
	ReplicasBalanced RIBalance = 1 << iota
	SlavesBalanced
)

type RI struct {
	Matrix [][]int

	balance RIBalance
	params  VbmapParams
}

func MakeRI(m [][]int, params VbmapParams) RI {
	ri := RI{}
	ri.Matrix = m
	ri.balance = ReplicasBalanced | SlavesBalanced
	ri.params = params

	for node := 0; node < params.NumNodes; node++ {
		sum := 0
		for i := 0; i < params.NumNodes; i++ {
			elem := m[i][node]

			sum += elem
			if elem > 1 {
				ri.balance &^= SlavesBalanced
			}
		}

		if sum != params.NumSlaves {
			ri.balance &^= ReplicasBalanced
		}
	}

	return ri
}

func (ri RI) IsBalanced(need RIBalance) bool {
	return ri.balance&need != 0
}

func (ri RI) NumInboundReplications(node Node) int {
	j := int(node)

	result := 0
	for i := range ri.Matrix {
		result += ri.Matrix[i][j]
	}

	return result
}

type RIGenerator interface {
	SetParams(params map[string]string) error
	Generate(params VbmapParams, searchParams SearchParams) (RI, error)
	fmt.Stringer
}

type DontAcceptRIGeneratorParams struct{}

func (DontAcceptRIGeneratorParams) SetParams(params map[string]string) error {
	for k := range params {
		return fmt.Errorf("unsupported parameter '%s'", k)
	}

	return nil
}

func (ri RI) String() string {
	buffer := matrixToBuffer(ri.Matrix, ri.params)

	fmt.Fprintf(
		buffer, "Slaves balanced: %t\n", ri.IsBalanced(SlavesBalanced))
	fmt.Fprintf(
		buffer,
		"Replicas balanced: %t\n",
		ri.IsBalanced(ReplicasBalanced))

	return buffer.String()
}

func matrixToString(m [][]int, params VbmapParams) string {
	buffer := matrixToBuffer(m, params)
	return buffer.String()
}

func matrixToBuffer(m [][]int, params VbmapParams) *bytes.Buffer {
	buffer := &bytes.Buffer{}

	nodes := params.Nodes()

	fmt.Fprintf(buffer, "    |")
	for _, node := range nodes {
		fmt.Fprintf(buffer, "%3d ", params.Tags[node])
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "----|")
	for range nodes {
		fmt.Fprintf(buffer, "----")
	}
	fmt.Fprintf(buffer, "|\n")

	for i, row := range m {
		fmt.Fprintf(buffer, "%3d |", params.Tags[Node(i)])
		rowSum := 0
		for _, elem := range row {
			rowSum += elem
			fmt.Fprintf(buffer, "%3d ", elem)
		}
		fmt.Fprintf(buffer, "| %d\n", rowSum)
	}

	fmt.Fprintf(buffer, "____|")
	for range nodes {
		fmt.Fprintf(buffer, "____")
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "    |")
	for i := range nodes {
		colSum := 0
		for j := range nodes {
			colSum += m[j][i]
		}

		fmt.Fprintf(buffer, "%3d ", colSum)
	}
	fmt.Fprintf(buffer, "|\n")

	return buffer
}
