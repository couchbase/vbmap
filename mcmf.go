// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software is governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"fmt"
)

// BIG is a constant cost used to heavily penalize edge usage.
// Used for lexicographic optimization where minimizing edge count takes
// priority over other, smaller cost differences.
const BIG = 1000000

// inf is a value representing "infinity" for shortest path distances.
// Deliberately smaller than math.MaxInt to avoid overflow when computing
// dist[v] + cost + h[v] - h[e.to] during edge relaxation.
const INF = 1 << 30

// edge represents a directed edge in the flow network.
// Each edge stores: destination vertex (to), remaining capacity (cap),
// traversal cost (cost), and index of reverse edge in g[to] (rev).
type edge struct {
	to   int // destination vertex
	cap  int // remaining flow capacity
	cost int // edge cost
	rev  int // index of reverse edge in g[to]
}

// mcmfSolver implements min-cost max-flow using successive shortest
// augmenting paths with vertex potentials (a.k.a. cost scaling).
// The algorithm works as follows:
// 1. Find shortest path from source to sink using reduced costs
// 2. Push as much flow as possible along this path
// 3. Update potentials and repeat until desired flow is reached
//
// Vertex potentials (h) maintain reduced costs to ensure non-negative
// edge weights, enabling Dijkstra-like shortest path finding. After each
// iteration, potentials are updated: h[v] += dist[v] - dist[source]
type mcmfSolver struct {
	n     int      // number of vertices in the graph
	g     [][]edge // adjacency list: g[u] = list of edges from u
	h     []int    // vertex potentials for reduced costs
	dist  []int    // shortest path distances in reduced cost graph
	prevv []int    // previous vertex in shortest path (reconstruction)
	preve []int    // edge index in previous vertex's adjacency list
}

// newMCMFSolver creates a new min-cost max-flow solver for a graph
// with n vertices. Vertices are numbered 0 to n-1.
func newMCMFSolver(n int) *mcmfSolver {
	return &mcmfSolver{
		n:     n,
		g:     make([][]edge, n),
		h:     make([]int, n),
		dist:  make([]int, n),
		prevv: make([]int, n),
		preve: make([]int, n),
	}
}

// addEdge adds a directed edge (from -> to) with capacity and cost,
// and also creates an implicit reverse edge (to -> from) with
// zero capacity and negative cost for flow cancellation.
//
// The reverse edge allows the algorithm to "undo" flow by pushing
// flow back along the reverse direction, enabling efficient
// augmentation without needing to recompute flows from scratch.
func (s *mcmfSolver) addEdge(from, to, cap, cost int) {
	fwd := edge{to: to, cap: cap, cost: cost, rev: len(s.g[to])}
	rev := edge{to: from, cap: 0, cost: -cost, rev: len(s.g[from])}
	s.g[from] = append(s.g[from], fwd)
	s.g[to] = append(s.g[to], rev)
}

// parentEdges returns the adjacency list of v's parent in the shortest-path
// tree. Because every vertex wants to know where it came from.
func (s *mcmfSolver) parentEdges(v int) []edge {
	return s.g[s.prevv[v]]
}

// minCostMaxFlow computes a flow of size flowNeeded from sV (source)
// to t (sink) with minimum total cost. Returns:
// - actual flow pushed (should equal flowNeeded)
// - total cost of the flow
// - error if flow cannot be pushed
//
// Algorithm: successive shortest augmenting paths
// For each iteration:
// 1. Use BFS with reduced costs to find shortest path
// 2. Bottleneck flow d = min remaining capacity on path
// 3. Push flow d along the path, updating edge capacities
// 4. Update vertex potentials to maintain reduced costs
//
// Reduced costs ensure non-negative edge weights after first iteration,
// allowing efficient shortest path computation. The reduced cost
// c_h(u,v) = c(u,v) + h[u] - h[v] is always >= 0 for edges
// on which capacity remains.
func (s *mcmfSolver) minCostMaxFlow(sV, t int,
	flowNeeded int) (int, int, error) {

	resultFlow := 0
	resultCost := 0

	for flowNeeded > 0 {
		// Initialize per-iteration shortest-path state.
		//
		// Important: prevv/preve must be reset each iteration. Otherwise,
		// vertices that are unreachable in the current residual graph could
		// still have a prevv from a previous iteration, and we'd incorrectly
		// update their potentials (h) using an "infinite" dist.
		for i := 0; i < s.n; i++ {
			s.dist[i] = INF
			s.prevv[i] = -1
			s.preve[i] = -1
		}
		s.dist[sV] = 0

		// SPFA-like BFS to find shortest path using reduced costs
		// Only considers edges with remaining capacity
		q := []int{sV}
		for len(q) > 0 {
			v := q[0]
			q = q[1:]

			for ei, e := range s.g[v] {
				// Skip edges with no capacity
				if e.cap <= 0 {
					continue
				}
				// Relaxation using reduced cost:
				// c_h(v, e.to) = e.cost + h[v] - h[e.to]
				if s.dist[e.to] > s.dist[v] + e.cost +
					s.h[v] - s.h[e.to] {
					s.dist[e.to] = s.dist[v] + e.cost +
						s.h[v] - s.h[e.to]
					s.prevv[e.to] = v
					s.preve[e.to] = ei
					q = append(q, e.to)
				}
			}
		}

		// No path found - cannot push required flow
		if s.prevv[t] == -1 {
			return resultFlow, resultCost,
				fmt.Errorf("cannot push required flow")
		}

		// Update vertex potentials based on shortest path distances
		// This maintains the property that reduced costs are
		// non-negative
		for v := 0; v < s.n; v++ {
			if s.dist[v] < INF {
				// dist[sV] is always 0.
				s.h[v] += s.dist[v]
			}
		}

		// Find bottleneck flow (minimum capacity along path)
		d := flowNeeded
		for v := t; v != sV; v = s.prevv[v] {
			ei := s.preve[v]
			e := &s.parentEdges(v)[ei]
			d = min(d, e.cap)
		}

		// Push flow and update result counters
		flowNeeded -= d
		resultFlow += d
		resultCost += d * s.h[t]

		// Update edge capacities: decrease forward edges,
		// increase reverse edges (for flow cancellation)
		for v := t; v != sV; v = s.prevv[v] {
			ei := s.preve[v]
			fwd := &s.parentEdges(v)[ei]
			fwd.cap -= d
			s.g[fwd.to][fwd.rev].cap += d
		}
	}

	return resultFlow, resultCost, nil
}
