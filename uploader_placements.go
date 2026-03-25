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
	"slices"
)

func getUploader(vb int, uploaders Uploaders) Node {
	return Node(uploaders[vb])
}

func getFromScratchUploaders(vbmap Vbmap, prevUploaders Uploaders,
	prevVbmap Vbmap) []int {
	fromScratch := []int{}
	for vb, chain := range vbmap {
		uploader := getUploader(vb, prevUploaders)
		prevChain := prevVbmap[vb]
		active := chain[0]
		if active != uploader && slices.Contains(prevChain, active) {
			fromScratch = append(fromScratch, vb)
		}
	}
	return fromScratch
}

func makeActivePlacements(assignment []Node, ri RIMap) []ActivePlacement {
	numVbs := len(assignment)
	aps := make([]ActivePlacement, numVbs)
	for vb := 0; vb < numVbs; vb++ {
		active := assignment[vb]
		ap := makeActivePlacement(vb, active, ri)
		aps[vb] = ap
	}
	return aps
}

func shuffleUploaders(uploaders Uploaders,
	order map[int]int) Uploaders {

	if uploaders == nil {
		return nil
	}
	if order == nil {
		return uploaders
	}
	numVbs := len(uploaders)
	shuffled := make(Uploaders, numVbs)
	for vb := 0; vb < numVbs; vb++ {
		newPos := order[vb]
		if newPos >= 0 && newPos < numVbs {
			shuffled[newPos] = uploaders[vb]
		}
	}
	return shuffled
}

func getActivePlacementsWithUploaders(
	ri RIMap,
	activeNumVbsMap map[Node]int,
	prevParams *prevVbmapParams,
	params *VbmapParams,
	currentUploaders Uploaders) ([]ActivePlacement, error) {

	numVbs := params.NumVBuckets
	numNodes := params.NumNodes

	// Sanity-check that node quotas account for all vbuckets.
	totalQuota := 0
	for _, quota := range activeNumVbsMap {
		totalQuota += quota
	}
	if totalQuota != numVbs {
		return nil, fmt.Errorf("quota mismatch: %d != %d",
			totalQuota, numVbs)
	}

	const src = 0

	vbIndex := func(i int) int {
		return 1 + numNodes + i
	}

	sink := 1 + numNodes + numVbs

	solver := newMCMFSolver(sink + 1)

	// Node -> sink edges enforce per-node active placement quotas.
	for i, n := range params.Nodes() {
		quota := activeNumVbsMap[n]
		solver.addEdge(i+1, sink, quota, 0)
	}

	for vb := 0; vb < numVbs; vb++ {
		// Source -> vb edge ensures each vbucket is assigned exactly
		// once.
		solver.addEdge(src, vbIndex(vb), 1, 0)

		uploader := getUploader(vb, currentUploaders)
		prevChain := prevParams.vbmap[vb]

		for i, n := range params.Nodes() {
			// Cost policy:
			// - 0 for uploader (preferred)
			// - BIG-1 for previous active (allow but discourage)
			// - BIG for previous chain member (strongly discourage
			//       reusing chain)
			// - 1 for other nodes (neutral)
			cost := 0
			if n == uploader {
				cost = 0
			} else if len(prevChain) > 0 && n == prevChain[0] {
				cost = BIG - 1
			} else if slices.Contains(prevChain, n) {
				cost = BIG
			} else {
				cost = 1
			}
			solver.addEdge(vbIndex(vb), i+1, 1, cost)
		}
	}

	flow, cost, err := solver.minCostMaxFlow(src, sink, numVbs)
	if err != nil {
		diag.Printf("MCMF error: %v", err)
		return nil, err
	}
	if flow != numVbs {
		diag.Printf("MCMF flow mismatch: %d != %d", flow, numVbs)
		return nil, fmt.Errorf("flow mismatch: %d != %d",
			flow, numVbs)
	}

	diag.Printf("MCMF complete: flow=%d, cost=%d", flow, cost)

	assignment := make([]Node, numVbs)
	for vb := 0; vb < numVbs; vb++ {
		assigned := false
		// Pick the saturated vb -> node edge,
		// which represents the chosen assignment.
		for _, e := range solver.g[vbIndex(vb)] {
			if e.cap == 0 && e.to != 0 {
				assignment[vb] = params.Nodes()[e.to-1]
				assigned = true
				break
			}
		}
		if !assigned {
			// Fallback guard for unexpected residual-graph states.

			// In min-cost max-flow, the final graph is a residual
			// graph: an edge being “chosen” is usually detected by:
			//
			// - forward edge capacity became 0 (fully used), and
			// - reverse edge capacity became > 0 (flow can be undone).
			// - The main path already reads assignments by finding
			//   saturated vb -> node edges.
			//
			// The fallback does an extra defensive check
			// (revEdge.cap > 0) in case residual state is unusual
			// or ambiguous, so we still confirm “this edge really
			// carried flow” before deciding the vbucket assignment,
			// instead of failing or picking the wrong edge.
			for i, e := range solver.g[vbIndex(vb)] {
				if e.cap == 0 {
					revIdx := e.rev
					revEdge := solver.g[e.to][revIdx]
					if revEdge.cap > 0 {
						assignment[vb] =
							params.Nodes()[e.to-1]
						diag.Printf(
							"vb %d assigned via check #%d",
							vb, i)
						assigned = true
						break
					}
				}
			}
		}
		if !assigned {
			return nil, fmt.Errorf("no assignment for vb %d",
				vb)
		}
	}

	return makeActivePlacements(assignment, ri), nil
}
