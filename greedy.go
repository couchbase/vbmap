package main

import (
	"math/rand"
	"time"
)

type Matrix [][]int

type SlavesMap map[Node]int
type RIMap map[Node]SlavesMap

type Costs map[Node]int

type ReplicaCosts Costs
type SlaveCosts map[Node]map[Node]int

type ActivePlacement struct {
	vb        int
	active    Node
	slavesMap SlavesMap
}

type SlavePlacement struct {
	active   Node
	tagsUsed map[Tag]bool
}

type GreedyRI struct {
	riMap  RIMap
	params VbmapParams
}

type prevVbmapParams struct {
	NumNodes     int
	riMap        RIMap
	activeVbsMap map[Node][]int
	R            Matrix
	vbmap        Vbmap
}

type VbCtx struct {
	vb    int
	chain []Node
}

type GreedyRIGenerator struct {
	DontAcceptRIGeneratorParams
}

func makeGreedyRIGenerator() *GreedyRIGenerator {
	return &GreedyRIGenerator{}
}

func (GreedyRIGenerator) String() string {
	return "greedy"
}

func (GreedyRIGenerator) Generate(_ VbmapParams, _ SearchParams) (RI, error) {
	return RI{}, nil
}

func (ri GreedyRI) String() string {

	nodesIdx := make(map[Node]int)

	for idx, node := range ri.params.Nodes() {
		nodesIdx[node] = idx
	}

	matrix := makeMatrix(ri.params.NumNodes, ri.params.NumNodes)

	for a, nodesMap := range ri.riMap {
		aIdx := nodesIdx[a]
		for r, _ := range nodesMap {
			rIdx := nodesIdx[r]
			matrix[aIdx][rIdx] = 1
		}
	}

	buffer := matrixToBuffer(matrix, ri.params)
	return buffer.String()
}

func (sm SlavesMap) Copy(in SlavesMap) {
	for node, v := range in {
		sm[node] = v
	}
	return
}

func (sm SlavesMap) Delete(node Node) {
	delete(sm, node)
}

func ppMatrix(m Matrix, banner string) {
	diag.Printf("%s:\n", banner)
	for _, row := range m {
		diag.Printf("%v\n", row)
	}
}

func makeMatrix(rows int, cols int) Matrix {
	M := make(Matrix, rows)

	for i := range M {
		M[i] = make([]int, cols)
	}
	return M
}

func makeRIMap(nodes []Node) RIMap {
	ri := make(RIMap)
	for _, n := range nodes {
		ri[n] = make(map[Node]int)
	}
	return ri
}

func makeCosts(params *VbmapParams) Costs {
	costs := make(map[Node]int)
	for _, n := range params.Nodes() {
		costs[n] = 0
	}
	return costs
}

func makeReplicaCosts(params *VbmapParams) ReplicaCosts {
	return ReplicaCosts(makeCosts(params))
}

func makeSlaveCosts(params *VbmapParams) SlaveCosts {
	costs := make(SlaveCosts)
	for _, n := range params.Nodes() {
		costs[n] = make(map[Node]int)
	}

	return costs
}

// Returns the best slave node for the node associated with the given placement
// and whether or not it results in a new slave for that node in the event
// prevRIMap is non-nil.

// sp: the slave placement for which we are trying to find a valid slave node
// prevRI: if non-nil / non-empty, the RI matrix prior to the creation of the
//         current RI matrix
// lowestCostNodes: list of nodes to pick the slave from.

func getBestSlave(
	sp SlavePlacement, prevRI RIMap, lowestCostNodes []Node) (Node, bool) {

	newSlavePicked := false
	candidates := lowestCostNodes

	if prevRI != nil {
		// intersect the candidates with the previous slave nodes
		candidates = []Node{}

		if prevSlaveNodes, ok := prevRI[sp.active]; ok {
			for _, node := range lowestCostNodes {
				if prevSlaveNodes[node] > 0 {
					candidates = append(candidates, node)
				}
			}
		}
		// if the intersection is zero, fall back to all lowest cost nodes
		if len(candidates) == 0 {
			candidates = lowestCostNodes
			newSlavePicked = true
		}
	}

	// select a winner at random from the list of candidates
	winnerId := rand.Intn(len(candidates))
	return candidates[winnerId], newSlavePicked
}

func getLeastCostSlaves(
	sp SlavePlacement, tagMap TagMap, costs Costs, currRI RIMap) []Node {

	minCost := -1
	activeTag := tagMap[sp.active]

	candidates := []Node{}
	// find the lowest cost valid slave nodes for the active node being
	// considered
	for node, cost := range costs {
		nodeTag := tagMap[node]
		// A node is a valid slave for a given active node if its:
		// 1) not in the same tag as the active
		// 2) not a node that is already a slave for the given active node
		// 3) not using the same tag as previously assigned slaves for this node
		//    at this time
		isValidNode := nodeTag != activeTag &&
			currRI[sp.active][node] == 0 &&
			!sp.tagsUsed[nodeTag]

		if isValidNode {
			if minCost == -1 || cost < minCost {
				// first valid node or node with lower cost encountered.
				minCost = cost
				candidates = []Node{node}
			} else if cost == minCost {
				candidates = append(candidates, node)
			}
		}
	}

	return candidates
}

// Returns the index of the best placement from the given slice of
// SlavePlacements and the associated best slave node. "Best" in this case
// means a slave that doesn't result in the creation of a new slave when
// compared to the prevRI map, if provided. We return bestIdx as -1, when there
// is no slave placement with a valid slave.

// sps: the list of placements to look through to find one that ideally results
//      in the creation of no new slave relationships; if no such placement is
//      found, the first placement index for which a slave can be assigned is
//      returned
// tagMap: the map of node to tag
// numTags: the total number of tags
// costs: the map of node to number of inbound slave relationships it currently
//        has.
// currRI: the state of the RI map that's currently being built
// prevRI: the RI matrix of the system that preceded the creation of the current
//         RI matrix.

func getBestSlavePlacementIdx(
	sps []SlavePlacement,
	tagMap TagMap,
	numTags int,
	costs Costs,
	currRI RIMap,
	prevRI RIMap) (int, Node) {

	bestIdx := -1
	var bestSlave Node

	for idx := 0; idx < len(sps); idx++ {
		sp := sps[idx]

		// reset tagsUsed, if a slave has been picked from all the possible
		// tags.
		if len(sp.tagsUsed) == numTags-1 {
			sp.tagsUsed = make(map[Tag]bool)
		}

		leastCostSlaves := getLeastCostSlaves(sp, tagMap, costs, currRI)

		// if there is no valid slave to pick from, continue to the next sp.
		if len(leastCostSlaves) == 0 {
			continue
		}

		slave, newSlavePicked := getBestSlave(sp, prevRI, leastCostSlaves)

		if !newSlavePicked {
			bestIdx = idx
			bestSlave = slave
			break
		}

		if bestIdx == -1 {
			// initialize bestIdx and bestSlave on the first run through.
			bestIdx = idx
			bestSlave = slave
		}

	}

	return bestIdx, bestSlave
}

func generateRI(params *VbmapParams, prevRI RIMap) RIMap {
	ri := makeRIMap(params.Nodes())
	costs := makeCosts(params)

	nodesTagMap := params.Tags

	sps := []SlavePlacement{}
	for active, _ := range ri {
		sps = append(sps, SlavePlacement{active, make(map[Tag]bool)})
	}

	numTags := params.Tags.TagsCount()

	for s := 0; s < params.NumSlaves; s++ {
		rand.Shuffle(len(sps),
			func(i, j int) {
				sps[i], sps[j] = sps[j], sps[i]
			})

		for i := 0; i < len(sps); i++ {
			bestIdx, bestSlave := getBestSlavePlacementIdx(sps[i:], nodesTagMap,
				numTags, costs, ri, prevRI)

			// bestIdx is -1, when there exists no sp which needs a slave
			// assignment.
			if bestIdx == -1 {
				continue
			}

			// Swap the slave placement at best index with the one at index i.
			sps[i], sps[i+bestIdx] = sps[i+bestIdx], sps[i]

			sp := sps[i]

			ri[sp.active][bestSlave] = 1
			costs[bestSlave] += 1

			// mark the bestSlave's tag as picked, to avoid picking
			// another slave from the same tag.
			sp.tagsUsed[nodesTagMap[bestSlave]] = true
		}
	}

	return ri
}

// Returns the best replica node for the given ActivePlacement and whether or
// not it results in the creation of a new replica. "Best" in this case means a
// node that is a valid replica node for the activePlacment and is:
//
// (1) a node that is carrying the minimum total number of replicas and
// (2) the minimum number of replicas from that active node
// at this point in the creation of the current replication matrix.
//
// We maintain two costs; replicaCosts and slaveCosts.
// 1) replicaCost signifies the number of replicas that are placed on a node
// across all vbuckets.
// 2) slaveCost signifies the number of replicas that are placed on a node for
// all vbuckets that share the same active node.
func getBestReplica(
	ap ActivePlacement,
	tagMap TagMap,
	replicaCosts map[Node]int,
	slaveCosts map[Node]int,
	prevVbmap Vbmap) (Node, bool) {

	var lowestCostNodes []Node
	minReplicaCost := -1
	minSlaveCost := -1

	for node, cost := range replicaCosts {
		// valid replica nodes for vbucket that is on a given active node are:
		// 1) on one of the valid slave nodes of the active node
		isValidNode := ap.slavesMap[node] != 0

		if isValidNode {
			slaveCost := slaveCosts[node]
			// we first compare the total replicas node is carrying and then
			// compare the total number of replicas the node is carrying from
			// the given active node (what is called here the "slave coset").
			// comparing total replica costs causes total replicas to end up
			// being balanced; comparing slave costs tends to cause replicas
			// from a given node to be approximately evenly balanced across the
			// slaves of that node.
			isCostLower := cost < minReplicaCost ||
				(cost == minReplicaCost && slaveCost < minSlaveCost)

			if minReplicaCost < 0 || isCostLower {
				// first valid node encountered or cost is lower
				minReplicaCost = cost
				minSlaveCost = slaveCost
				lowestCostNodes = []Node{node}
			} else if cost == minReplicaCost && slaveCost == minSlaveCost {
				lowestCostNodes = append(lowestCostNodes, node)
			}
		}
	}

	newReplicaPicked := false
	candidates := lowestCostNodes

	if prevVbmap != nil {
		candidates = []Node{}
		preferredReplicas := prevVbmap[ap.vb][1:]
		for _, n := range lowestCostNodes {
			for _, pr := range preferredReplicas {
				if n == pr {
					candidates = append(candidates, n)
				}
			}
		}

		if len(candidates) == 0 {
			candidates = lowestCostNodes
			newReplicaPicked = true
		}
	}

	// pick a winner at random from the list of candidates
	winnerId := rand.Intn(len(candidates))
	return candidates[winnerId], newReplicaPicked
}

func usedSameNodeTwice(chain []Node) bool {
	active := chain[0]
	replicas := chain[1:]

	m := make(map[Node]bool)

	m[active] = true

	for _, r := range replicas {
		if _, ok := m[r]; ok {
			return true
		}
		m[r] = true
	}
	return false
}

func shuffleVbmap(vbmap [][]Node) ([][]Node, map[int]int) {

	if vbmap == nil {
		return nil, nil
	}

	vbCtxs := []VbCtx{}
	order := make(map[int]int)

	for vb, chain := range vbmap {
		vbCtxs = append(vbCtxs, VbCtx{vb, chain})
	}

	rand.Shuffle(len(vbCtxs),
		func(i, j int) {
			vbCtxs[i], vbCtxs[j] = vbCtxs[j], vbCtxs[i]
		})

	newVbmap := [][]Node{}

	for idx, vbCtx := range vbCtxs {
		newVbmap = append(newVbmap, vbCtx.chain)
		order[vbCtx.vb] = idx
	}

	return newVbmap, order
}

func generateVbmap(params *VbmapParams, ri RIMap,
	prevParams *prevVbmapParams) Vbmap {

	activeVbs := SpreadSum(params.NumVBuckets, params.NumNodes)
	nodes := params.Nodes()

	rand.Shuffle(len(nodes),
		func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})

	activeNumVbsMap := make(map[Node]int)

	for i, numVbs := range activeVbs {
		activeNumVbsMap[nodes[i]] = numVbs
	}

	vbmap := doGenerateVbmap(params, ri, activeNumVbsMap, prevParams)

	for vb, chain := range vbmap {
		if usedSameNodeTwice(chain) {
			diag.Printf("vb - %d, chain - %v", vb, chain)
			panic("Same node used twice.")
		}
	}

	return vbmap
}

func doSimpleActivePlacements(
	activeNumVbsMap map[Node]int, ri RIMap) []ActivePlacement {

	aps := []ActivePlacement{}

	vb := 0

	for active, numVbs := range activeNumVbsMap {
		for i := 0; i < numVbs; i++ {
			ap := makeActivePlacement(vb, active, ri)
			aps = append(aps, ap)
			vb++
		}
	}
	return aps
}

func makeActivePlacement(vb int, active Node, ri RIMap) ActivePlacement {
	ap := ActivePlacement{vb, active, make(map[Node]int)}
	ap.slavesMap.Copy(ri[active])
	return ap
}

func getActivePlacements(ri RIMap, activeNumVbsMap map[Node]int,
	prevParams *prevVbmapParams, params *VbmapParams) []ActivePlacement {

	aps := []ActivePlacement{}

	prevActiveVbsMap := prevParams.activeVbsMap

	vbsDone := make(map[int]bool)

	if prevActiveVbsMap != nil {
		for active, numVbs := range activeNumVbsMap {
			if prevVbs, ok := prevActiveVbsMap[active]; ok {
				var vbs []int
				if numVbs >= len(prevVbs) {
					// pick all the vbs used by 'active' in the previous vbmap.
					vbs = prevVbs
				} else {
					// Pick a part of the previously used vbs by 'active'.
					Shuffle(prevVbs)
					vbs = prevVbs[0:numVbs]
				}

				activeNumVbsMap[active] -= len(vbs)

				for _, vb := range vbs {
					ap := makeActivePlacement(vb, active, ri)
					aps = append(aps, ap)
					vbsDone[vb] = true
				}
			}
		}
	}

	spareVbs := []int{}

	for vb := 0; vb < params.NumVBuckets; vb++ {
		if _, ok := vbsDone[vb]; !ok {
			spareVbs = append(spareVbs, vb)
		}
	}

	// Spread the spare vbs onto all the nodes

	vbIdx := 0

	for active, numVbs := range activeNumVbsMap {
		for numVbs > 0 {
			ap := makeActivePlacement(spareVbs[vbIdx], active, ri)
			aps = append(aps, ap)
			vbIdx++
			numVbs--
		}
	}

	return aps
}

func resetRI(
	params *VbmapParams, aps []ActivePlacement, vbmap Vbmap, ri RIMap,
	numReplicaDone int) {
	for i := 0; i < params.NumVBuckets; i++ {
		ap := aps[i]
		vb := ap.vb
		ap.slavesMap.Copy(ri[ap.active])
		// Remove previously picked replicas from the RI for each
		// vbucket.
		for _, usedReplica := range vbmap[vb][1:numReplicaDone] {
			ap.slavesMap.Delete(usedReplica)
		}
	}
	return
}

func getNextActivePlacement(params *VbmapParams, prevParams *prevVbmapParams,
	replicaCosts ReplicaCosts, slaveCosts SlaveCosts,
	aps []ActivePlacement) (ActivePlacement, Node) {

	var bestReplica Node

	for idx, ap := range aps {
		replica, newReplicaPicked := getBestReplica(ap, params.Tags,
			replicaCosts, slaveCosts[ap.active], prevParams.vbmap)
		// Initialize the bestReplica with the replica found for ap[0].
		if idx == 0 {
			bestReplica = replica
		}

		if !newReplicaPicked {
			aps[0], aps[idx] = aps[idx], aps[0]
			bestReplica = replica
			break
		}
	}
	return aps[0], bestReplica
}

func updateVbmapReplicaChain(vbmap Vbmap, r int, params *VbmapParams,
	prevParams *prevVbmapParams, replicaCosts ReplicaCosts,
	slaveCosts SlaveCosts, aps []ActivePlacement) {

	tagsNodesMap := params.Tags.TagsNodesMap()

	for i := 0; i < params.NumVBuckets; i++ {
		ap, replica := getNextActivePlacement(params, prevParams, replicaCosts,
			slaveCosts, aps[i:])

		vb := ap.vb

		replicaCosts[replica] += 1
		slaveCosts[ap.active][replica] += 1
		vbmap[vb][r] = replica

		// Once a replica is picked, remove all the nodes that share the same
		// tag as the replica, to prevent selecting replicas from the same tag.
		// The idea is to basically spread the replicas initially across as
		// many tags as possible and then comeback to pick more than one
		// replica per tag if numReplicas needed is greater than numTags-1.

		rTag := params.Tags[replica]
		tagNodes := tagsNodesMap[rTag]

		for _, n := range tagNodes {
			ap.slavesMap.Delete(n)
		}
	}
	return
}

func doGenerateVbmap(
	params *VbmapParams, ri RIMap, activeNumVbsMap map[Node]int,
	prevParams *prevVbmapParams) Vbmap {

	vbmap := makeVbmap(*params)
	replicaCosts := makeReplicaCosts(params)
	slaveCosts := makeSlaveCosts(params)

	aps := getActivePlacements(ri, activeNumVbsMap, prevParams, params)

	// populate the vbmap based on the active placement chosen.
	for _, ap := range aps {
		vbmap[ap.vb][0] = ap.active
	}

	numReplicasPicked := 0
	numTags := params.Tags.TagsCount()

	for r := 1; r <= params.NumReplicas; r++ {
		rand.Shuffle(len(aps),
			func(i, j int) {
				aps[i], aps[j] = aps[j], aps[i]
			})

		if numReplicasPicked == numTags-1 {
			resetRI(params, aps, vbmap, ri, r)
			numReplicasPicked = 0
		}

		updateVbmapReplicaChain(vbmap, r, params, prevParams, replicaCosts,
			slaveCosts, aps)

		numReplicasPicked += 1
	}

	return vbmap
}

func makeR(params *VbmapParams, vbmap Vbmap) (r R) {
	nodesIdx := make(map[Node]int)

	for idx, node := range params.Nodes() {
		nodesIdx[node] = idx
	}

	m := makeMatrix(params.NumNodes, params.NumNodes)

	for _, chain := range vbmap {
		aIdx := nodesIdx[chain[0]]
		replicas := chain[1:]
		for _, r := range replicas {
			rIdx := nodesIdx[r]
			m[aIdx][rIdx] += 1
		}
	}
	return *makeRFromMatrix(*params, m)
}

func generatePrevVbmapParams(vbmap Vbmap) *prevVbmapParams {

	prev := prevVbmapParams{}

	if vbmap == nil {
		return &prev
	}

	// Build a map of vbs placed on each active node in the previous vbmap.

	// activeVbsMap (Map from node -> activeVbs present on node), is used for
	// allocating (if possible) the same set of vbuckets to the same node in the
	// new vbmap.

	activeVbsMap := make(map[Node][]int)

	nodes := []Node{}
	maxNodeId := Node(-1)

	for vb, chain := range vbmap {
		active := Node(chain[0])
		if active > maxNodeId {
			maxNodeId = active
		}
		// if active is -1 it means the vbucket is not present (data loss) and
		// we should skip it
		if active != Node(-1) {
			if _, ok := activeVbsMap[active]; !ok {
				activeVbsMap[active] = []int{vb}
				nodes = append(nodes, active)
			} else {
				activeVbsMap[active] = append(activeVbsMap[active], vb)
			}
		}
	}

	// number of nodes needs to be set to the max node in the previous map + 1
	// rather than just the count of unique nodes found in the map.
	// this is because it's possible that some of the nodes are not represented
	// in the old map. E.g. there can be 3 nodes but no vbuckets on node 0.
	numNodes := int(maxNodeId) + 1

	riMap := makeRIMap(nodes)
	R := makeMatrix(numNodes, numNodes)

	for _, chain := range vbmap {
		a := chain[0]
		if a != -1 {
			replicas := chain[1:]
			for _, r := range replicas {
				// -1 indicates there should be a configured replica, but
				// there's no actual replica; ignore when constructing the prev
				// RI and R matrices
				if r != -1 {
					riMap[a][r] = 1
					R[a][r] += 1
				}
			}
		}
	}

	ppMatrix(R, "Prev R")

	prev.NumNodes = numNodes
	prev.riMap = riMap
	prev.R = R
	prev.vbmap = vbmap
	prev.activeVbsMap = activeVbsMap

	return &prev
}

func noSolution(params *VbmapParams) bool {
	numNodes := params.NumNodes
	numReplicas := params.NumReplicas

	for _, tagNodes := range params.Tags.TagsNodesMap() {
		// The number of nodes each node in a tag can replicate to must be >=
		// the numReplicas, for a feasible solution to exist.
		if numReplicas > numNodes-len(tagNodes) {
			return true
		}
	}
	return false
}

func printMoveStats(prev Vbmap, curr Vbmap, params VbmapParams) {
	if prev == nil {
		return
	}
	activeTakeovers := 0
	replicas := 0

	for i := 0; i < params.NumVBuckets; i++ {
		if prev[i][0] != curr[i][0] {
			activeTakeovers += 1
		}
		prevMap := make(map[Node]bool)

		for _, pn := range prev[i] {
			prevMap[pn] = true
		}

		for _, cn := range curr[i] {
			if _, ok := prevMap[cn]; !ok {
				replicas += 1
			}
		}
	}

	diag.Printf("Move stats:")
	diag.Printf("    Active takeovers - %d", activeTakeovers)
	diag.Printf("    Total new replicas - %d", replicas)
}

// We shuffle the previous vbmap before we generate the new vbmap; re-order
// the new vbmap based on the old ordering of previous vbmap.

// Essentially re-assign the vbucket chains to the corresponding vbuckets as
// defined in the previous vbmap.

func reorderVbmap(
	order map[int]int, prevParams *prevVbmapParams, params *VbmapParams,
	vbmap Vbmap) Vbmap {

	if order == nil {
		return vbmap
	}

	prevVbmap := prevParams.vbmap

	newVbmap := [][]Node{}
	oldVbmap := [][]Node{}

	for i := 0; i < params.NumVBuckets; i++ {
		newVbmap = append(newVbmap, vbmap[order[i]])
		oldVbmap = append(oldVbmap, prevVbmap[order[i]])
	}

	// Reset the prev vbmap to it's original order.

	prevParams.vbmap = oldVbmap

	return newVbmap

}

// Generate a VBMap using a greedy assignment approach
func generateVbmapGreedy(params VbmapParams, prevVbmap Vbmap) (Vbmap, error) {
	if noSolution(&params) {
		return nil, ErrorNoSolution
	}

	// Shuffling the previous vbmap is a necessary step. The key idea is that
	// the previous vbmap can be extremely sorted, i.e it can look like [[0,1],
	// [0,1] ... [0,2], [0,2]], where all similar chains are consecutive.
	//
	// Since we try to match as many chains as possible in generateVbmap
	// between the newVbmap and oldVbmap, we end up getting skewed maps
	// sometimes (Around 2 times for 10 runs) if the previous vbmap is ordered.
	// Shuffling previous vbmap avoids the above skew.

	prevVbmapShuffled, shuffleOrder := shuffleVbmap(prevVbmap)

	// Initialize the structures from the previous vbucket map including:
	// * number of nodes
	// * RI matrix - the connectivity matrix indicating which
	//   nodes may be "slaves" (i.e. carry replicas for) which other
	//   nodes
	// * R matrix - number of replications each node does to every other
	prevVbmapParams := generatePrevVbmapParams(prevVbmapShuffled)

	start := time.Now()

	// Generate the RI matrx using greedy assignment of slaves
	// and attempting to minimize the difference with the previous RI
	// matrix, if provided
	riMap := generateRI(&params, prevVbmapParams.riMap)

	greedyRI := GreedyRI{riMap, params}

	diag.Printf("RI (greedy):\n%s", greedyRI.String())

	// Generate the new balanced VB map using a greedy assignment
	// approach and attempting to minimize the difference between the new
	// VB map and the previous one, if provided.
	vbmapShuffled := generateVbmap(&params, riMap, prevVbmapParams)

	// Unshuffle the vbmap based on the previous vbucket to chains mapping
	// derived from prevVbmap.

	vbmap := reorderVbmap(shuffleOrder, prevVbmapParams, &params, vbmapShuffled)

	dt := time.Since(start)

	r := makeR(&params, vbmap)
	diag.Printf("R (greedy):\n%s", r.String())
	diag.Printf("Time spent for vbmap generation (greedy) - %s", dt)

	PrintVbStats(vbmap, params, "greedy")

	printMoveStats(prevVbmapParams.vbmap, vbmap, params)

	return vbmap, nil
}
