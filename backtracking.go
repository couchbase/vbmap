package main

import (
	"fmt"
)

type BtRIGenerator struct{}

func (_ BtRIGenerator) String() string {
	return "backtracking"
}

func (_ BtRIGenerator) Generate(params VbmapParams) (RI RI, err error) {
	ctx := makeContext(params)

	if backtrack(ctx, 0, 0) {
		RI = ctx.ri
	} else {
		err = fmt.Errorf("Couldn't find a solution")
	}

	return
}

type context struct {
	ri     RI
	params VbmapParams

	rowNodesLeft []int
	colNodesLeft []int

	rowSlavesPerTag []map[Tag]int
	expSlavesPerTag int

	slotsMap [][]int
}

func makeContext(params VbmapParams) (ctx context) {
	ctx.ri = make([][]bool, params.NumNodes)
	for i, _ := range ctx.ri {
		ctx.ri[i] = make([]bool, params.NumNodes)
	}

	ctx.rowNodesLeft = duplicate(params.NumNodes, params.NumSlaves)
	ctx.colNodesLeft = duplicate(params.NumNodes, params.NumSlaves)

	ctx.rowSlavesPerTag = make([]map[Tag]int, params.NumNodes)
	ctx.slotsMap = make([][]int, params.NumNodes)

	tags := params.Tags.TagsList()

	usableTags := len(tags) - 1
	ctx.expSlavesPerTag = params.NumSlaves / usableTags
	if params.NumSlaves%usableTags != 0 {
		ctx.expSlavesPerTag += 1
	}

	for i := 0; i < params.NumNodes; i++ {
		ctx.slotsMap[i] = make([]int, params.NumNodes)

		ctx.rowSlavesPerTag[i] = make(map[Tag]int)
		for _, tag := range tags {
			if params.Tags[Node(i)] != tag {
				ctx.rowSlavesPerTag[i][tag] = 0
			}
		}

		for j := params.NumNodes - 1; j >= 0; j-- {
			var prev int
			if j == params.NumNodes-1 {
				prev = 0
			} else {
				prev = ctx.slotsMap[i][j+1]
			}

			if params.Tags[Node(i)] != params.Tags[Node(j)] {
				ctx.slotsMap[i][j] = prev + 1
			} else {
				ctx.slotsMap[i][j] = prev
			}
		}
	}

	ctx.params = params

	return
}

func backtrack(ctx context, i, j int) bool {
	if afterLast(ctx, i, j) {
		return true
	}

	ni, nj := next(ctx, i, j)

	if ctx.params.Tags[Node(i)] == ctx.params.Tags[Node(j)] {
		return backtrack(ctx, ni, nj)
	}

	values := possibleValues(ctx, i, j)
	for _, v := range values {
		mark(ctx, i, j, v)
		if backtrack(ctx, ni, nj) {
			return true
		}
	}

	rollback(ctx, i, j)
	return false
}

func possibleValues(ctx context, i, j int) (values []bool) {
	if ctx.slotsMap[i][j] > 0 && ctx.slotsMap[j][i] > 0 &&
		ctx.rowNodesLeft[i] > 0 && ctx.colNodesLeft[j] > 0 {
		values = append(values, true)
	}

	if ctx.slotsMap[i][j] > ctx.rowNodesLeft[i] &&
		ctx.slotsMap[j][i] > ctx.colNodesLeft[j] {
		values = append(values, false)
	}

	jTag := ctx.params.Tags[Node(j)]
	if ctx.rowSlavesPerTag[i][jTag] >= ctx.expSlavesPerTag {
		// Setting one in this position would go beyond preferred
		// number of slaves on tag. So we swap values to prefer zero.
		if len(values) == 2 {
			values[0], values[1] = values[1], values[0]
		}
	}

	return
}

func afterLast(ctx context, i, j int) bool {
	return i == ctx.params.NumNodes-1 &&
		j == ctx.params.NumNodes-1
}

func next(ctx context, i, j int) (ri, rj int) {
	if afterLast(ctx, i, j) {
		return i, j
	}

	ri = i
	rj = j + 1

	if rj == ctx.params.NumNodes {
		rj = 0
		ri += 1
	}

	return
}

func rollback(ctx context, i, j int) {
	mark(ctx, i, j, false)
}

func mark(ctx context, i, j int, value bool) {
	if ctx.ri[i][j] == value {
		return
	}

	var change int
	if value {
		change = 1
	} else {
		change = -1
	}

	ctx.ri[i][j] = value
	ctx.rowNodesLeft[i] -= change
	ctx.colNodesLeft[j] -= change

	jTag := params.Tags[Node(j)]
	ctx.rowSlavesPerTag[i][jTag] += change
}

func duplicate(n int, x int) (result []int) {
	result = make([]int, 0, n)

	for ; n > 0; n-- {
		result = append(result, x)
	}

	return
}
