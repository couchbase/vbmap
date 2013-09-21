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
	ri      RI
	params  VbmapParams
	colSums []int
	rowSums []int
}

func makeContext(params VbmapParams) (ctx context) {
	ctx.ri = make([][]bool, params.NumNodes)
	for i, _ := range ctx.ri {
		ctx.ri[i] = make([]bool, params.NumNodes)
	}

	ctx.colSums = make([]int, params.NumNodes)
	ctx.rowSums = make([]int, params.NumNodes)

	ctx.params = params

	return
}

func backtrack(ctx context, i, j int) bool {
	return false
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
	ctx.rowSums[i] += change
	ctx.colSums[j] += change
}
