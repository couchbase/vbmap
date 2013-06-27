package main

import (
	"fmt"
)

type DummyRIGenerator struct {}

func (_ DummyRIGenerator) String() string {
	return "dummy"
}

func (_ DummyRIGenerator) Generate(params VbmapParams) (RI RI, err error) {
	if params.Tags.TagsCount() != params.NumNodes {
		err = fmt.Errorf("Dummy RI generator is rack unaware and " +
			"doesn't support more than one node on the same tag")
		return
	}

	RI = make([][]int, params.NumNodes)
	for i := range RI {
		RI[i] = make([]int, params.NumNodes)
	}

	for i, row := range RI {
		for j := range row {
			k := (j - i + params.NumNodes - 1) % params.NumNodes
			if k < params.NumSlaves {
				RI[i][j] = 1
			}
		}
	}

	return
}
