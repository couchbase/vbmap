package main

import (
	"log"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

type TestingWriter struct {
	t *testing.T
}

func (w TestingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf("%s", string(p))
	return len(p), nil
}

func setup(t *testing.T) {
	diag = log.New(TestingWriter{t}, "", 0)
}

func trivialTags(nodes int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	for n := 0; n < nodes; n++ {
		tags[Node(n)] = Tag(n)
	}

	return
}

func TestRReplicaBalance(t *testing.T) {
	seed = time.Now().UTC().UnixNano()
	t.Logf("Using seed %d", seed)
	rand.Seed(seed)

	setup(t)

	for nodes := 1; nodes <= 50; nodes++ {
		tags := trivialTags(nodes)

		for replicas := 1; replicas <= 3; replicas++ {
			t.Log("=======================================")
			t.Logf("Generating R for %d node, %d replicas",
				nodes, replicas)

			params = VbmapParams{
				Tags:        tags,
				NumNodes:    nodes,
				NumSlaves:   10,
				NumVBuckets: 1024,
				NumReplicas: replicas,
			}

			normalizeParams(&params)

			gen := DummyRIGenerator{}

			RI, err := gen.Generate(params)
			if err != nil {
				t.Errorf("Couldn't generate RI: %s", err.Error())
			}

			R := buildR(params, RI)
			if R.evaluation() != 0 {
				t.Error("Generated map R has non-zero evaluation")
			}
		}
	}
}

func (_ VbmapParams) Generate(rand *rand.Rand, size int) reflect.Value {
	nodes := rand.Int()%100 + 1
	replicas := rand.Int() % 4

	params = VbmapParams{
		Tags:        trivialTags(nodes),
		NumNodes:    nodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: replicas,
	}
	normalizeParams(&params)

	return reflect.ValueOf(params)
}

func TestRIProperties(t *testing.T) {
	setup(t)

	gen := DummyRIGenerator{}

	check := func(params VbmapParams) bool {
		RI, err := gen.Generate(params)
		if err != nil {
			return false
		}

		if len(RI) != params.NumNodes {
			return false
		}

		if len(RI[0]) != params.NumNodes {
			return false
		}

		colSums := make([]int, params.NumNodes)
		rowSums := make([]int, params.NumNodes)

		for i, row := range RI {
			for j, elem := range row {
				colSums[j] += elem
				rowSums[i] += elem
			}
		}

		for i := range colSums {
			if colSums[i] != params.NumSlaves {
				return false
			}

			if rowSums[i] != params.NumSlaves {
				return false
			}
		}

		return true
	}

	if err := quick.Check(check, &quick.Config{MaxCount: 10000}); err != nil {
		t.Error(err)
	}
}

func TestRProperties(t *testing.T) {
	setup(t)

	gen := DummyRIGenerator{}
	check := func(params VbmapParams, seed int64) bool {
		rand.Seed(seed)

		RI, err := gen.Generate(params)
		if err != nil {
			return false
		}

		R := buildR(params, RI)
		if params.NumReplicas == 0 {
			// no replicas? R should obviously be empty
			for _, row := range R.matrix {
				for _, elem := range row {
					if elem != 0 {
						return false
					}
				}
			}
		} else {
			// check that we follow RI topology
			for i, row := range RI {
				for j, elem := range row {
					if elem == 0 && R.matrix[i][j] != 0 ||
						elem != 0 && R.matrix[i][j] == 0 {
						return false
					}
				}
			}

			totalVBuckets := 0

			// check active vbuckets balance
			for _, sum := range R.rowSums {
				if sum%params.NumReplicas != 0 {
					return false
				}

				vbuckets := sum / params.NumReplicas
				expected := params.NumVBuckets / params.NumNodes

				if vbuckets != expected && vbuckets != expected+1 {
					return false
				}

				totalVBuckets += vbuckets
			}

			if totalVBuckets != params.NumVBuckets {
				return false
			}
		}

		return true
	}

	if err := quick.Check(check, &quick.Config{MaxCount: 1000}); err != nil {
		t.Error(err)
	}
}
