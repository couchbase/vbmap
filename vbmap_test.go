package main

import (
	"log"
	"math/rand"
	"testing"
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
	seed = time.Now().UTC().UnixNano()
	t.Logf("Using seed %d", seed)
	rand.Seed(seed)

	diag = log.New(TestingWriter{t}, "", 0)
}

func TestExhaustive(t *testing.T) {
	setup(t)

	for nodes := 1; nodes <= 50; nodes++ {
		tags := make(map[Node]Tag)
		for n := 0; n < nodes; n++ {
			tags[Node(n)] = Tag(n)
		}

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
