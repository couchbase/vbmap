package main

import (
	"fmt"
	"os"
	"flag"
	"strings"
	"strconv"
)

type Node uint;
type Tag uint;
type TagMap map[Node]Tag;

var (
	numNodes uint
	numSlaves uint
	numVBuckets uint
	numReplicas uint

	nodes []Node = nil
	tags TagMap = nil
)

func (tags *TagMap) Set(s string) error {
	*tags = make(TagMap)

	for _, pair := range strings.Split(s, ",") {
		tagNode := strings.Split(pair, ":")
		if len(tagNode) != 2 {
			return fmt.Errorf("Invalid tag-node pair: %s", pair)
		}

		node, err := strconv.ParseUint(tagNode[0], 10, strconv.IntSize)
		if err != nil {
			return err
		}

		tag, err := strconv.ParseUint(tagNode[1], 10, strconv.IntSize)
		if err != nil {
			return err
		}

		(*tags)[Node(node)] = Tag(tag)
	}
	return nil
}

func (tags TagMap) String() string {
	return fmt.Sprintf("%v", map[Node]Tag(tags))
}

func traceMsg(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format + "\n", args...)
}

func errorMsg(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format + "\n", args...)
	os.Exit(1)
}

func checkInput() {
	if numNodes == 0 || numSlaves ==0 || numVBuckets == 0 {
		errorMsg("num_nodes, num_slaves and num_vbuckets must be greater than zero")
	}

	if numSlaves >= numNodes {
		numSlaves = numNodes - 1
	}

	var n uint
	for n = 0; n < numNodes; n++ {
		nodes = append(nodes, Node(n))
	}

	if tags == nil {
		traceMsg("Tags are not specified. Assuming every not on a separate tag.")

		tags = make(TagMap)

		for i, n := range nodes {
			tags[n] = Tag(i)
		}
	}

	// each node should have a tag assigned
	for _, n := range nodes {
		_, present := tags[n]
		if !present {
			errorMsg("Tag for node %v not specified", n)
		}
	}
}

func main() {
	flag.UintVar(&numNodes, "num-nodes", 25, "Number of nodes")
	flag.UintVar(&numSlaves, "num-slaves", 10, "Number of slaves")
	flag.UintVar(&numVBuckets, "num-vbuckets", 1024, "Number of VBuckets")
	flag.UintVar(&numReplicas, "num-replicas", 1, "Number of replicas")
	flag.Var(&tags, "tags", "Tags")

	flag.Parse()

	checkInput()

	traceMsg("Finalized parameters")
	traceMsg("  Number of nodes: %d", numNodes)
	traceMsg("  Number of slaves: %d", numSlaves)
	traceMsg("  Number of vbuckets: %d", numVBuckets)
	traceMsg("  Number of replicas: %d", numReplicas)
	traceMsg("  Tags assignments:")

	for _, n := range nodes {
		traceMsg("    %v -> %v", n, tags[n])
	}
}
