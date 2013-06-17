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

type VbmapParams struct {
	Tags TagMap

	NumNodes int
	NumSlaves int
	NumVBuckets int
	NumReplicas int
}

var (
	params VbmapParams = VbmapParams{
		Tags : nil,
	}
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
	if params.NumNodes <= 0 || params.NumSlaves <= 0 || params.NumVBuckets <= 0 {
		errorMsg("num-nodes, num-slaves and num-vbuckets must be greater than zero")
	}

	if params.NumReplicas < 0 {
		errorMsg("num-replicas must be greater of equal than zero")
	}

	if params.NumSlaves >= params.NumNodes {
		params.NumSlaves = params.NumNodes - 1
	}

	if params.Tags == nil {
		traceMsg("Tags are not specified. Assuming every not on a separate tag.")

		params.Tags = make(TagMap)

		for i := 0; i < params.NumNodes; i++ {
			params.Tags[Node(i)] = Tag(i)
		}
	}

	// each node should have a tag assigned
	for i := 0; i < params.NumNodes; i++ {
		_, present := params.Tags[Node(i)]
		if !present {
			errorMsg("Tag for node %v not specified", i)
		}
	}
}

func main() {
	flag.IntVar(&params.NumNodes, "num-nodes", 25, "Number of nodes")
	flag.IntVar(&params.NumSlaves, "num-slaves", 10, "Number of slaves")
	flag.IntVar(&params.NumVBuckets, "num-vbuckets", 1024, "Number of VBuckets")
	flag.IntVar(&params.NumReplicas, "num-replicas", 1, "Number of replicas")
	flag.Var(&params.Tags, "tags", "Tags")

	flag.Parse()

	checkInput()

	traceMsg("Finalized parameters")
	traceMsg("  Number of nodes: %d", params.NumNodes)
	traceMsg("  Number of slaves: %d", params.NumSlaves)
	traceMsg("  Number of vbuckets: %d", params.NumVBuckets)
	traceMsg("  Number of replicas: %d", params.NumReplicas)
	traceMsg("  Tags assignments:")

	for i := 0; i < params.NumNodes; i++ {
		traceMsg("    %d -> %v", i, params.Tags[Node(i)])
	}
}
