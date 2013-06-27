package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
	"log"
	"encoding/json"
)

type TagHist []uint
type Engine struct {
	generator RIGenerator
}
type OutputFormat string

var availableGenerators []RIGenerator = []RIGenerator{
	GlpkRIGenerator{},
	DummyRIGenerator{},
}

var (
	seed         int64
	tagHistogram TagHist     = nil
	params       VbmapParams = VbmapParams{
		Tags: nil,
	}
	engine       Engine = Engine{availableGenerators[0]}
	outputFormat OutputFormat = "text"
)

func (tags *TagMap) Set(s string) error {
	*tags = make(TagMap)

	for _, pair := range strings.Split(s, ",") {
		tagNode := strings.Split(pair, ":")
		if len(tagNode) != 2 {
			return fmt.Errorf("invalid tag-node pair (%s)", pair)
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

func (hist *TagHist) Set(s string) error {
	values := strings.Split(s, ",")
	*hist = make(TagHist, len(values))

	for i, v := range values {
		count, err := strconv.ParseUint(v, 10, strconv.IntSize)
		if err != nil {
			return err
		}

		(*hist)[i] = uint(count)
	}

	return nil
}

func (hist TagHist) String() string {
	return fmt.Sprintf("%v", []uint(hist))
}

func (engine *Engine) Set(s string) error {
	for _, gen := range availableGenerators {
		if s == gen.String() {
			*engine = Engine{gen}
			return nil
		}
	}

	return fmt.Errorf("unknown engine")
}

func (engine Engine) String() string {
	return engine.generator.String()
}

func (format *OutputFormat) Set(s string) error {
	switch s {
	case "text", "json":
		*format = OutputFormat(s)
	default:
		return fmt.Errorf("unrecognized output format")
	}

	return nil
}

func (format OutputFormat) String() string {
	return string(format)
}

func checkInput() {
	if params.NumNodes <= 0 || params.NumSlaves <= 0 || params.NumVBuckets <= 0 {
		log.Fatalf("num-nodes, num-slaves and num-vbuckets must be greater than zero")
	}

	if params.NumReplicas < 0 {
		log.Fatalf("num-replicas must be greater of equal than zero")
	}

	if params.NumReplicas+1 > params.NumNodes {
		params.NumReplicas = params.NumNodes - 1
	}

	if params.NumSlaves >= params.NumNodes {
		params.NumSlaves = params.NumNodes - 1
	}

	if params.NumSlaves < params.NumReplicas {
		params.NumReplicas = params.NumSlaves
	}

	if params.Tags != nil && tagHistogram != nil {
		log.Fatalf("Options --tags and --tag-histogram are exclusive")
	}

	if params.Tags == nil && tagHistogram == nil {
		log.Printf("Tags are not specified. Assuming every node on a separate tag.")
		tagHistogram = make(TagHist, params.NumNodes)

		for i := 0; i < params.NumNodes; i++ {
			tagHistogram[i] = 1
		}
	}

	if tagHistogram != nil {
		tag := 0
		params.Tags = make(TagMap)

		for i := 0; i < params.NumNodes; i++ {
			for tag < len(tagHistogram) && tagHistogram[tag] == 0 {
				tag += 1
			}
			if tag >= len(tagHistogram) {
				log.Fatalf("Invalid tag histogram. Counts do not add up.")
			}

			tagHistogram[tag] -= 1
			params.Tags[Node(i)] = Tag(tag)
		}

		if tag != len(tagHistogram)-1 || tagHistogram[tag] != 0 {
			log.Fatalf("Invalid tag histogram. Counts do not add up.")
		}
	}

	// each node should have a tag assigned
	for i := 0; i < params.NumNodes; i++ {
		_, present := params.Tags[Node(i)]
		if !present {
			log.Fatalf("Tag for node %v not specified", i)
		}
	}
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)

	log.Printf("Started as:\n  %s", strings.Join(os.Args, " "))

	// TODO
	flag.IntVar(&params.NumNodes, "num-nodes", 25, "number of nodes")
	flag.IntVar(&params.NumSlaves, "num-slaves", 10, "number of slaves")
	flag.IntVar(&params.NumVBuckets, "num-vbuckets", 1024, "number of VBuckets")
	flag.IntVar(&params.NumReplicas, "num-replicas", 1, "number of replicas")
	flag.Var(&params.Tags, "tags", "tags")
	flag.Var(&tagHistogram, "tag-histogram", "tag histogram")
	flag.Var(&engine, "engine", "engine used to generate the topology")
	flag.Var(&outputFormat, "output-format", "output format")

	flag.Int64Var(&seed, "seed", time.Now().UTC().UnixNano(), "random seed")

	flag.Parse()

	rand.Seed(seed)

	checkInput()

	log.Printf("Finalized parameters")
	log.Printf("  Number of nodes: %d", params.NumNodes)
	log.Printf("  Number of slaves: %d", params.NumSlaves)
	log.Printf("  Number of vbuckets: %d", params.NumVBuckets)
	log.Printf("  Number of replicas: %d", params.NumReplicas)
	log.Printf("  Tags assignments:")

	for i := 0; i < params.NumNodes; i++ {
		log.Printf("    %d -> %v", i, params.Tags[Node(i)])
	}

	solution, err := VbmapGenerate(params, engine.generator)
	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	switch outputFormat {
	case "text":
		fmt.Print(solution.String())
	case "json":
		json, err := json.Marshal(solution)
		if err != nil {
			log.Fatalf("Couldn't encode the solution: %s", err.Error())
		}
		fmt.Print(string(json))
	default:
		panic("should not happen")
	}
}
