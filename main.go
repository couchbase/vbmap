// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

type tagHistoValue []uint
type engineValue struct {
	generator RIGenerator
}
type outputFormatValue string

const (
	noSolutionExitCode   = 1
	generalErrorExitCode = 2
)

var availableGenerators = []RIGenerator{
	makeMaxFlowRIGenerator(),
	makeGlpkRIGenerator(),
}

var diag *log.Logger

var (
	seed         int64
	tagHisto     tagHistoValue
	params       = VbmapParams{Tags: nil}
	engine       = engineValue{availableGenerators[0]}
	engineParams string
	searchParams SearchParams
	relaxAll     bool

	outputFormat outputFormatValue = "text"
	diagTo       string
	profTo       string

	currentMapPath string
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

func (hist *tagHistoValue) Set(s string) error {
	values := strings.Split(s, ",")
	*hist = make(tagHistoValue, len(values))

	for i, v := range values {
		count, err := strconv.ParseUint(v, 10, strconv.IntSize)
		if err != nil {
			return err
		}

		(*hist)[i] = uint(count)
	}

	return nil
}

func (hist tagHistoValue) String() string {
	return fmt.Sprintf("%v", []uint(hist))
}

func (engine *engineValue) Set(s string) error {
	for _, gen := range availableGenerators {
		if s == gen.String() {
			*engine = engineValue{gen}
			return nil
		}
	}

	return fmt.Errorf("unknown engine")
}

func (engine engineValue) String() string {
	if engine.generator != nil {
		return engine.generator.String()
	}

	return "no-engine"
}

func (format *outputFormatValue) Set(s string) error {
	switch s {
	case "text", "json", "ext-json":
		*format = outputFormatValue(s)
	default:
		return fmt.Errorf("unrecognized output format")
	}

	return nil
}

func (format outputFormatValue) String() string {
	return string(format)
}

func normalizeParams(params *VbmapParams) {
	if params.NumReplicas+1 > params.NumNodes {
		params.NumReplicas = params.NumNodes - 1
	}

	if params.NumSlaves >= params.NumNodes {
		params.NumSlaves = params.NumNodes - 1
	}

	if params.NumSlaves < params.NumReplicas {
		params.NumReplicas = params.NumSlaves
	}

	numSlaves := params.NumSlaves
	for _, tagNodes := range params.Tags.TagsNodesMap() {
		tagMaxSlaves := params.NumNodes - len(tagNodes)
		if tagMaxSlaves < numSlaves {
			numSlaves = tagMaxSlaves
		}
	}

	if numSlaves >= params.NumReplicas {
		// prefer having replicas over tag awareness
		params.NumSlaves = numSlaves
	}

	if params.NumReplicas == 0 {
		params.NumSlaves = 0
	}
}

func checkInput() {
	if params.NumNodes <= 0 || params.NumSlaves <= 0 || params.NumVBuckets <= 0 {
		fatal("num-nodes, num-slaves and num-vbuckets must be greater than zero")
	}

	if params.NumReplicas < 0 {
		fatal("num-replicas must be greater of equal than zero")
	}

	if params.Tags != nil && tagHisto != nil {
		fatal("Options --tags and --tag-histogram are exclusive")
	}

	if params.Tags == nil && tagHisto == nil {
		diag.Printf("Tags are not specified. Assuming every node on a separate tag.")
		tagHisto = make(tagHistoValue, params.NumNodes)

		for i := 0; i < params.NumNodes; i++ {
			tagHisto[i] = 1
		}
	}

	nodes := params.Nodes()

	if tagHisto != nil {
		tag := 0
		params.Tags = make(TagMap)

		for _, node := range nodes {
			for tag < len(tagHisto) && tagHisto[tag] == 0 {
				tag++
			}
			if tag >= len(tagHisto) {
				fatal("Invalid tag histogram. Counts do not add up.")
			}

			tagHisto[tag]--
			params.Tags[node] = Tag(tag)
		}

		if tag != len(tagHisto)-1 || tagHisto[tag] != 0 {
			fatal("Invalid tag histogram. Counts do not add up.")
		}
	}

	// each node should have a tag assigned
	for _, node := range nodes {
		_, present := params.Tags[node]
		if !present {
			fatal("Tag for node %v not specified", node)
		}
	}

	normalizeParams(&params)
}

func fatalExitCode(code int, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(code)
}

func fatal(format string, args ...interface{}) {
	fatalExitCode(generalErrorExitCode, format, args...)
}

func parseEngineParams(str string) (params map[string]string) {
	params = make(map[string]string)

	for _, kv := range strings.Split(str, ",") {
		var k, v string

		switch split := strings.SplitN(kv, "=", 2); len(split) {
		case 1:
			k = split[0]
		case 2:
			k = split[0]
			v = split[1]
		default:
			fatal("should not happen")
		}

		if k != "" {
			params[k] = v
		}
	}

	return
}

func readVbmap(path string) (vbmap Vbmap, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&vbmap)

	if err == nil {
		diag.Printf("Succesfully read current vbucket map:\n[")
		n := len(vbmap)
		for i, row := range vbmap {
			sep := ","
			if i == n-1 {
				sep = ""
			}

			diag.Printf("  %v%s", row, sep)
		}
		diag.Printf("]")
	}

	return
}

func main() {
	flag.IntVar(&params.NumNodes, "num-nodes", 25, "number of nodes")
	flag.IntVar(&params.NumSlaves, "num-slaves", 10, "number of slaves")
	flag.IntVar(&params.NumVBuckets, "num-vbuckets", 1024, "number of VBuckets")
	flag.IntVar(&params.NumReplicas, "num-replicas", 1, "number of replicas")
	flag.Var(&params.Tags, "tags", "tags")
	flag.Var(&tagHisto, "tag-histogram", "tag histogram")
	flag.Var(&engine, "engine", "engine used to generate the topology")
	flag.StringVar(&engineParams, "engine-params", "", "engine specific params")
	flag.Var(&outputFormat, "output-format", "output format")
	flag.StringVar(&diagTo, "diag", "", "where to send diagnostics")
	flag.StringVar(&profTo, "cpuprofile", "", "write cpuprofile to path")

	flag.IntVar(&searchParams.NumRRetries, "num-r-retries", 25,
		"number of attempts to generate matrix R (for each RI attempt)")
	flag.BoolVar(&searchParams.StrictReplicaBalance,
		"strict-replica-balance", false,
		"require that all nodes have "+
			"the same number of replica vbuckets (±1)")
	flag.BoolVar(&searchParams.RelaxNumSlaves,
		"relax-num-slaves", false,
		"allow relaxing number of slaves")
	flag.BoolVar(&relaxAll, "relax-all", false, "relax all constraints")
	flag.StringVar(&searchParams.DotPath,
		"dot", "", "output the flow graph for matrix R to path")

	flag.StringVar(&currentMapPath, "current-map", "",
		"a path to current vbucket map")

	flag.Int64Var(&seed, "seed", time.Now().UTC().UnixNano(), "random seed")

	flag.Parse()

	var diagSink io.Writer
	if diagTo == "" {
		diagSink = os.Stderr
	} else {
		diagFile, err := os.Create(diagTo)
		if err != nil {
			fatal("Couldn't create diagnostics file %s: %s",
				diagTo, err.Error())
		}
		defer diagFile.Close()

		diagSink = diagFile
	}

	if profTo != "" {
		profFile, err := os.Create(profTo)
		if err != nil {
			fatal("Couldn't create profile file %s: %s",
				profTo, err.Error())
		}
		defer profFile.Close()

		pprof.StartCPUProfile(profFile)
		defer pprof.StopCPUProfile()
	}

	diag = log.New(diagSink, "", 0)
	diag.Printf("Started as:\n  %s", strings.Join(os.Args, " "))

	diag.Printf("Using %d as a seed", seed)
	rand.Seed(seed)

	checkInput()

	engineParamsMap := parseEngineParams(engineParams)
	if err := engine.generator.SetParams(engineParamsMap); err != nil {
		fatal("Couldn't set engine params: %s", err.Error())
	}

	diag.Printf("Finalized parameters")
	diag.Printf("  Number of nodes: %d", params.NumNodes)
	diag.Printf("  Number of slaves: %d", params.NumSlaves)
	diag.Printf("  Number of vbuckets: %d", params.NumVBuckets)
	diag.Printf("  Number of replicas: %d", params.NumReplicas)
	diag.Printf("  Tags assignments:")

	for _, node := range params.Nodes() {
		diag.Printf("    %v -> %v", node, params.Tags[node])
	}

	if relaxAll {
		searchParams.StrictReplicaBalance = false
		searchParams.RelaxNumSlaves = true
	}

	currentMap := Vbmap(nil)
	if currentMapPath != "" {
		var err error
		currentMap, err = readVbmap(currentMapPath)
		if err != nil {
			fatal("Could not read current vbucket map: %s", err.Error())
		}
	}

	solution, err :=
		VbmapGenerate(params, engine.generator, searchParams, currentMap)
	if err != nil {
		switch err {
		case ErrorNoSolution:
			fatalExitCode(noSolutionExitCode, "%s", err.Error())
		default:
			fatal("%s", err.Error())
		}
	}

	switch outputFormat {
	case "text":
		fmt.Print(solution.String())
	case "json":
		json, err := json.Marshal(solution)
		if err != nil {
			fatal("Couldn't encode the solution: %s", err.Error())
		}
		fmt.Print(string(json))
	case "ext-json":
		extJSONMap := make(map[string]interface{})
		extJSONMap["numNodes"] = params.NumNodes
		extJSONMap["numSlaves"] = params.NumSlaves
		extJSONMap["numVBuckets"] = params.NumVBuckets
		extJSONMap["numReplicas"] = params.NumReplicas
		extJSONMap["map"] = solution

		tags := make([]Tag, params.NumNodes)
		extJSONMap["tags"] = tags

		for i, t := range params.Tags {
			tags[i] = t
		}

		json, err := json.Marshal(extJSONMap)
		if err != nil {
			fatal("Couldn't encode the solution: %s", err.Error())
		}
		fmt.Print(string(json))
	default:
		panic("should not happen")
	}
}
