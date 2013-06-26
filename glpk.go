package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"text/template"
	"log"
)

type GlpkRIGenerator struct {}

const dataTemplate = `
data;

param nodes := {{ .NumNodes }};
param slaves := {{ .NumSlaves }};
param tags_count := {{ .Tags.TagsCount }};
param tags := {{ range $node, $tag := .Tags }}{{ $node }} {{ $tag }} {{ end }};

end;
`

type GlpkResult uint

const (
	GLPK_NO_SOLUTION = GlpkResult(iota)
)

func (e GlpkResult) Error() string {
	switch e {
	case GLPK_NO_SOLUTION:
		return "The problem has no solution"
	default:
		panic(fmt.Sprintf("Got unknown GLPK result code: %d", e))
	}
}

func genDataFile(file io.Writer, params VbmapParams) error {
	tmpl := template.Must(template.New("data").Parse(dataTemplate))
	return tmpl.Execute(file, params)
}

func readSolution(params VbmapParams, outPath string) (RI, error) {
	output, err := os.Open(outPath)
	if err != nil {
		return nil, err
	}
	defer output.Close()

	var values []int = make([]int, params.NumNodes*params.NumNodes)

	for i := range values {
		_, err := fmt.Fscan(output, &values[i])
		if err == io.EOF && i == 0 {
			return nil, GLPK_NO_SOLUTION
		}

		if err != nil {
			return nil, fmt.Errorf("Invalid GLPK output (%s)", err.Error())
		}
	}

	result := make([][]int, params.NumNodes)
	for i := range result {
		result[i] = values[i*params.NumNodes : (i+1)*params.NumNodes]
	}

	return result, nil
}

func (_ GlpkRIGenerator) Generate(params VbmapParams) (RI, error) {
	file, err := ioutil.TempFile("", "vbmap_glpk_data")
	if err != nil {
		return nil, err
	}
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	if err := genDataFile(file, params); err != nil {
		return nil, fmt.Errorf("Couldn't generate data file %s: %s",
			file.Name(), err.Error())
	}

	output, err := ioutil.TempFile("", "vbmap_glpk_output")
	if err != nil {
		return nil, err
	}
	output.Close()
	defer func() {
		os.Remove(output.Name())
	}()

	// TODO: model path
	// TODO: params
	cmd := exec.Command("glpsol",
		"--model", "vbmap.mod",
		"--tmlim", "10",
		"--data", file.Name(),
		"--display", output.Name(),
		"--seed", fmt.Sprint(rand.Int31()),
		"--mipgap", "0.5")

	terminal, err := cmd.CombinedOutput()

	log.Printf("=======================GLPK output=======================")
	log.Printf("%s", string(terminal))
	log.Printf("=========================================================")

	if err != nil {
		return nil, err
	}

	return readSolution(params, output.Name())
}
