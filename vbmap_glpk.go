package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"text/template"
)

const dataTemplate = `
data;

param nodes := {{ .NumNodes }};
param slaves := {{ .NumSlaves }};
param tags_count := {{ .Tags.TagsCount }};
param tags := {{ range $node, $tag := .Tags }}{{ $node }} {{ $tag }} {{ end }};

end;
`

type GlpkResult uint;
const (
	GLPK_NO_SOLUTION = GlpkResult(iota)
)

func (e GlpkResult) Error() string {
	switch e {
	case GLPK_NO_SOLUTION:
		return "The problem has no solution";
	default:
		panic(fmt.Sprintf("Got unknown GLPK result code: %d", e))
	}
}

func genDataFile(file io.Writer, params VbmapParams) error {
	tmpl := template.Must(template.New("data").Parse(dataTemplate))
	return tmpl.Execute(file, params)
}

func invokeGlpk(params VbmapParams) ([][]int, error) {
	file, err := ioutil.TempFile("", "vbmap_glpk_data")
	if err != nil {
		return nil, err
	}
	defer func () {
		file.Close()
		os.Remove(file.Name())
	}()

	if err := genDataFile(file, params); err != nil {
		errorMsg("Couldn't generate data file %s: %s",
			file.Name(), err.Error())
	}

	output, err := ioutil.TempFile("", "vbmap_glpk_output")
	if err != nil {
		return nil, err
	}
	output.Close()
	defer func () {
		os.Remove(output.Name())
	}()

	// TODO: model path
	// TODO: params
	cmd := exec.Command("glpsol",
		"--model", "vbmap.mod",
		"--data", file.Name(),
		"--display", output.Name())
	terminal, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	traceMsg("=======================GLPK output=======================")
	traceMsg("%s", string(terminal))
	traceMsg("=========================================================")

	return readSolution(params, output.Name())
}

func readSolution(params VbmapParams, outPath string) ([][]int, error) {
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
		result[i] = values[i * params.NumNodes : (i + 1) * params.NumNodes]
	}

	return result, nil
}
