package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"text/template"
	"math/rand"
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
		"--tmlim", "5",
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

func shuffle(a []int) {
	for i := range a {
		j := i + rand.Intn(len(a) - i)
		a[i], a[j] = a[j], a[i]
	}
}

func spreadSum(sum int, n int) (result []int) {
	result = make([]int, n)

	quot := sum / n
	rem := sum % n

	for i := range result {
		result[i] = quot
		if rem != 0 {
			rem -= 1
			result[i] += 1
		}
	}

	shuffle(result)

	return
}

func buildInitialR(params VbmapParams, RI [][]int) (R [][]int) {
	activeVbsPerNode := spreadSum(params.NumVBuckets, params.NumNodes)

	R = make([][]int, len(RI))
	for i, row := range RI {
		rowSum := activeVbsPerNode[i] * params.NumReplicas
		slaveVbs := spreadSum(rowSum, params.NumSlaves)

		R[i] = make([]int, len(row))

		slave := 0
		for j, elem := range row {
			if elem != 0 {
				R[i][j] = slaveVbs[slave]
				slave += 1
			}
		}
	}

	return
}

func dumpR(params VbmapParams, R [][]int) {
	fmt.Fprintf(os.Stderr, "    |")
	for i := 0; i < params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "%3d ", params.Tags[Node(i)])
	}
	fmt.Fprintf(os.Stderr, "|\n")

	fmt.Fprintf(os.Stderr, "----|")
	for i := 0; i < params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "----")
	}
	fmt.Fprintf(os.Stderr, "|\n")

	colSums := make([]int, params.NumNodes)
	for i, row := range R {
		rowSum := 0

		fmt.Fprintf(os.Stderr, "%3d |", params.Tags[Node(i)])
		for j, elem := range row {
			rowSum += elem
			colSums[j] += elem
			fmt.Fprintf(os.Stderr, "%3d ", elem)
		}
		fmt.Fprintf(os.Stderr, "| %d\n", rowSum)
	}

	fmt.Fprintf(os.Stderr, "____|")
	for i := 0; i < params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "____")
	}
	fmt.Fprintf(os.Stderr, "|\n")

	fmt.Fprintf(os.Stderr, "    |")
	for i := 0; i < params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "%3d ", colSums[i])
	}
	fmt.Fprintf(os.Stderr, "|\n")
}

func VbmapGenerate(params VbmapParams) ([][]int, error) {
	RI, err := invokeGlpk(params)
	if err != nil {
		return nil, err
	}

	return buildInitialR(params, RI), nil
}
