package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
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

func invokeGlpk(params VbmapParams) ([][]int, error) {
	file, err := ioutil.TempFile("", "vbmap_glpk_data")
	if err != nil {
		return nil, err
	}
	defer func() {
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

	traceMsg("=======================GLPK output=======================")
	traceMsg("%s", string(terminal))
	traceMsg("=========================================================")

	if err != nil {
		return nil, err
	}

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
		result[i] = values[i*params.NumNodes : (i+1)*params.NumNodes]
	}

	return result, nil
}

func deepCopy(a [][]int) (b [][]int) {
	b = make([][]int, len(a))
	for i, row := range a {
		b[i] = make([]int, len(row))
		copy(b[i], row)
	}

	return
}

type RCandidate struct {
	params VbmapParams
	matrix [][]int

	rowSums          []int
	colSums          []int
	expectedColSum   int
	expectedOutliers int

	outliers      int
	rawEvaluation int
}

func abs(v int) int {
	if v < 0 {
		return -v
	} else {
		return v
	}
}

func shuffle(a []int) {
	for i := range a {
		j := i + rand.Intn(len(a)-i)
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
	if params.NumSlaves == 0 {
		return
	}

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

func makeRCandidate(params VbmapParams, RI [][]int) (result RCandidate) {
	result.params = params
	result.matrix = buildInitialR(params, RI)
	result.colSums = make([]int, params.NumNodes)
	result.rowSums = make([]int, params.NumNodes)

	for i, row := range result.matrix {
		rowSum := 0
		for j, elem := range row {
			rowSum += elem
			result.colSums[j] += elem
		}
		result.rowSums[i] = rowSum
	}

	numReplications := params.NumVBuckets * params.NumReplicas
	result.expectedColSum = numReplications / params.NumNodes
	result.expectedOutliers = numReplications % params.NumNodes

	for _, sum := range result.colSums {
		result.rawEvaluation += abs(sum - result.expectedColSum)
		if sum == result.expectedColSum+1 {
			result.outliers += 1
		}
	}

	return
}

func (cand RCandidate) computeEvaluation(rawEval int, outliers int) (eval int) {
	eval = rawEval
	if outliers > cand.expectedOutliers {
		eval -= cand.expectedOutliers
	} else {
		eval -= outliers
	}

	return
}

func (cand RCandidate) evaluation() int {
	return cand.computeEvaluation(cand.rawEvaluation, cand.outliers)
}

func (cand RCandidate) swapOutliersChange(row int, j int, k int) (change int) {
	a, b := cand.matrix[row][j], cand.matrix[row][k]
	ca := cand.colSums[j] - a + b
	cb := cand.colSums[k] - b + a

	if cand.colSums[j] == cand.expectedColSum+1 {
		change -= 1
	}
	if cand.colSums[k] == cand.expectedColSum+1 {
		change -= 1
	}
	if ca == cand.expectedColSum+1 {
		change += 1
	}
	if cb == cand.expectedColSum+1 {
		change += 1
	}

	return
}

func (cand RCandidate) swapRawEvaluationChange(row int, j int, k int) (change int) {
	a, b := cand.matrix[row][j], cand.matrix[row][k]
	ca := cand.colSums[j] - a + b
	cb := cand.colSums[k] - b + a

	evalA := abs(ca - cand.expectedColSum)
	evalB := abs(cb - cand.expectedColSum)

	oldEvalA := abs(cand.colSums[j] - cand.expectedColSum)
	oldEvalB := abs(cand.colSums[k] - cand.expectedColSum)

	change = evalA - oldEvalA + evalB - oldEvalB

	return
}

func (cand RCandidate) swapBenefit(row int, j int, k int) int {
	eval := cand.evaluation()

	swapOutliers := cand.outliers + cand.swapOutliersChange(row, j, k)
	swapRawEval := cand.rawEvaluation + cand.swapRawEvaluationChange(row, j, k)
	swapEval := cand.computeEvaluation(swapRawEval, swapOutliers)

	return swapEval - eval
}

func (cand *RCandidate) swapElems(row int, j int, k int) {
	if cand.matrix[row][j] == 0 || cand.matrix[row][k] == 0 {
		panic(fmt.Sprintf("swapping one or more zeros (%d: %d <-> %d)",
			row, j, k))
	}

	cand.rawEvaluation += cand.swapRawEvaluationChange(row, j, k)
	cand.outliers += cand.swapOutliersChange(row, j, k)

	a, b := cand.matrix[row][j], cand.matrix[row][k]

	cand.colSums[j] += b - a
	cand.colSums[k] += a - b
	cand.matrix[row][j], cand.matrix[row][k] = b, a
}

func (cand RCandidate) dump() {
	fmt.Fprintf(os.Stderr, "    |")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "%3d ", cand.params.Tags[Node(i)])
	}
	fmt.Fprintf(os.Stderr, "|\n")

	fmt.Fprintf(os.Stderr, "----|")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "----")
	}
	fmt.Fprintf(os.Stderr, "|\n")

	for i, row := range cand.matrix {
		fmt.Fprintf(os.Stderr, "%3d |", cand.params.Tags[Node(i)])
		for _, elem := range row {
			fmt.Fprintf(os.Stderr, "%3d ", elem)
		}
		fmt.Fprintf(os.Stderr, "| %d\n", cand.rowSums[i])
	}

	fmt.Fprintf(os.Stderr, "____|")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "____")
	}
	fmt.Fprintf(os.Stderr, "|\n")

	fmt.Fprintf(os.Stderr, "    |")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(os.Stderr, "%3d ", cand.colSums[i])
	}
	fmt.Fprintf(os.Stderr, "|\n")
	fmt.Fprintf(os.Stderr, "Evaluation: %d\n", cand.evaluation())
}

func (cand RCandidate) copy() (result RCandidate) {
	result.params = cand.params
	result.expectedColSum = cand.expectedColSum
	result.expectedOutliers = cand.expectedOutliers
	result.outliers = cand.outliers
	result.rawEvaluation = cand.rawEvaluation

	result.matrix = make([][]int, cand.params.NumNodes)
	for i, row := range cand.matrix {
		result.matrix[i] = make([]int, cand.params.NumNodes)
		copy(result.matrix[i], row)
	}

	result.rowSums = make([]int, cand.params.NumNodes)
	copy(result.rowSums, cand.rowSums)

	result.colSums = make([]int, cand.params.NumNodes)
	copy(result.colSums, cand.colSums)

	return
}

type TabuPair struct {
	row, j, k int
}

type TabuElem struct {
	row, col int
}

type Tabu struct {
	tabu        map[TabuPair]int
	elemIndex   map[TabuElem]TabuPair
	expireIndex map[int]TabuPair
}

func makeTabuPair(row int, j int, k int) TabuPair {
	if j > k {
		j, k = k, j
	}
	return TabuPair{row, j, k}
}

func makeTabu() Tabu {
	return Tabu{make(map[TabuPair]int),
		make(map[TabuElem]TabuPair),
		make(map[int]TabuPair)}
}

func (tabu Tabu) add(time int, row int, j int, k int) {
	oldItem, present := tabu.elemIndex[TabuElem{row, j}]
	if present {
		tabu.expire(tabu.tabu[oldItem])
	}

	oldItem, present = tabu.elemIndex[TabuElem{row, k}]
	if present {
		tabu.expire(tabu.tabu[oldItem])
	}

	item := makeTabuPair(row, j, k)
	tabu.tabu[item] = time
	tabu.expireIndex[time] = item

	tabu.elemIndex[TabuElem{row, j}] = item
	tabu.elemIndex[TabuElem{row, k}] = item
}

func (tabu Tabu) member(row int, j int, k int) bool {
	_, present := tabu.tabu[makeTabuPair(row, j, k)]
	return present
}

func (tabu Tabu) expire(time int) {
	item := tabu.expireIndex[time]
	delete(tabu.expireIndex, time)
	delete(tabu.tabu, item)
	delete(tabu.elemIndex, TabuElem{item.row, item.j})
	delete(tabu.elemIndex, TabuElem{item.row, item.k})
}

func doBuildR(params VbmapParams, RI [][]int) (best RCandidate) {
	cand := makeRCandidate(params, RI)
	best = cand.copy()

	if params.NumSlaves <= 1 || params.NumReplicas == 0 {
		// nothing to optimize here; just return
		return
	}

	attempts := 10 * params.NumNodes * params.NumNodes
	expire := 10 * params.NumNodes
	noImprovementLimit := params.NumNodes * params.NumNodes

	highElems := make([]int, params.NumNodes)
	lowElems := make([]int, params.NumNodes)

	candidateRows := make([]int, params.NumNodes)
	tabu := makeTabu()

	noCandidate := 0
	swapTabued := 0
	swapDecreased := 0
	swapIndifferent := 0
	swapIncreased := 0

	t := 0
	noImprovementIters := 0

	for t = 0; t < attempts; t++ {
		if t >= expire {
			tabu.expire(t - expire)
		}

		if noImprovementIters > noImprovementLimit {
			break
		}

		noImprovementIters++

		if best.evaluation() == 0 {
			break
		}

		highElems = []int{}
		lowElems = []int{}

		candidateRows = []int{}

		for i, elem := range cand.colSums {
			switch {
			case elem <= cand.expectedColSum:
				lowElems = append(lowElems, i)
			case elem > cand.expectedColSum:
				highElems = append(highElems, i)
			}
		}

		lowIx := lowElems[rand.Intn(len(lowElems))]
		highIx := highElems[rand.Intn(len(highElems))]

		for row := 0; row < params.NumNodes; row++ {
			lowElem := cand.matrix[row][lowIx]
			highElem := cand.matrix[row][highIx]

			if lowElem != 0 && highElem != 0 && highElem != lowElem {
				benefit := cand.swapBenefit(row, lowIx, highIx)

				if benefit >= 0 && rand.Intn(20) != 0 {
					continue
				}

				candidateRows = append(candidateRows, row)
			}
		}

		if len(candidateRows) == 0 {
			noCandidate++
			continue
		}

		row := candidateRows[rand.Intn(len(candidateRows))]

		if tabu.member(row, lowIx, highIx) {
			swapTabued++
			continue
		}

		old := cand.evaluation()

		cand.swapElems(row, lowIx, highIx)
		tabu.add(t, row, lowIx, highIx)

		if old == cand.evaluation() {
			swapIndifferent++
		} else if old < cand.evaluation() {
			swapIncreased++
		} else {
			swapDecreased++
		}

		if cand.evaluation() < best.evaluation() {
			best = cand.copy()
			noImprovementIters = 0
		}
	}

	traceMsg("Search stats")
	traceMsg("  iters -> %d", t)
	traceMsg("  no improvement termination? -> %v",
		noImprovementIters == noImprovementLimit)
	traceMsg("  noCandidate -> %d", noCandidate)
	traceMsg("  swapTabued -> %d", swapTabued)
	traceMsg("  swapDecreased -> %d", swapDecreased)
	traceMsg("  swapIndifferent -> %d", swapIndifferent)
	traceMsg("  swapIncreased -> %d", swapIncreased)
	traceMsg("")

	return
}

func buildR(params VbmapParams, RI [][]int) (best RCandidate) {
	bestEvaluation := (1 << 31) - 1

	for i := 0; i < 10; i++ {
		R := doBuildR(params, RI)
		if R.evaluation() < bestEvaluation {
			best = R
			bestEvaluation = R.evaluation()
		}

		if bestEvaluation == 0 {
			traceMsg("Found balanced map R after %d attempts", i)
			break
		}
	}

	return
}

func VbmapGenerate(params VbmapParams) (*RCandidate, error) {
	RI, err := invokeGlpk(params)
	if err != nil {
		return nil, err
	}

	R := buildR(params, RI)

	return &R, nil
}
