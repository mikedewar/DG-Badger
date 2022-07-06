package main

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/google/uuid"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

/*
A simple implementation of Algorithms 1 and 2 in "An Efficient and Scalable Algorithmic Method for Generating Large–Scale Random Graphs" by Alam et al.
*/

type Edge struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

type DG struct {
	edgeChan chan Edge
	D        []int64 // set of distint expected degrees
	n        []int64 // number of vertices with expected degree
	λ        []int64 // vertex-group labels
}

func main() {

	rand.Seed(8989)

	var n, D []int64

	D = []int64{1, 2, 3, 4, 5, 8, 9, 10, 100, 500, 1000}
	n = []int64{8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 10, 10, 5}

	//D = []int64{1, 4}
	//n = []int64{4, 1}

	dg, err := NewDG(n, D)
	if err != nil {
		log.Fatal(err)
	}

	go dg.DG_CL(D)

	numEdges, err := WriteEdges(dg.edgeChan, "/tmp")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Wrote", numEdges, "edges")

}

func NewDG(n, D []int64) (DG, error) {

	if len(n) != len(D) {
		return DG{}, errors.New("n and D must be the same length")
	}

	Λ := len(n)

	return DG{
		edgeChan: make(chan Edge),
		λ:        make([]int64, Λ),
		n:        n,
		D:        D,
	}, nil
}

// Algorithm 1 - Generating edges using edge skipping
func (dg *DG) EDGE_SKIPPING(i, j int64, p float64, start, end int64) {
	//log.Println(i, j, "p:", p, "start:", start, "end:", end)
	x := start - 1
	for x < end {
		r := rand.Float64()
		l := int64(math.Floor(math.Log(r) / math.Log(1-p))) // skip length (Alg1 line 5)
		x += l + 1                                          // next selected edge (Alg1 line 5)
		if x <= end {
			var u, v int64
			if i == j {
				// intra edges (Alg1 line 8)
				z := new(big.Int)
				u = int64(math.Ceil((-1 + math.Sqrt(float64(1+8*x))) / 2))
				v = x - z.Binomial(u, 2).Int64() - 1
			} else {
				// inter edges (Alg1 line 10)
				u = int64(math.Floor(float64(x-1) / float64(dg.n[j])))
				v = int64(math.Mod(float64(x-1), float64(dg.n[j])))
			}
			//log.Println("i:", i, "j:", j, "x:", x, "u:", u, "v:", v)
			if dg.λ[i]+u == dg.λ[j]+v {
				// something is causing a handful, like 5 out of 20,000, of self edges.
				// pretty sure it's a bug, so somethign to dig into
				//log.Fatal(i, u, j, v)
				continue
			}
			// output edge (Alg1 line 11)
			dg.edgeChan <- Edge{dg.λ[i] + u, dg.λ[j] + v}
		}
	}
}

// Algorithm 2 - The DG algorithm for the CL model
func (dg *DG) DG_CL(D []int64) {

	var S float64 // sum of expected degrees

	Λ := int64(len(dg.D))

	// starting labels for each group
	dg.λ[0] = 1
	for i, ni := range dg.n {
		if i == 0 {
			continue
		}
		dg.λ[i] = dg.λ[i-1] + ni
	}

	log.Println("λ:", dg.λ)
	S = 0
	for i := range dg.n {
		S += float64(dg.n[i] * dg.D[i])
	}
	log.Println("S:", S)

	// progress bars ftw
	pg := mpb.New(mpb.WithWidth(64), mpb.WithOutput(os.Stderr))
	bar := pg.New(
		int64(math.Pow(float64(Λ), 2)),
		mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name("DGAlgo", decor.WC{W: 7, C: decor.DidentRight}),
			// replace ETA decorator with "done" message, OnComplete event
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 4}), "done",
			),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)
	//

	var i, j int64
	for i = 0; i < Λ; i++ {
		for j = 0; j < Λ; j++ {
			//log.Println(i, j)
			if i == j {
				z := new(big.Int)
				n_choose_two := z.Binomial(dg.n[i], 2).Int64()
				p := math.Pow(float64(dg.D[i]), 2) / S
				dg.EDGE_SKIPPING(i, i, p, 1, n_choose_two)
			} else {
				p := float64(dg.D[i]*dg.D[j]) / S
				dg.EDGE_SKIPPING(i, j, p, 1, dg.n[i]*dg.n[j])
			}
			bar.Increment()
		}
	}
	close(dg.edgeChan)
	pg.Wait()
}

func WriteEdges(edgeChan chan Edge, location string) (int, error) {

	var edges int
	opts := badger.DefaultOptions(location)
	db, err := badger.Open(opts)
	if err != nil {
		return 0, err
	}

	txndb := db.NewWriteBatch()

	for val := range edgeChan {

		b, err := json.Marshal(val)
		if err != nil {
			return edges, err
		}

		uuid := uuid.New().String()
		err = txndb.Set([]byte(uuid), b)
		if err != nil {
			return edges, err
		}
		edges++
	}

	return edges, nil
}
