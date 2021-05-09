package main

/*

Source: https://github.com/eliangcs/pystock-data

symbol,end_date,amend,period_focus,fiscal_year,doc_type,revenues,op_income,net_income,eps_basic,eps_diluted,dividend,assets,cur_assets,cur_liab,cash,equity,cash_flow_op,cash_flow_inv,cash_flow_fin

GOOG,2009-06-30,False,Q2,2009,10-Q,5522897000.0,1873894000.0,1484545000.0,4.7,4.66,0.0,35158760000.0,23834853000.0,2000962000.0,11911351000.0,31594856000.0,3858684000.0,-635974000.0,46354000.0

*/

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var ddl = `
CREATE TABLE fund_activity
(
  id VARCHAR(36) PRIMARY KEY
  , ticker VARCHAR(5)
  , end_date DATE
  , amend BOOL
  , period_focus VARCHAR(2)
  , fiscal_year INT
  , doc_type VARCHAR(8)
  , revenues DECIMAL
  , op_income DECIMAL
  , net_income DECIMAL
  , eps_basic DECIMAL
  , eps_diluted DECIMAL
  , dividend DECIMAL
  , assets DECIMAL
  , cur_assets DECIMAL
  , cur_liab DECIMAL
  , cash DECIMAL
  , equity DECIMAL
  , cash_flow_op DECIMAL
  , cash_flow_inv DECIMAL
  , cash_flow_fin DECIMAL
);
`

func randUuid() string {
	u, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	var rv string
	if rv = u.String(); rv == "" {
		panic("Unable to generate UUID")
	}
	return rv
}

func randBool() bool {
	return rand.Intn(2) == 0
}

func randQ() string {
	return fmt.Sprintf("Q%d", rand.Intn(4)+1)
}

func randAmt(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

func randDate() time.Time {
	min := time.Date(2010, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2021, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// yyyyMmDd := fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day())

// perl -e '@s = (); while (<>) { if (rand() < 0.05) { @a = split /\t/; push(@s, "\"$a[0]\""); } } print join(", ", @s) . "\n";' < symbols.txt
var tickers = [280]string{"ISL", "BVX", "CQH", "CVR", "VFL", "EIA", "MAB", "EIM", "FRD", "GST", "MXC", "MZA", "NHC", "NHS", "NRO", "PW", "PLX", "UTG", "RVP", "SKY", "TGC", "VHC", "TWOU", "ACIU", "ACST", "ACCP", "AEHR", "AEZS", "ALDX", "AMRK", "ANAT", "ABCB", "ALOG", "AGIIL", "ATOM", "ATRC", "EARS", "BOSC", "BIDU", "BWINB", "BANF", "BOTJ", "BLPH", "BDSI", "BLVDU", "BRID", "CAPR", "CASC", "CDEV", "CHFN", "IMOS", "CHCO", "CLRO", "CGNX", "COUP", "CRTO", "CRIS", "CYTXW", "DFRG", "TACOW", "DHXM", "EQFN", "ESSA", "EYEG", "FB", "DAVE", "LION", "FNLC", "FMBH", "FDTS", "FHK", "MDIV", "FVE", "FLDM", "FSBC", "FTEK", "FLGT", "GLMD", "GFN", "GEOS", "GLAD", "QQQC", "GUID", "HAFC", "HRMN", "HTLF", "QYLD", "HBANP", "HYGS", "IBKCP", "IDXX", "INSY", "XENT", "ITIC", "DTYL", "IRIX", "IVFGC", "KTWO", "KALU", "KELYB", "KTEC", "KINS", "KOPN", "KTOS", "LANC", "LTRX", "LUNA", "MFIN", "NGHCP", "NEOS", "NURO", "NEWS", "NXEOU", "NVAX", "NVGN", "NTRI", "OVLY", "OPTT", "OTIV", "OTEL", "WUBA", "ATEN", "ACN", "AGC", "OXLCO", "FRSH", "PKBK", "A", "AFB", "AWH", "PENN", "PFIS", "PBCT", "PNTR", "POLA", "PTF", "ANTX", "AIF", "ARCX", "AHP", "QSII", "BXS", "BHP", "BIO", "RICK", "RGLS", "BGR", "BBF", "RTIX", "SABR", "BSL", "BWP", "BPI", "SAL", "GCVRZ", "SMIT", "CBT", "CBM", "CMO", "CCP", "KMX", "MCRB", "SIFI", "CNCO", "EBR", "CMCM", "SLM", "SMBK", "SOHOM", "SPKEP", "ZNH", "CTR", "STLR", "CXH", "SFR", "SBCP", "TTOO", "TTWO", "TNDM", "CLB", "CXW", "TERP", "TSLA", "CUBI", "CVRR", "DCT", "DAL", "DRD", "DSM", "TOUR", "QURE", "VALU", "VNQI", "EMR", "EEQ", "ESV", "VREX", "VECO", "VIIZ", "VRA", "ENZ", "ERA", "VBND", "VIP", "VNOM", "VYGR", "FLAG", "WHLRD", "WVVI", "WVVIP", "PFD", "FNV", "ZN", "GFA", "GD", "BRSS", "GLP", "AVAL", "HRS", "HLS", "HLF", "HFC", "IFF", "VGM", "INVH", "JHS", "HTD", "JCI", "KEYS", "LGI", "LII", "LSI", "L", "MTZ", "MFG", "CAF", "NFG", "NPK", "NCS", "NPTN", "NWL", "NEA", "NID", "NUM", "NXP", "ORC", "PBA", "PCM", "PNF", "PLNT", "PPL", "PJH", "PSA", "PIM", "PVH", "Q", "SD", "PER", "SWM", "LBF", "SRG", "SBY", "SIX", "SNA", "STWD", "STOR", "SUP", "TEP", "TCP", "TVE", "TVPT", "TRMR", "VR", "IAE", "PPR", "GWW", "W", "WEC", "WLL", "ZTO"}

func randTicker() string {
	return tickers[rand.Intn(len(tickers))]
}

var wg sync.WaitGroup // Global

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(
			os.Stderr,
			`
      Usage: %s --ddl | N_ROWS

        --ddl - prints the table's DDL to be dumped to STDOUT, then exits
        N_ROWS - (type: INT) the total number of rows to generate and load into the table

      Environment variables:
      
        BATCH_SIZE - the number of rows copied at a time  (Default: 128)
        N_THREADS - the number of goroutines run concurrently (Default: 4)


`, os.Args[0])
		os.Exit(1)
	}
	if "--ddl" == os.Args[1] {
		fmt.Println(ddl)
		os.Exit(0)
	}

	nRows, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	batchSize := 128
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		batchSize, err = strconv.Atoi(bs)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "BATCH_SIZE must be an int value: %v\n", err)
	}
	fmt.Printf("BATCH_SIZE: %d\n", batchSize)

	nThreads := 4
	if nt := os.Getenv("N_THREADS"); nt != "" {
		nThreads, err = strconv.Atoi(nt)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "N_THREADS must be an int value: %v\n", err)
	}
	fmt.Printf("N_THREADS: %d\n", nThreads)

	for i := 0; i < nThreads; i++ {
		wg.Add(1)
		go doInserts(int(nRows/nThreads), batchSize)
	}
	wg.Wait()
}

func copyRows(ctx context.Context, tx pgx.Tx, rows [][]interface{}, n int) error {
	// https://pkg.go.dev/github.com/jackc/pgx#hdr-Copy_Protocol
	_, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{"fund_activity"},
		[]string{"id", "ticker", "end_date", "amend", "period_focus", "fiscal_year", "doc_type", "revenues", "op_income", "net_income", "eps_basic", "eps_diluted", "dividend", "assets", "cur_assets", "cur_liab", "cash", "equity", "cash_flow_op", "cash_flow_inv", "cash_flow_fin"},
		pgx.CopyFromRows(rows[0:n]),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading rows: %v\n", err)
	}
	return err
	//fmt.Println("Copy count:", copyCount)
}

func doInserts(nRows, batchSize int) {
	defer wg.Done()
	conn, err := pgx.Connect(context.Background(), "") // Connection params pulled from environment
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	rand.Seed(time.Now().UnixNano())

	rows := [][]interface{}{}
	nRowsInBatch := 0
	for i := 0; i < nRows; i++ {
		if i > 0 && i%batchSize == 0 {
			err := crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
				return copyRows(context.Background(), tx, rows, nRowsInBatch)
			})
			if err != nil {
				log.Fatal("error: ", err)
			}
			nRowsInBatch = 0
		}
		t := randDate()
		row := []interface{}{randUuid(), randTicker(), t, randBool(), randQ(), t.Year(), "10-K", randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(4.0, 5.0), randAmt(4.0, 5.0), randAmt(0.0, 1.0), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09), randAmt(1.0e+06, 20.0e+09)}
		if i < batchSize {
			rows = append(rows, row)
		} else {
			rows[i%batchSize] = row
		}
		nRowsInBatch += 1
	}
	if nRowsInBatch > 0 {
		err := crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return copyRows(context.Background(), tx, rows, nRowsInBatch)
		})
		if err != nil {
			log.Fatal("error: ", err)
		}
		nRowsInBatch = 0
	}
}
