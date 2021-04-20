package main
//
// Convert CSV files to parquet
//

import (
	"bufio"
	"encoding/csv"
	_ "fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New(os.Args[0], "Convert CSV files to Parquet format.").DefaultEnvars()
	input := app.Arg("input", "Input CSV file path.").Required().String()
	output := app.Arg("output", "Output parquet file path.").Required().String()
	definition := app.Arg("definition", "Definition file path.").Required().String()
	noHeader := app.Flag("no-header", "If true, file does not contain header line.").Bool()
	padLines := app.Flag("pad-lines", "If true, append empty fields to matchg header length.").Bool()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	/* Read in definition file.
	 * Example (see parquet-go docs for details):
	 *     name=Name, type=UTF8, encoding=PLAIN_DICTIONARY
	 *     name=Age, type=INT32
	 *     name=Id, type=INT64
	 *     name=Weight, type=FLOAT
	 *     name=Sex, type=BOOLEAN
	 */

	defFile, err := os.Open(*definition)
	if err != nil {
		log.Printf("Definition file open: %v", err)
		os.Exit(1)
	}
	defer defFile.Close()

	var md []string
	scanner := bufio.NewScanner(defFile)
	for scanner.Scan() {
		md = append(md, scanner.Text())
	}
	if scanner.Err() != nil {
		log.Printf("Definition file scanner: %v", scanner.Err())
		os.Exit(1)
	}
	defMap := make(map[string]int)
	for i, w := range md {
		s := strings.Split(w, ",")
		for _, v := range s {
			lv := strings.ToLower(v)
			if name := strings.TrimPrefix(lv, "name="); name != lv { // Got name
				defMap[name] = i
			}
		}
	}

	//write
	fw, err := local.NewLocalFileWriter(*output)
	if err != nil {
		log.Println("Can't open output file", err)
		os.Exit(0)
	}
	pw, err := writer.NewCSVWriter(md, fw, 4)
	if err != nil {
		log.Println("Can't create csv writer", err)
		os.Exit(0)
	}
	//start := time.Now()
	//elapsed := time.Since(start)
	//log.Printf("Bitmap data server initialized in %v.", elapsed)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			log.Printf("Interrupt signal received.  Exiting ...")
			os.Exit(0)
		}
	}()

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	csvFile, err := os.Open(*input)
	if err != nil {
		log.Println("Can't open input file", err)
		os.Exit(1)
	}
	defer csvFile.Close()
	reader := csv.NewReader(bufio.NewReader(csvFile))
	reader.FieldsPerRecord = -1

	// array of indices into record buffer
	var bufferIndices []int
	if !*noHeader {
		header, _ := reader.Read()
		bufferIndices = make([]int, len(header))
		for i, v := range header {
			canonicalName := strings.ReplaceAll(strings.ToLower(v), " ", "_")
			if idx, ok := defMap[canonicalName]; ok {
				bufferIndices[i] = idx
			} else {
				bufferIndices[i] = -1 // Field will be skipped
			}
		}
	} else {
		// No header row, OK but input CSV must be isomorphic with definition
		bufferIndices = make([]int, len(md))
		for i := 0; i < len(bufferIndices); i++ {
			bufferIndices[i] = i
		}

	}

	rec := make([]*string, len(md))
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		if *padLines && !*noHeader && len(line) != len(bufferIndices) {
			newLine := make([]string, len(bufferIndices))
			copy(newLine, line)
			line = newLine
		}

		// Verify input line lengh matches len of bufferIndices
		if len(line) != len(bufferIndices) {
			log.Printf("ERROR: Input line len is %d, Definition len is %d!", len(line),
				len(bufferIndices))
			log.Printf("%#v\n", line)
			log.Printf("Either fix definition file or provide header in input file.")
			os.Exit(1)
		}

		for i := range line {
			if bufferIndices[i] != -1 {
				rec[bufferIndices[i]] = &line[i]
			}
		}
		if err = pw.WriteString(rec); err != nil {
			log.Println("Write error", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	log.Println("Write Finished")
	fw.Close()
	os.Exit(0)

}
