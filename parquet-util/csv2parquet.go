package main

//
// Convert CSV files to parquet
//

import (
	"bufio"
	"context"
	"encoding/csv"
	_ "fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/xitongsys/parquet-go-source/local"
	pqs3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New(os.Args[0], "Convert CSV files to Parquet format.").DefaultEnvars()
	input := app.Arg("input", "Input CSV file path. (local or S3://)").Required().String()
	output := app.Arg("output", "Output parquet file path. (local or S3://)").Required().String()
	definition := app.Arg("definition", "Definition file path.").Required().String()
	noHeader := app.Flag("no-header", "If true, file does not contain header line.").Bool()
	padLines := app.Flag("pad-lines", "If true, append empty fields to match header length.").Bool()
	region := app.Flag("aws-region", "AWS region of S3 buckets.").Default("us-east-1").String()

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
	outUrl, err := url.Parse(*output)
	if err != nil {
		log.Println("Can't parse output path", err)
		os.Exit(0)
	}
	var fw source.ParquetFile
	if outUrl.Scheme == "s3" {
		// Initialize S3 client
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String(*region)},
		)

		if err != nil {
			log.Printf("Creating S3 session: %v", err)
			os.Exit(0)
		}

		// Create S3 service client
		log.Printf("Opening Output S3 path s3://%s/%s", outUrl.Host, path.Base(outUrl.Path))
		s3svc := s3.New(sess)
_ = s3svc
		//fw, err = pqs3.NewS3FileWriterWithClient(context.Background(), s3svc, outUrl.Host,
		//	*aws.String(path.Base(outUrl.Path)), nil)
		fw, err = pqs3.NewS3FileWriterWithClient(context.Background(), nil, outUrl.Host,
			*aws.String(path.Base(outUrl.Path)), "", nil)
    //have (context.Context, *"github.com/aws/aws-sdk-go/service/s3".S3, string, string, nil)
    //want (context.Context, s3iface.S3API, string, string, string, []func(*s3manager.Uploader))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fw, err = local.NewLocalFileWriter(*output)
		if err != nil {
			log.Println("Can't open output file", err)
			os.Exit(0)
		}
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

	var reader *csv.Reader
	inUrl, err := url.Parse(*input)
	if err != nil {
		log.Println("Can't parse input path", err)
		os.Exit(0)
	}
	if inUrl.Scheme == "s3" {
		// Initialize S3 client
		insess, err := session.NewSession(&aws.Config{
			Region: aws.String(*region)},
		)

		if err != nil {
			log.Printf("Creating S3 session for input: %v", err)
			os.Exit(0)
		}

		// Create S3 service client
		log.Printf("Opening Input S3 path s3://%s/%s", inUrl.Host, path.Base(inUrl.Path))
		ins3svc := s3.New(insess)
		result, err := ins3svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(inUrl.Host),
			Key:    aws.String(path.Base(inUrl.Path)),
		})
		if err != nil {
			log.Fatal(err)
		}
		reader = csv.NewReader(bufio.NewReader(result.Body))
	} else {
		csvFile, err := os.Open(*input)
		if err != nil {
			log.Println("Can't open input file", err)
			os.Exit(1)
		}
		defer csvFile.Close()
		reader = csv.NewReader(bufio.NewReader(csvFile))
	}

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
