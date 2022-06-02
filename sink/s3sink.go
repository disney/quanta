package sink

// S3Sink - Support for SELECT * INTO "s3://..."

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/rlmcpherson/s3gof3r"
	pqs3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io"
	"path"
	"strings"
)

type (
	// S3CSVSink - State for AWS S3 implemention of Sink interface for CSV output.
	S3CSVSink struct {
		outBucket      *s3gof3r.Bucket
		outBucketConf  *s3gof3r.Config
		writer         io.WriteCloser
		csvWriter      *csv.Writer
		headersWritten bool
		delimiter      rune
		assumeRoleArn  string
		config         *aws.Config
	}
)

type (
	// S3ParquetSink - State for AWS S3 implemention of Sink interface for Parquet output.
	S3ParquetSink struct {
		csvWriter      *writer.CSVWriter
		outFile        source.ParquetFile
		md        	   []string
		assumeRoleArn  string
		config         *aws.Config
	}
)

var (
	// Ensure that we implement the Sink interface
	// to ensure this can be called form the Into task
	_ exec.Sink = (*S3CSVSink)(nil)
	_ exec.Sink = (*S3ParquetSink)(nil)
)

// NewS3Sink - Construct S3Sink
func NewS3Sink(ctx *plan.Context, path string, params map[string]interface{}) (exec.Sink, error) {

	var s exec.Sink
	s = &S3CSVSink{}
	if fmt, ok := params["format"]; ok && fmt == "parquet" {
		s = &S3ParquetSink{}
	}
	err := s.Open(ctx, path, params)
	if err != nil {
		u.Errorf("Error creating S3 sink '%v' for path '%v'\n", err, path)
	}
	return s, err
}

// Open CSV session to S3
func (s *S3CSVSink) Open(ctx *plan.Context, bucketpath string, params map[string]interface{}) error {

	if delimiter, ok := params["delimiter"]; !ok {
		s.delimiter = '\t'
	} else {
		ra := []rune(delimiter.(string))
		s.delimiter = ra[0]
	}

	if assumeRoleArn, ok := params["assumeRoleArn"]; ok {
		s.assumeRoleArn = assumeRoleArn.(string)
	} else {
		s.assumeRoleArn = ""
	}
	
	bucket, file, err := parseBucketName(bucketpath)
	if err != nil {
		return err
	}

	// k, err := s3gof3r.EnvKeys() // get S3 keys from environment
	k, err := s3gof3r.InstanceKeys() // get S3 keys from environment
	if err != nil {
		return err
	}
	s3 := s3gof3r.New("", k)
	s.outBucket = s3.Bucket(bucket)
	s.outBucketConf = s3gof3r.DefaultConfig
	s.outBucketConf.Concurrency = 16
	w, err := s.outBucket.PutWriter(file, nil, s.outBucketConf)
	if err != nil {
		return err
	}
	s.writer = w
	s.csvWriter = csv.NewWriter(w)
	s.csvWriter.Comma = s.delimiter
	return nil
}

// Next batch of output data
func (s *S3CSVSink) Next(dest []driver.Value, colIndex map[string]int) error {
	if !s.headersWritten {
		cNames := make([]string, len(colIndex))
		for k, i := range colIndex {
			cNames[i] = k
		}
		headers := []byte(strings.Join(cNames, string(s.delimiter)) + "\n")
		if s.writer == nil {
			return fmt.Errorf("nil writer, open call must have failed")
		}
		if _, err := s.writer.Write(headers); err != nil {
			return err
		}
		s.headersWritten = true
	}
	vals := make([]string, len(dest))
	for i, v := range dest {
		if val, ok := v.(string); ok {
			vals[i] = strings.TrimSpace(val)
		} else if val, ok := v.(value.StringValue); ok {
			vals[i] = strings.TrimSpace(val.Val())
		} else if val, ok := v.(value.BoolValue); ok {
			vals[i] = strings.TrimSpace(val.ToString())
		} else {
			vals[i] = strings.TrimSpace(fmt.Sprintf("%v", v))
		}
	}
	if err := s.csvWriter.Write(vals); err != nil {
		return err
	}
	return nil
}

// Close S3 session.
func (s *S3CSVSink) Close() error {
	// Channel closed so close the output chunk
	if s.writer == nil {
		return nil
	}
	if s.csvWriter != nil {
		s.csvWriter.Flush()
	}
	if err := s.writer.Close(); err != nil {
		return err
	}
	return nil
}

// Open Parquet session to S3
func (s *S3ParquetSink) Open(ctx *plan.Context, bucketpath string, params map[string]interface{}) error {

	bucket, file, err := parseBucketName(bucketpath)
	if err != nil {
		return err
	}

	region := "us-east-1"
	if r, ok := params["region"]; ok {
		region = r.(string)
	}

	if assumeRoleArn, ok := params["assumeRoleArn"]; ok {
		s.assumeRoleArn = assumeRoleArn.(string)
		u.Debug("Using Arn Role : '%s'\n", s.assumeRoleArn)
	} else {
		s.assumeRoleArn = ""
	}

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	if err != nil {
		return fmt.Errorf("Creating S3 session: %v", err)
	}

	s3svc := &s3.S3{}
	if s.assumeRoleArn != "" {
		creds := stscreds.NewCredentials(sess, s.assumeRoleArn)
		s.config = aws.NewConfig().
			WithCredentials(creds).
			WithRegion(region).
			WithMaxRetries(10)
		s3svc = s3.New(sess, s.config)
		if s3svc != nil {
			u.Debug("Retrieved AWS session for writing Parquet using Arn Role '%s' \n", s.assumeRoleArn)
		}
	} else {
		s3svc = s3.New(sess)
	}

	// Create S3 service client
	u.Infof("Opening Output S3 path s3:///%s/%s", bucket, file)
	s.outFile, err = pqs3.NewS3FileWriterWithClient(context.Background(), s3svc, bucket, file, nil)
	if err != nil {
		u.Error(err)
		return err
	}

	// Construct parquet metadata
	s.md = make([]string, len(ctx.Projection.Proj.Columns))
	for i, v := range ctx.Projection.Proj.Columns {
		switch v.Type {
		case value.IntType:
			s.md[i] = fmt.Sprintf("name=%s, type=INT64", v.As)
		case value.NumberType:
			s.md[i] = fmt.Sprintf("name=%s, type=FLOAT", v.As)
		case value.BoolType:
			s.md[i] = fmt.Sprintf("name=%s, type=BOOLEAN", v.As)
		default:
			s.md[i] = fmt.Sprintf("name=%s, type=UTF8, encoding=PLAIN_DICTIONARY", v.As)
		}
	}

	s.csvWriter, err = writer.NewCSVWriter(s.md, s.outFile, 4)
	if err != nil {
		u.Errorf("Can't create csv writer %s", err)
		return err
	}
	s.csvWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	s.csvWriter.CompressionType = parquet.CompressionCodec_SNAPPY
	return nil
}

// Next batch of output data
func (s *S3ParquetSink) Next(dest []driver.Value, colIndex map[string]int) error {

	vals := make([]string, len(dest))
	for i, v := range dest {
		if val, ok := v.(string); ok {
			vals[i] = strings.TrimSpace(val)
		} else if val, ok := v.(value.StringValue); ok {
			vals[i] = strings.TrimSpace(val.Val())
		} else if val, ok := v.(value.BoolValue); ok {
			vals[i] = strings.TrimSpace(val.ToString())
		} else {
			vals[i] = strings.TrimSpace(fmt.Sprintf("%v", v))
		}
	}
	rec := make([]*string, len(vals))
	for j := 0; j < len(vals); j++ {
		rec[j] = &vals[j]
	}
	if err := s.csvWriter.WriteString(rec); err != nil {
		return err
	}
	return nil
}

// Close S3 session.
func (s *S3ParquetSink) Close() error {

	if err := s.csvWriter.WriteStop(); err != nil {
		return fmt.Errorf("WriteStop error %v", err)
	}
	u.Debug("Parquet write Finished")
	s.outFile.Close()
	return nil
}

func parseBucketName(bucketPath string) (bucket, file string, err error) {

	noScheme := strings.Replace(bucketPath, "s3://", "", 1)
	bucket, file = path.Split(noScheme)
	if bucket == "" {
		err = fmt.Errorf("no bucket specified")
		return
	}
	if file == "" {
		err = fmt.Errorf("no file specified")
		return
	}
	bucket = strings.Replace(bucket, "/", "", 1)
	file = strings.Replace(file, "/", "", 1)
	if bucket == "" {
		err = fmt.Errorf("no bucket specified")
	}
	if file == "" {
		err = fmt.Errorf("no file specified")
	}
	return
}
