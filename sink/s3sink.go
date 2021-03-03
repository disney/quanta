package sink
// S3Sink - Support for SELECT * INTO "s3://..."

import (
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"path"
	"strings"
)

type (
    // S3Sink - State for AWS S3 implemention of Sink interface.
	S3Sink struct {
		outBucket      *s3gof3r.Bucket
		outBucketConf  *s3gof3r.Config
		writer         io.WriteCloser
		csvWriter      *csv.Writer
		headersWritten bool
		delimiter      rune
	}
)

var (
	// Ensure that we implement the Sink interface
	// to ensure this can be called form the Into task
	_ exec.Sink = (*S3Sink)(nil)
)

// NewS3Sink - Construct S3Sink
func NewS3Sink(ctx *plan.Context, path string, params map[string]interface{}) (exec.Sink, error) {
	s := &S3Sink{}
	err := s.Open(ctx, path, params)
	if err != nil {
		u.Errorf("Error creating S3 sink '%v' for path '%v'\n", err, path)
	}
	return s, err
}

// Open session to S3
func (s *S3Sink) Open(ctx *plan.Context, bucketpath string, params map[string]interface{}) error {

	if delimiter, ok := params["delimiter"]; !ok {
		s.delimiter = '\t'
	} else {
		ra := []rune(delimiter.(string))
		s.delimiter = ra[0]
	}
	noScheme := strings.Replace(bucketpath, "s3://", "", 1)
	bucket, file := path.Split(noScheme)
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}
	if file == "" {
		return fmt.Errorf("no file specified")
	}
	bucket = strings.Replace(bucket, "/", "", 1)
	file = strings.Replace(file, "/", "", 1)
	k, err := s3gof3r.EnvKeys() // get S3 keys from environment
	if err != nil {
		return err
	}
	s3 := s3gof3r.New("", k)
	s.outBucket = s3.Bucket(bucket)
	s.outBucketConf = s3gof3r.DefaultConfig
	s.outBucketConf.Concurrency = 16
	w, err := s.outBucket.PutWriter(file, nil, s.outBucketConf)
    if  err != nil {
		return err
	}
	s.writer = w
	s.csvWriter = csv.NewWriter(w)
	s.csvWriter.Comma = s.delimiter
	return nil
}

// Next batch of output data
func (s *S3Sink) Next(dest []driver.Value, colIndex map[string]int) error {
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
func (s *S3Sink) Close() error {
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
