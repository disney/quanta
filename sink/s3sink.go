package sink

// S3Sink - Support for SELECT * INTO "s3://..."

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"
	"path"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/rlmcpherson/s3gof3r"
	pgs3 "github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
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
		acl            string
		sseKmsKeyId    string
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
		acl            string
		sseKmsKeyId    string		
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
		u.Debug("Format == Parquet")
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
		u.Debug("assumeRoleArn : '%s'\n", s.assumeRoleArn)
	}
	
	if acl, ok := params["acl"]; ok {
		s.acl = acl.(string)
		u.Debug("ACL : '%s'\n", s.acl)
	}

	if sseKmsKeyId, ok := params["sseKmsKeyId"]; ok {
		s.sseKmsKeyId = sseKmsKeyId.(string)
		u.Debug("kms : '%s'\n", s.sseKmsKeyId)
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

	u.Warnf("Parquet Sink: Bucket for parquet write: %s", bucketpath)

	region := "us-east-1"
	if r, ok := params["region"]; ok {
		region = r.(string)
	}

	if assumeRoleArn, ok := params["assumeRoleArn"]; ok {
		s.assumeRoleArn = assumeRoleArn.(string)
		u.Warnf("Parquet Sink: Assuming Arn Role : ", s.assumeRoleArn)
	}

	if acl, ok := params["acl"]; ok {
		s.acl = acl.(string)
		u.Warnf("Parquet Sink: ACL : ", s.acl)
	}

	if sseKmsKeyId, ok := params["sseKmsKeyId"]; ok {
		s.sseKmsKeyId = sseKmsKeyId.(string)
		u.Warnf("Parquet Sink: sseKmsKeyId : ", s.sseKmsKeyId)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		u.Errorf("Parquet Sink: Could not load the default config: %v",err)
	}

	var s3svc *s3.Client

	if s.assumeRoleArn != "" {
		u.Warnf("Parquet Sink: With assume role arn.") 
		
		client := sts.NewFromConfig(cfg)
		provider := stscreds.NewAssumeRoleProvider(client, s.assumeRoleArn, func(a *stscreds.AssumeRoleOptions){
			a.RoleSessionName = "quanta-exporter-session"})
		value,err := provider.Retrieve(context.TODO())

		if err != nil {
			return fmt.Errorf("Failed to retrieve credentials: %v",err)
		}

		u.Warnf("Credential values: %v", value)
		u.Warnf("Access Key: ", value.AccessKeyID)
		u.Warnf("Secret Key: ", value.SecretAccessKey)
		u.Warnf("Session Token: ", value.SessionToken)

		cfg.Credentials = awsv2.NewCredentialsCache(provider)
		_,err = cfg.Credentials.Retrieve(context.TODO())
		if err != nil {
			return fmt.Errorf("Failed to retrieve credentials from cache: %v", err)
		}	

		s3svc = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.Region = region
			o.Credentials = provider
			o.RetryMaxAttempts = 10
		})
	} else {
		u.Warnf("Parquet Sink: Without assume role arn.")
		
		s3svc = s3.NewFromConfig(cfg, 	func(o *s3.Options) {
			o.Region = region
			o.RetryMaxAttempts = 10
		})
	}

	u.Warnf("Parquet Sink: After NewFromConfig.")

	if s3svc == nil {
		u.Errorf("Parquet Sink: Failed to create S3 session.")
		return fmt.Errorf("Failed creating S3 session.")
	}

	// Create S3 service client
	u.Warnf("Parquet Sink: Opening Output S3 path s3:///%s/%s", bucket, file)
	s.outFile, err = pgs3.NewS3FileWriterWithClient(context.Background(), s3svc, bucket, file, nil, func(p *s3.PutObjectInput){
		p.SSEKMSKeyId = &s.sseKmsKeyId
		p.ServerSideEncryption = "aws:kms"
		p.ACL = types.ObjectCannedACL(s.acl)
	})

	u.Warnf("Parquet Sink: After NewS3FileWriterWithClient.")

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

	u.Warnf("Parquet Sink: After constructing parquet metadata.")

	s.csvWriter, err = writer.NewCSVWriter(s.md, s.outFile, 4)
	if err != nil {
		u.Errorf("Parquet Sink: Can't create csv writer %s", err)
		return err
	}

	u.Warnf("Parquet Sink: After NewCSVWriter.")

	s.csvWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	s.csvWriter.CompressionType = parquet.CompressionCodec_SNAPPY
	return nil
}

// Next batch of output data
func (s *S3ParquetSink) Next(dest []driver.Value, colIndex map[string]int) error {

	// u.Warnf("Parquet Sink: Inside Next.")

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

	// u.Warnf("Parquet Sink: After Row creation.")

	rec := make([]*string, len(vals))
	for j := 0; j < len(vals); j++ {
		rec[j] = &vals[j]
	}
	if err := s.csvWriter.WriteString(rec); err != nil {
		return err
	}

	// u.Warnf("Parquet Sink: After WriteString.")

	return nil
}

// Close S3 session.
func (s *S3ParquetSink) Close() error {

	u.Warnf("Parquet Sink: Inside Close.")

	if err := s.csvWriter.WriteStop(); err != nil {
		return fmt.Errorf("Parquet Sink: WriteStop error %v", err)
	}
	u.Warnf("Parquet Sink: Parquet write Finished")
	if err := s.outFile.Close(); err != nil {
		u.Errorf("Parquet Sink: Outfile close error: %v", err)
	}
	u.Warnf("Parquet Sink: After Outfile Close.")
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
