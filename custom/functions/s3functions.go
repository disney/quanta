package functions

import (
	"fmt"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

const (
	publicACLIndicator = "http://acs.amazonaws.com/groups/global/AllUsers"
)

// IsBucketPublic - Check S3 bucket location and returns true if it allows public access (or error string)
//
//     is_bucket_public("error", "myPublicBucket") => true
//
type IsBucketPublic struct{}

// Type is string
func (m *IsBucketPublic) Type() value.ValueType { return value.StringType }

// Validate is bucket public
func (m *IsBucketPublic) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for is_bucket_public(str_region, str_bucket_name) but got %s", n)
	}
	return isBucketPublicEval, nil
}

func isBucketPublicEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args[0].ToString())},
	)

	if err != nil {
		//return fmt.Errorf("Creating S3 session: %v", err)
		u.Errorf("Creating S3 session: %v", err)
		return value.NewStringValue(fmt.Sprintf(err.Error())), true
	}

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.GetBucketAclInput{
		Bucket: aws.String(args[1].ToString()),
	}

	result, err := svc.GetBucketAcl(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				u.Errorf(aerr.Error())
				return value.NewStringValue(fmt.Sprintf(aerr.Error())), true
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			u.Errorf(err.Error())
			return value.NewStringValue(fmt.Sprintf(err.Error())), true
		}
	}
	for _, v := range result.Grants {
		if v.Grantee.URI != nil && *v.Grantee.URI == publicACLIndicator {
			return value.BoolValueTrue, true
		}
	}

	return value.BoolValueFalse, true
}

// IsBucketEncrypted - Check S3 bucket location and returns true if encryption is enabled.
//
//     is_bucket_encrypted("region", "myPublicBucket") => true or error string
//
type IsBucketEncrypted struct{}

// Type is String
func (m *IsBucketEncrypted) Type() value.ValueType { return value.StringType }

// Validate is bucket encrypted.
func (m *IsBucketEncrypted) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for is_bucket_encrypted(str_region, str_bucket_name) but got %s", n)
	}
	return isBucketEncryptedEval, nil
}

func isBucketEncryptedEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args[0].ToString())},
	)

	if err != nil {
		//return fmt.Errorf("Creating S3 session: %v", err)
		u.Errorf("Creating S3 session: %v", err)
		return value.NewStringValue(fmt.Sprintf(err.Error())), true
	}

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.GetBucketEncryptionInput{
		Bucket: aws.String(args[1].ToString()),
	}

	_, err2 := svc.GetBucketEncryption(input)
	if err2 != nil {
		if aerr, ok := err2.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				u.Errorf(aerr.Error())
				return value.NewStringValue(fmt.Sprintf(aerr.Error())), true
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			u.Errorf(err2.Error())
			return value.NewStringValue(fmt.Sprintf(err2.Error())), true
		}
	}

	return value.BoolValueTrue, true
}

// IsBucketReadable - Check S3 bucket location and returns true if bucket can be read
//
//     is_bucket_readable("region", "myPublicBucket") => true or error string
//
type IsBucketReadable struct{}

// Type is String
func (m *IsBucketReadable) Type() value.ValueType { return value.StringType }

// Validate is bucket readable
func (m *IsBucketReadable) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for is_bucket_readable(str_region, str_bucket_name) but got %s", n)
	}
	return isBucketReadableEval, nil
}

func isBucketReadableEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args[0].ToString())},
	)

	if err != nil {
		//return fmt.Errorf("Creating S3 session: %v", err)
		u.Errorf("Creating S3 session: %v", err)
		return value.NewStringValue(fmt.Sprintf(err.Error())), true
	}

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.ListObjectsInput{
		Bucket: aws.String(args[1].ToString()),
	}

	_, err2 := svc.ListObjects(input)
	if err2 != nil {
		if aerr, ok := err2.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				u.Errorf(aerr.Error())
				return value.NewStringValue(fmt.Sprintf(aerr.Error())), true
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			u.Errorf(err2.Error())
			return value.NewStringValue(fmt.Sprintf(err2.Error())), true
		}
	}

	return value.BoolValueTrue, true
}

// IsBucketWritable - Check S3 bucket location and returns true if bucket can be written
//
//     is_bucket_writeable("region", "myPublicBucket") => true or error string
//
type IsBucketWritable struct{}

// Type is String
func (m *IsBucketWritable) Type() value.ValueType { return value.StringType }

// Validate is bucket writeable
func (m *IsBucketWritable) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for is_bucket_writeable(str_region, str_bucket_name) but got %s", n)
	}
	return isBucketWritableEval, nil
}

func isBucketWritableEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args[0].ToString())},
	)

	if err != nil {
		//return fmt.Errorf("Creating S3 session: %v", err)
		u.Errorf("Creating S3 session: %v", err)
		return value.NewStringValue(fmt.Sprintf(err.Error())), true
	}

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("dummydata")),
		Bucket: aws.String(args[1].ToString()),
		Key:    aws.String("_TEST"),
	}

	_, err2 := svc.PutObject(input)
	if err2 != nil {
		if aerr, ok := err2.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				u.Errorf(aerr.Error())
				return value.NewStringValue(fmt.Sprintf(aerr.Error())), true
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			u.Errorf(err2.Error())
			return value.NewStringValue(fmt.Sprintf(err2.Error())), true
		}
	}

	input2 := &s3.DeleteObjectInput{
		Bucket: aws.String(args[1].ToString()),
		Key:    aws.String("_TEST"),
	}

	_, err3 := svc.DeleteObject(input2)
	if err3 != nil {
		if aerr, ok := err3.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				u.Errorf(aerr.Error())
				return value.NewStringValue(fmt.Sprintf(aerr.Error())), true
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			u.Errorf(err3.Error())
			return value.NewStringValue(fmt.Sprintf(err3.Error())), true
		}
	}
	return value.BoolValueTrue, true
}
