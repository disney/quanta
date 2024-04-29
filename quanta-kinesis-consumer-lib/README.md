

### Set up localstack for local testing of AWS api's 

Using a terminal and a package manager install once:
```
	brew install localstack
	localstack --version
```
get
```
    3.2.0
```
In a new terminal window:
```
    localstack start
```
Leave that running and, in a different terminal window. 
```
    pip install awscli-local
```
Try:
```
	awslocal s3api create-bucket --bucket sample-bucket
```
and get
```
{
    "Location": "/sample-bucket"
}
```
This is how we can do AWS commands:
```
	awslocal s3 ls
		2024-03-24 04:21:13 sample-bucket
```
