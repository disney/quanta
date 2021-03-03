
# Build and Deploy Instructions

# Environment Build-Out
The following steps should lead to a complete running Quanta stack (all hosts Amazon Linux 2 AMI):

1) If hosting on AWS create a security group with the following open TCP ports.  `8300-8301`, and `4000-4001`.  The first two are for Consul SERF protocol communication between consul server nodes themselves and also communication between Consul agents and server nodes.  Applications that communicate with Quanta do not need to connect via these ports unless they intend to health check Quanta via a locally installed Consul agent.
Port `4000` is for Quanta communication (GRPC) from the `quanta-proxy` to the nodes and for inter-node communication.   Also, `quanta-proxy` listens on port `4000` for database connections from client applications using the `mysql` network protocol.  Port `4001` is the endpoint for the token exchange service.  This service can be used to redeem a JWT token for a set of temporary MySQL credentials.

2) Spin up an EC2 instance (t2.micro).  Follow instructions [HERE](https://github.com/disney/quanta/tree/master/Docker#consul-server-cluster) to create a single node Consul cluster.

3) Spin up 3-15 EC2 instances for Quanta nodes (r5n.12xlarge).  Create a 400 Gib volume using `gp2` SSD (1200 IOPS) mounted on '/data'.  Follow re-requisite instructions [HERE](https://github.com/disney/quanta/tree/master/Docker#ec2-dockerhost-with-consul-agent-service) to create the necessary stack for deploying the `quanta-node` image.  This stack includes the Consul agent and all the bits necessary to host a Quanta component.  It is required for `quanta-node`, `quanta-loader`, and `quanta-proxy`.

4) Spin up an EC2 instance for the Quanta loader (r5n.12xlarge).  No additional storage.  Follow the same re-requisite as the quanta-node in the previous step.  This would be a good time to install AWS credentials for S3 access.  Note that this instance can be stopped in between data load runs.

5) The Quanta proxy will require a modest (r5n.2xlarge) with no additional storage and the same pre-requisite as step #3.

6) Log into the `quanta-node` hosts created in step #3 and pull the `quanta-node` docker image from `containerregistry.disney.com/digital/quanta-node:latest`.  Then follow [THIS LINK](https://github.com/disney/quanta/tree/master/Docker#configuration-of-a-quanta-node) to configure it.  You can find the configuration files [HERE](https://github.com/disney/quanta/tree/master/configuration) and copy them directly.  Configure the entire stack to start at boot using this [LINK](https://github.com/disney/quanta/tree/master/Docker#launching-a-quanta-node-at-boot).  Do this step for all nodes.  Reboot them as you go.

7) Log into the `quanta-proxy` and `quanta-loader` hosts and pull their respective docker images using the same base URI.  The configuration files are already pre-loaded into the docker images.  The `quanta-proxy` can be configured to auto-start using this [LINK](https://github.com/disney/quanta/tree/master/Docker#launching-the-quanta-proxy-at-boot).  The `quanta-proxy` node should be rebooted as well.

8) Verify that all `quanta-nodes` are up and running in healthy status.  Use this [SCRIPT](https://github.com/disney/quanta/tree/master/Docker#health-check).  The number printed to STDOUT should match your node count.

![Architecture](docs/Quanta_Deployment_Architecture.png)

# Overview of Docker Images
In normal production deployments, Docker images can be retrieved directly from Quay (containerregistry.disney.com) and pushed into the Cloud via TeamCity.  Then the desired orchestration services can host the images (i.e. AWS ECS or Kubernetes).
For development sandboxing and testing the images can be built from source and deployed as desired.  The information cotained in this documentation can be utilized for either manual deployment or used as a basis for configuring orchestration services.

The **quanta-node** docker image should be is installed/configured to run as a service daemon via SysV mechanisms (`systemctl`/`initctl`).  All logging of the service is to STDOUT or STDERR of the container.  A minimum of 3 nodes are required on substantial hardware as nodes require significant amounts of RAM to host data (r5n-12xlarge on AWS).  For production a 10-15 node configuration will be required to host 90 days of online data.  Most of the "heavy lifting" during the processing of queries occurs on the nodes.  For data file storage EBS volumes of 400 GiB in size on volume type gp2 for an IOPS throuput of 1200.

Similar to the quanta-nodes, the **quanta-proxy** docker image should be run as a continuously available online service.  They do not require alot of RAM. A moderate mount of CPU can be helpful in maintaining low latency resposes to queries.  They are stateless in nature.  On AWS an r5n-2xlarge will suffice.

**quanta-loader** is ephemeral and are only running during bulk data load events.  They are processing intensive and require a moderate amount of RAM.  An r5n-12xlarge (or the same size as a node) is a good "rule of thumb".

# Pre-requisites
As Quanta is a "masterless" architecture, it relies on a separate small cluster of Consul server mode nodes that provide distributed coordination primarily for fault tolerance and automatic service discovery.  Also, each Quanta component (node, loader, proxy) is deployed on a docker host with the Consul agent pre-installed.

## Consul server cluster
The simplest approach for Consul cluster nodes is to deploy them as Docker containers.  This is in contrast with the Consul agents which are better directly installed at the host level as a Linux service.  Hashicorp, the maker of Consul maintains an image on Dockerhub which is already pre-made and readily available.  All that is required is a very modest hardware footprint (2 VCPUs and 1 Gb of RAM) with Docker installed.  For development purposes, just a single node is required.  Production should be deployed with 3 nodes.  Adding additional nodes beyond that does not improve overall performance.  

For a development/test environment spin up a Docker node and run the following script:
```bash
docker run -d --net=host -e 'CONSUL_LOCAL_CONFIG={"leave_on_terminate": true}' consul agent -server -bind=0.0.0.0 -bootstrap-expect=1
```
This will pull the image from Dockerhub automatically. Remember the address of this host as it will be used by the agents running alongside the Quanta component hosts.  Ideally this would be set up to use DNS.
Run the following command to verify that the Consul server is running:
```bash
docker ps
```
You should see 1 instance running.

## EC2 Dockerhost with Consul agent service
At a minimum, the host (EC2 or other) must contain the following host level services:
1) Docker can be installed by following the instructions in the [AWS Docker Basics](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html) document.  Make sure that the Docker daemon is launched as a service by verifying after reboot by running `docker info` without using sudo.
2) Install Consul by following these instructions:
Download and install the binaries:
```bash
CONSUL_VERSION="1.4.2"
curl --silent --remote-name https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip
curl --silent --remote-name https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_SHA256SUMS
curl --silent --remote-name https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_SHA256SUMS.sig
unzip consul_${CONSUL_VERSION}_linux_amd64.zip
sudo chown root:root consul
sudo mv consul /usr/local/bin/
consul --version
consul -autocomplete-install
complete -C /usr/local/bin/consul consul
sudo useradd --system --home /etc/consul.d --shell /bin/false consul
sudo mkdir --parents /opt/consul
sudo chown --recursive consul:consul /opt/consul
```
Setup Consul to run as a service.  Create a file at `/etc/init/consul.conf` with the following contents:
```bash
description "Consul"
author      "Guy Molinari"

start on (runlevel [345] and started docker)
stop on (runlevel [!345] or stopping docker)

respawn

exec /usr/local/bin/consul agent -bind=0.0.0.0 -retry-join=<consul-cluster-address> -data-dir=/opt/consul
```
Note: The address for the `-retry-join` parameter should be the address(s) of your consul server cluster (Installed separately in a previous step).  

Once the `/etc/init/consul.conf` file is created you can start it as follows:
```bash
initctl reload-configuration
initctl start consul
```

# Manual Image Creation Steps
## Quanta-Node
Follow these steps:
```bash
git clone git@github.com:disney/quanta.git
cd quanta
make build_all
docker build -t containerregistry.disney.com/digital/quanta-node:latest -f Docker/Dockerfile .
```
Note that you should specify your environment specific repo for the `-t` flag (ECR for AWS).  If you do not
already have one for `quanta-node` you can create it using the AWS CLI as follows:
```bash
aws ecr create-repository --repository-name quanta-node --region us-east-1
```
At this point you might want to create repos for `quanta-proxy` and `quanta-loader` if you haven't already done so.

Push the `quanta-node` image into your repository.
1) Get your docker login script for AWS (or `docker login` direct for other enviroments).  For AWS call:
```bash
aws ecr get-login --no-include-email --region us-east-1
```
2) This will return a docker login script with an access token.  Run that script.
3)  Now, push the image:
```bash
docker push containerregistry.disney.com/digital/quanta-node:latest
```
You should substitute your repository URL or course. Your image is now available for deployment.

## Quanta-proxy and Quanta-loader
Follow this step to create binaries for both components:
```bash
git clone git@github.com:disney/quanta.git
cd quanta
make build_all
```
Create the docker image for `quanta-proxy` as follows:
```bash
docker build -t containerregistry.disney.com/digital/quanta-proxy:latest -f Docker/DeployProxyDockerfile .
```
Push the proxy to the Docker repository as previously outlined under `quanta-node`.

Create the docker image for `quanta-loader` as follows:
```bash
docker build -t containerregistry.disney.com/digital/quanta-loader:latest -f Docker/DeployLoaderDockerfile .
```
Push the loader to the Docker repository as previously outlined under `quanta-node`.

# Configuration of a Quanta Node
The quanta-node docker image assumes that all data and configuration is contained within a mount point called `/data`.  When the container is started a physical to logical mapping is provided to the `docker run` command (shown later).

Follow this directory structure for landing configuration and data files:
```bash
/data
└── <host-key> (use the EC2 hostname i.e. ip-10-180-97-82)
    ├── bitmap (node will land all of its data files under this directory)
    └── config
        ├── cities
        │   └── schema.yaml
        ├── cityzip
        │   └── schema.yaml
        └── callsign
            └── schema.yaml
```
The value of `host-key` is used to seed the rendezvous hashing algorithm for consistent hashing used for data sharding .  This ensures that the proper host for a given data item can be quickly determined.  The schema syntax for metadata files is located [HERE](https://github.com/disney/quanta/tree/master/configuration/README.md).  You can find known working configuration files for cities, and cityzip [HERE](https://github.com/disney/quanta/tree/master/configuration) as well.  Copy these files into the structure outlined above and also create an empty `bitmap` directory.

# Configuration of a Quanta Proxy and Loader
These components also need to access the schema file, however since there is no need to update these dynamically at this point, they are "baked" into their respective Docker images at build time.  At some point access to this metadata will be centralized.  For now they must be kept in sync whenever they are modified.

# Launching a Quanta Node at Boot
The following script once installed in `/etc/init/quanta-node.conf` will automatically launch the quanta node docker image upon host restart.  This assumes that the docker image was previosly installed and is up-to-date.

```bash
description "Quanta Node"
author      "Guy Molinari"

start on (runlevel [345] and started consul)
stop on (runlevel [!345] or stopping consul)

respawn

script
    exec docker run -d -t --name quanta-node --net=host --ulimit nofile=1048576:1048576 --rm \
      --mount type=bind,source=/home/ec2-user/data,target=/data \
      -p 0.0.0.0:4000:4000 \
      -e "DATA_DIR=/data/${HOSTNAME}" \
      -e "BIND=0.0.0.0" \
      -t containerregistry.disney.com/digital/quanta-node:latest
end script
```

# Launching the Quanta Proxy at Boot
The following script once installed in `/etc/init/quanta-proxy.conf` will automatically launch the quanta proxy docker image upon host restart.  This assumes that the docker image was previosly installed and is up-to-date.

```bash
description "Quanta Proxy"
author      "Guy Molinari"

start on (runlevel [345] and started consul)
stop on (runlevel [!345] or stopping consul)

respawn

script
    exec docker run -d -t --name quanta-proxy --net=host --rm \
      --mount type=bind,source=/home/ec2-user,target=/data \
      -p 0.0.0.0:4000:4000 \
      -e "SCHEMA_DIR=/data/config" \
      -e "METADATA_DIR=/data/metadata" \
      -e "PUBLIC_KEY_URL=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_hQV7XE2Jz/.well-known/jwks.json" \
      -t containerregistry.disney.com/digital/quanta-proxy:latest
end script
```

# Launching the Quanta Loader
The following should be saved at a script that can be run on demand whenever data needs to be loaded.  Also, credentials to access S3 buckets will be needed.
```bash
docker run --name quanta-loader --net=host --rm --ulimit nofile=262144:262144 \
  -e "BUF_SIZE=10000" \
  -e "INDEX=<my-table>" \
  -e "BUCKET_PATH=<my-bucket>" \
  -e "SCHEMA_DIR=/config" \
  -e "METADATA_DIR=/metadata" \
  -e "AWS_ACCESS_KEY_ID=<my-access-key" \
  -e "AWS_SECRET_ACCESS_KEY=<my-secret-key>" \
  -t containerregistry.disney.com/digital/quanta-loader:latest
```

# Health Check 
The following command will query Consul for the health of nodes and print out the number of nodes in healthy state:
```bash
curl http://127.0.0.1:8500/v1/health/checks/quanta | python -m json.tool | grep passing | wc -l
```
