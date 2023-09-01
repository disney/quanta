
### Docker nodes brings up a test cluster locally using docker for the nodes, proxy, consul, and sqlrunner

#### Init

Pull the consul image:

```
docker pull consul:1.10
```

#### development loop

Build the image.

CD to the root of the project and then

```
docker build -t node -f test/docker-nodes/Dockerfile .
```

##### run the containers

Delete containers from previous run as needed. 

Then:
```
./test/docker-nodes/docker-start-nodes.sh
```

A common error is that consul is already running in a terminal. Stop it.

I'm using docker desktop so I'm using a GUI to browse the logs.
Note that the sqlrunner test can be re-run by just doing that part of the script.

Examine the logs, edit the code.
Delete all the containers.

Go to 'development loop' above to build and run again.

