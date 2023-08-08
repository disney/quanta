
# docker pull consul:1.10

# docker build -t node -f test/docker-nodes/Dockerfile .

docker network rm my-net2

# everything will be in this subnet
docker network create \
        -d bridge \
        --subnet=172.20.0.0/16 \
        my-net2

docker run \
    -d \
    -p 8500:8500 \
    -p 8600:8600/udp \
    --network my-net2 \
    --name=badger \
    consul:1.10 agent -dev -ui -client=0.0.0.0
 
consulip=172.20.0.2
 
docker run -d --network my-net2 -t node quanta-node --consul-endpoint $consulip:8500  q-node-0 ./data-dir 0.0.0.0 4000

docker run -d --network my-net2 -t node quanta-node --consul-endpoint $consulip:8500  q-node-1 ./data-dir 0.0.0.0 4000

docker run -d --network my-net2 -t node quanta-node --consul-endpoint $consulip:8500  q-node-2 ./data-dir 0.0.0.0 4000

# how do we know when the nodes are all up?
sleep 5 

# the proxy. On port 4000 
docker run -d -p 4000:4000 --network my-net2 -t node quanta-proxy --consul-endpoint $consulip:8500 

sleep 5 

# the sqlrunner
# basic_queries
# insert_tests
# joins_sql

docker run -d -w /quanta/sqlrunner --network my-net2 -t node sqlrunner -script_file ./sqlscripts/insert_tests.sql \
        -validate \
        -host 172.20.0.6 \
        -consul 172.20.0.2:8500 \
        -user MOLIG004 \
        -db quanta \
        -port 4000 \
        -log_level DEBUG
