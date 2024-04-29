
# Requirements:
# The consul on the host machine must NOT be running.
# ALL the containers must be stopped and removed.
# Pull the consul image once: docker pull consul:1.10
# Build the node image (below) every time you change the code.

# docker build -t node -f test/docker-nodes/Dockerfile .

# fire it up:   ./test/docker-nodes/docker-start-nodes.sh

docker network rm mynet

# everything will be in this subnet
docker network create \
        -d bridge \
        --subnet=172.20.0.0/16 \
        mynet

docker run \
    -d \
    -p 8500:8500 \
    -p 8600:8600/udp \
    --network mynet \
    --name=myConsul \
    consul:1.10 agent -dev -ui -client=0.0.0.0
 
consulip=172.20.0.2
 
docker run -d --network mynet --name q-node-0 -t node quanta-node --consul-endpoint $consulip:8500  q-node-0 ./data-dir 0.0.0.0 4000

docker run -d --network mynet --name q-node-1 -t node quanta-node --consul-endpoint $consulip:8500  q-node-1 ./data-dir 0.0.0.0 4000

docker run -d --network mynet --name q-node-2 -t node quanta-node --consul-endpoint $consulip:8500  q-node-2 ./data-dir 0.0.0.0 4000
 

# how do we know when the nodes are all up?
sleep 10 

# the proxy. On port 4000 
docker run -d -p 4000:4000 --network mynet --name quanta-proxy-0 -t node quanta-proxy --consul-endpoint $consulip:8500 

sleep 15 

# the sqlrunner
# basic_queries
# insert_tests
# joins_sql

docker run -w /quanta/sqlrunner --name basic_queries --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries.sql \
        -validate \
        -host 172.20.0.6 \
        -consul 172.20.0.2:8500 \
        -user MOLIG004 \
        -db quanta \
        -port 4000 \
        -log_level DEBUG

# docker run -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/insert_tests.sql \
#         -validate \
#         -host 172.20.0.6 \
#         -consul 172.20.0.2:8500 \
#         -user MOLIG004 \
#         -db quanta \
#         -port 4000 \
#         -log_level DEBUG

# docker run -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/joins_sql.sql \
#         -validate \
#         -host 172.20.0.6 \
#         -consul 172.20.0.2:8500 \
#         -user MOLIG004 \
#         -db quanta \
#         -port 4000 \
#         -log_level DEBUG
