
# docker pull consul:1.10

# docker build -t node .

# sudo route add -host 172.20.0.0/16 -interface en0

# sudo route -n add -net 172.20.0.0/16 192.168.86.1 what is the switch ip ?  192.168.86.1 ??

# sudo route -n add 192.168.2.0/24 172.31.30.5 ??? 

# sudo route -n add -net 172.20.0.0/16  192.168.86.254

# sudo route delete 172.20.0.0/16

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

# consulip=192.168.86.118 # my mac 
# consulip=host.docker.internal
 
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
        -user MOLIG004 \
        -db quanta \
        -port 4000 \
        -log_level DEBUG

