docker run -d --net=host -e 'CONSUL_LOCAL_CONFIG={"leave_on_terminate": true}' consul agent -bind=10.180.97.82  -retry-join=10.180.97.91
