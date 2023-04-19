


docker run -d --net=host -e 'CONSUL_LOCAL_CONFIG={"leave_on_terminate": true}' consul agent -server -bind=10.180.97.91 -bootstrap-expect=1
