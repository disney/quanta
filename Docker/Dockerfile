# QUANTA server node deployment within a Docker Container

#FROM amd64/alpine:latest
FROM frolvlad/alpine-glibc

LABEL version="1.0.0" \
  maintainer="Guy Molinari" \
  maintainer.email="molinariguy@gmail.com" \
  description="Builds Docker image for use in production deployment of QUANTA server nodes" \
  repository="https://github.com/disney-platforms/quanta"

# Update 
RUN apk update
RUN apk upgrade
RUN apk add --no-cache --update alpine-sdk

# clean apk cache
RUN rm -rf /var/cache/apk/*

COPY ./bin/quanta-node /usr/bin/quanta-node
COPY ./Docker/entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod 755 /usr/bin/quanta-node
RUN chmod 755 /usr/bin/entrypoint.sh

# Expose default port, but can take in docker run --expose flag as additive port to expose
# Port mapping of server node. Default("127.0.0.1:4000")
EXPOSE 4000

VOLUME /data

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
