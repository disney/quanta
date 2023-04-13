# FROM containerregistry.disney.com/digital/amzn-lnx2-jdk8:27f75ff815ad-1.0.141
# FROM containers.disney.com/golang:1.14.15-alpine3.13-build-202107
FROM containerregistry.disney.com/digital/amazonlinux

LABEL version="0.0.1" \
  maintainer="Curtis Deems" \
  maintainer.email="curtis.deems@disney.com" \
  description="Quanta Loader" \
  repository="git@gitlab.disney.com:data-platforms/curtis_x_deems_nd/quanta-loader.git"

USER root
COPY ./quanta-loader /usr/bin/quanta-loader
COPY ./docker/entrypoint.sh /usr/bin/entrypoint.sh

RUN echo -e '#\
!/bin/bash \n\
cd /usr/bin \n\
' > /usr/bin/entrypoint.sh \
 && chmod +x /usr/bin/entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/usr/bin/entrypoint.sh"]

