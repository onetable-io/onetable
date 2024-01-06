FROM bitnami/spark:3.2.4

USER root

# Install vim and cd to onetable
RUN apt-get update \
    && apt-get install -y vim \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /home/onetable \
    && chmod -R 777 /home/onetable

WORKDIR /home/onetable