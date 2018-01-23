#!/bin/sh

git clone https://github.com/wurstmeister/kafka-docker

docker-compose up -d
docker-compose scale kafka=3
