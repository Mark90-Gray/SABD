#!/bin/bash
docker network create project_network
docker run --rm -t -i -p 27017:27017 --network=project_network --name mongo_server mongo /usr/bin/mongod --bind_ip_all
