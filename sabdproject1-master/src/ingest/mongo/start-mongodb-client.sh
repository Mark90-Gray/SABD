#!/bin/bash
docker run --rm -t -i --network=project_network --name=mongo_client mongo:latest /bin/bash
