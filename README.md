# Hybrid Search

This repo contains a public port of the hybrid search code used on [aerospike.com](https://aerospike.com/search).
To run this locally, follow the steps below.

> [!NOTE]
> Aerospike Vector Search (AVS) requires a feature key. [Request one](https://aerospike.com/docs/vector?utm_medium=web&utm_source=aerospike-github).

## Pre-requisites

1. An Aerospike feature key with vector search enabled.
2. Docker & docker-compose


## Running locally using docker-compose


1. Clone this repo and navigate to the `hybrid-search` directory.
    ```
    git clone https://github.com/aerospike-examples/hybrid-search.git && cd hybrid-search
    ```
2. Replace the `config/aerospike/features.replace.conf` and `config/vector/features.replace.conf` with a valid Aerospike feature key file.
    >**Note**
    >
    >The feature key file must have a line item for `vector-service`
3. Create the containers by running:
    ```bash
    DOCKER_BUILDKIT=0 docker compose up -d
    ```
4. Load data into the database by running:
    ```bash
    docker exec -it -w /server search-server python3 load.py
    ```
    >**Note**
    >
    >This will take some time. It's scraping and loading the Aerospike support knowledgebase.
5. Once the load script is finished, query the endpoint with query parameter `q`.  
   For example:
    ```
    http://localhost:8080/rest/v1/search/?q=secondary index
    ```
    
