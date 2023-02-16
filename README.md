# Kafka With Java

A simple project to demo how tp use Kafka with Java

## How to run:

- Need Java 17 and Gradle 7.3
- Have an Open Search server running + an admin connection string. Easy way is to use [Bonsai](https://bonsai.io/blog/welcome-to-opensearch)
- Have Docker and Docker Compose tools. Easy way is to install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- To starts a Kafka cluster + Conductor UI on local: `docker-compose -f docker/docker-compose-dev.yml up`
- Once Kafka cluster is ready, you can now execute the Java application
