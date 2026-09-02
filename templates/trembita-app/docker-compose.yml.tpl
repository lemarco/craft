# Local 3-node cluster for {{PROJECT_NAME}} development.
# Adjust image/build once you publish your own container; this references trembita-node patterns.

services:
  node1:
    image: trembita-node:local
    build:
      context: ..
      dockerfile: e2e/Dockerfile
    environment:
      TREMBITA_NODE_ID: "1"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_ADMIN: "0.0.0.0:8080"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
      TREMBITA_CA_CERT: /certs/ca.pem
      TREMBITA_NODE_CERT: /certs/node1.pem
      TREMBITA_NODE_KEY: /certs/node1.key
    volumes:
      - node1-data:/data
      - ./certs:/certs:ro
    ports:
      - "7443:7443"
      - "8080:8080"

  node2:
    image: trembita-node:local
    environment:
      TREMBITA_NODE_ID: "2"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
      TREMBITA_CA_CERT: /certs/ca.pem
      TREMBITA_NODE_CERT: /certs/node2.pem
      TREMBITA_NODE_KEY: /certs/node2.key
    volumes:
      - node2-data:/data
      - ./certs:/certs:ro

  node3:
    image: trembita-node:local
    environment:
      TREMBITA_NODE_ID: "3"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
      TREMBITA_CA_CERT: /certs/ca.pem
      TREMBITA_NODE_CERT: /certs/node3.pem
      TREMBITA_NODE_KEY: /certs/node3.key
    volumes:
      - node3-data:/data
      - ./certs:/certs:ro

volumes:
  node1-data:
  node2-data:
  node3-data:
