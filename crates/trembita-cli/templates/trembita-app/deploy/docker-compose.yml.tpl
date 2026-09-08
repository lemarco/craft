# Local 3-node cluster for {{PROJECT_NAME}} development.
# Build your app image or run `cargo run` on each node with distinct TREMBITA_NODE_ID.

services:
  node1:
    image: {{PROJECT_NAME}}:local
    build:
      context: ..
      dockerfile: deploy/Dockerfile
    env_file: .env.example
    environment:
      TREMBITA_NODE_ID: "1"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_HTTP: "0.0.0.0:443"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
    volumes:
      - node1-data:/data
    ports:
      - "7443:7443"
      - "443:443"

  node2:
    image: {{PROJECT_NAME}}:local
    env_file: .env.example
    environment:
      TREMBITA_NODE_ID: "2"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
    volumes:
      - node2-data:/data

  node3:
    image: {{PROJECT_NAME}}:local
    env_file: .env.example
    environment:
      TREMBITA_NODE_ID: "3"
      TREMBITA_LISTEN: "0.0.0.0:7443"
      TREMBITA_DATA_DIR: /data
      TREMBITA_JOB_QUEUE: jobs
      TREMBITA_PEERS: "1@node1:7443,2@node2:7443,3@node3:7443"
    volumes:
      - node3-data:/data

volumes:
  node1-data:
  node2-data:
  node3-data:
