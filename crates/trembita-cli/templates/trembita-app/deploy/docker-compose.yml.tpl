# Local 3-node cluster for {{PROJECT_NAME}} (dynamic join — do not set TREMBITA_NODE_ID).
# Mount mTLS material at TREMBITA_CERT_DIR (see dev/certs/generate.sh + node-0 bootstrap for joiners).

services:
  node1:
    image: {{PROJECT_NAME}}:local
    build:
      context: ..
      dockerfile: deploy/Dockerfile
    env_file: .env.example
    environment:
      TREMBITA_LISTEN: "0.0.0.0:443"
      TREMBITA_ALLOW_JOIN: "1"
      TREMBITA_DATA_DIR: /data
    volumes:
      - node1-data:/data
    ports:
      - "443:443"

  node2:
    image: {{PROJECT_NAME}}:local
    env_file: .env.example
    environment:
      TREMBITA_LISTEN: "0.0.0.0:443"
      TREMBITA_JOIN_SEEDS: "1@node1:443"
      TREMBITA_DATA_DIR: /data
    volumes:
      - node2-data:/data
    depends_on:
      - node1

  node3:
    image: {{PROJECT_NAME}}:local
    env_file: .env.example
    environment:
      TREMBITA_LISTEN: "0.0.0.0:443"
      TREMBITA_JOIN_SEEDS: "1@node1:443"
      TREMBITA_DATA_DIR: /data
    volumes:
      - node3-data:/data
    depends_on:
      - node1

volumes:
  node1-data:
  node2-data:
  node3-data:
