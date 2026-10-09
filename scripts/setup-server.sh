#!/usr/bin/env bash
# ==============================================================================
# Setup Script for Blank Ubuntu 22.04 LTS Server
# Installs Docker, Docker Compose, starts the Debezium CDC stack,
# registers connectors, and runs validation tests.
# ==============================================================================
set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}=================================================================${NC}"
echo -e "${CYAN}   Automated Setup: Debezium CDC POC on Ubuntu 22.04 LTS        ${NC}"
echo -e "${CYAN}=================================================================${NC}"

# 1. Update and install prerequisites
echo -e "\n${YELLOW}>>> [1/5] Updating system packages and installing prerequisites...${NC}"
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release jq git

# 2. Install Docker if not present
if ! command -v docker &> /dev/null; then
  echo -e "\n${YELLOW}>>> [2/5] Docker not found. Installing Docker Engine...${NC}"
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
  sudo chmod a+r /etc/apt/keyrings/docker.gpg

  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  sudo systemctl enable docker
  sudo systemctl start docker

  # Add current user to docker group
  sudo usermod -aG docker "$USER" || true
  echo -e "${GREEN}Docker installed successfully!${NC}"
else
  echo -e "${GREEN}>>> [2/5] Docker is already installed: $(docker --version)${NC}"
fi

# Ensure docker compose is available
if ! docker compose version &> /dev/null; then
  echo -e "${YELLOW}Installing docker-compose-plugin...${NC}"
  sudo apt-get install -y docker-compose-plugin
fi

# 3. Navigate to repository root
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

echo -e "\n${YELLOW}>>> [3/5] Starting Docker Compose Stack...${NC}"
echo -e "Starting: Zookeeper, Kafka, MongoDB (ReplicaSet), Connect, Postgres, CDC-Consumer, Kafka-UI..."
docker compose up -d --build

echo -e "\n${YELLOW}>>> Checking running containers...${NC}"
docker compose ps

# 4. Register Debezium MongoDB Source Connector
echo -e "\n${YELLOW}>>> [4/5] Registering Debezium MongoDB Source Connector...${NC}"
chmod +x ./scripts/register-connectors.sh
chmod +x ./scripts/test-cdc.sh
./scripts/register-connectors.sh

echo -e "\n${YELLOW}>>> Waiting 8 seconds for Debezium to initialize change stream cursors...${NC}"
sleep 8

# Restart cdc-consumer so it immediately attaches to the established topic
docker compose restart cdc-consumer
sleep 3

# 5. Run Verification Test
echo -e "\n${YELLOW}>>> [5/5] Running End-to-End Replication Test...${NC}"
./scripts/test-cdc.sh

echo -e "\n${GREEN}=================================================================${NC}"
echo -e "${GREEN}   Debezium CDC POC Deployment Complete!                         ${NC}"
echo -e "${GREEN}=================================================================${NC}"
echo -e "Web Management UI:"
echo -e "  - Kafka UI:      http://<SERVER_IP>:8080"
echo -e "  - Kafka Connect: http://<SERVER_IP>:8083/connectors"
echo -e "  - MongoDB:       mongodb://<SERVER_IP>:27017/?replicaSet=rs0"
echo -e "  - PostgreSQL:    postgresql://postgres:postgres_password@<SERVER_IP>:5432/inventory_sink"
echo -e "Logs:"
echo -e "  - View CDC consumer logs: docker compose logs -f cdc-consumer"
