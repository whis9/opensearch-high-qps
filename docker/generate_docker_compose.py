# Script by Aarshit Mittal
# Title: Docker Compose Generator for High-QPS OpenSearch Cluster
# Purpose: Automatically generate a docker-compose.yml file for running a multi-node OpenSearch cluster
# Usage:
#   1. Make sure you have Docker and Docker Compose installed.
#   2. Pull the required OpenSearch image manually before starting:
#      docker pull opensearchproject/opensearch:2.7.0
#   3. Run this script to generate docker-compose.yml for 40 OpenSearch nodes.
#   4. Use `docker-compose up -d` to launch all containers.

# --- Configuration Section ---

# Total number of OpenSearch nodes (this will also equal number of shards you plan to use)
node_count = 40

# Java heap size per node (adjust based on your server's available memory)
heap_size = "16g"

# Starting HTTP port (each node gets a unique port incrementally)
http_start_port = 9501

# Starting transport port (used for internal OpenSearch communication)
transport_start_port = 9601

# Initial master nodes (typically 3 nodes for cluster stability)
initial_master_nodes = ",".join([f"os-node{i}" for i in range(1, 4)])

# --- Docker Compose Generation ---

with open("docker-compose.yml", "w") as f:
    # Write header
    f.write("version: '3.8'\nservices:\n\n")

    # Generate service definition for each node
    for i in range(1, node_count + 1):
        f.write(f"  opensearch-node{i}:\n")
        f.write("    image: opensearchproject/opensearch:2.7.0  # Make sure this image is pulled\n")
        f.write(f"    container_name: os-node{i}\n")
        f.write("    environment:\n")
        f.write(f"      - node.name=os-node{i}\n")
        f.write("      - cluster.name=high-shard-cluster\n")
        f.write(f"      - discovery.seed_hosts={','.join(['os-node' + str(n) for n in range(1, node_count + 1)])}\n")
        f.write(f"      - cluster.initial_master_nodes={initial_master_nodes}\n")
        f.write("      - bootstrap.memory_lock=true  # Prevent swapping for performance\n")
        f.write("      - plugins.security.disabled=true  # Disable OpenSearch security plugin for simplicity\n")
        f.write(f"      - \"OPENSEARCH_JAVA_OPTS=-Xms{heap_size} -Xmx{heap_size}\"  # Set Java heap size\n")
        f.write("      - reindex.remote.whitelist=172.17.0.1:9200\n")
        f.write("    ulimits:\n")
        f.write("      memlock:\n")
        f.write("        soft: -1\n")
        f.write("        hard: -1\n")
        f.write("    volumes:\n")
        f.write(f"      - os-data{i}:/usr/share/opensearch/data  # Persist node data\n")
        f.write("    ports:\n")
        f.write(f"      - {http_start_port + i - 1}:9200  # Expose HTTP API for each node\n")
        f.write(f"      - {transport_start_port + i - 1}:9300  # Expose transport port\n\n")

    # Define persistent named volumes
    f.write("volumes:\n")
    for i in range(1, node_count + 1):
        f.write(f"  os-data{i}:\n")
