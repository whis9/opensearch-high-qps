# opensearch-high-qps
# High-QPS Search with Distributed OpenSearch

**Author:** Aarshit Mittal  
**Use Case:** Scalable fuzzy search across millions of documents using OpenSearch multinode architecture  
**Tech Stack:** OpenSearch, Docker, Python, MySQL, Scroll API, Multithreading

---

## Overview

This project showcases how to achieve high-QPS search using a **40-node OpenSearch cluster** paired with a **multithreaded Python pipeline**. The architecture is designed to support:

- 100s of `should` clauses per query
- 1M+ document searches
- 50k+ unique group/entity mappings
- Parallel OpenSearch + MySQL operations at scale

The core value is **distributing load across OpenSearch shards** efficiently to solve otherwise unscalable fuzzy search problems.

---

## Real Problem Solved

- You have **50,000 colleges**
- Each has **100–200 aliases**
- You want to **match 1 million resumes** to these colleges based on `resume text`

**Challenge**: That’s tens of millions of fuzzy match operations.

**Traditional approach**: Sequential queries or single-node OpenSearch = slow and inefficient

**Our solution**:
- Group 50 alias variations per `should` clause
- Fire them in **parallel (100 threads)**
- Each thread sends to a **random OpenSearch node** out of 40
- Results are verified and inserted in MySQL

---

## Project Structure

```
high-qps-opensearch-distributed/
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
├── docker/
│   └── generate_docker_compose.py
├── usecases/
│   └── resume_mapper.py
├── configs/
│   └── mapping.json
├── logs/
│   └── group_mapping.log
├── processed_groups.txt
└── docs/
    └── architecture.png
```

---

## Setup Instructions

### 1. Spin Up 40 OpenSearch Nodes

Run the Docker Compose generator:
```bash
python3 docker/generate_docker_compose.py
```
Then launch:
```bash
docker compose up -d
```

### 2. Create High-Shard Index

Use the `configs/mapping.json` file:
```bash
PUT /high_shard_index_40
{
  "settings": {
    "number_of_shards": 40,
    "number_of_replicas": 0
  }
}
```

### 3. Run Resume Mapping Script

```bash
python3 usecases/resume_mapper.py
```
Ensure your MySQL config is updated inside the script.

---

## Why This Works

- **One shard per node** = predictable load distribution
- **Random node access** = QPS load spread evenly
- **Should batching** = fewer requests, bigger payloads
- **Threading** = saturate CPU & network
- **Scroll API** = full document retrieval, not just top hits

---

## Performance Impact

- Mapped 1 million resumes to 50k colleges in **hours** (down from days)
- QPS stabilized across nodes, no bottlenecks or memory overuse
- Scales horizontally with more containers if needed

---

## Use Cases

- Resume enrichment
- Product-to-merchant linking
- Entity recognition and mapping
- High-cardinality text-based search

---

## License

This project is licensed under the MIT License. Credit to Aarshit Mittal.

