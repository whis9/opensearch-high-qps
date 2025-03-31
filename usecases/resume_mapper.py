# Description: This script processes educational groups (e.g., institutes), enriches them using alias/name variations,
# searches OpenSearch for relevant applicants, verifies them via MySQL, and maps them back into the database.

import threading
import queue
import random
import string
import requests
import mysql.connector
import logging
import urllib3
import os
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurable random port range for OpenSearch nodes
NODE_PORTS = list(range(9500, 9541))  # port range
INDEX_NAME = "high_shard_index_40"
PROCESSED_FILE = "processed_groups.txt"

# Logging setup
logging.basicConfig(
    filename="group_mapping.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# DB config (NOTE: Replace with your own credentials securely)
db_config = {
    "host": "localhost",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database"
}

group_queue = queue.Queue()
insert_lock = threading.Lock()
opensearch_semaphore = threading.BoundedSemaphore(20)  # Limit to 20 concurrent requests

# Load processed groups
if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, "r") as f:
        processed_groups = set(int(line.strip()) for line in f if line.strip().isdigit())
else:
    processed_groups = set()

def get_opensearch_url():
    """Return a random OpenSearch node URL."""
    return f"http://localhost:{random.choice(NODE_PORTS)}/"

def generate_variations(alias):
    """Clean and normalize alias to generate search-friendly variations."""
    variations = set()
    lowered = alias.lower().strip()
    cleaned = lowered.translate(str.maketrans("", "", string.punctuation)).strip()
    variations.add(lowered)
    variations.add(cleaned)
    return variations

def fetch_aliases(cursor, groupid):
    """Get all aliases and the main name for the group."""
    cursor.execute("SELECT alias_name FROM aliases WHERE inst_master_id = %s", (groupid,))
    aliases = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT name FROM school WHERE id = %s", (groupid,))
    institute_name = cursor.fetchone()
    if institute_name:
        aliases.append(institute_name[0])
    return aliases

def search_batch(batch, max_retries=5):
    """Search OpenSearch for a batch of variations with retries."""
    url = get_opensearch_url()
    should_clauses = [{"match_phrase": {"resume": v}} for v in batch]
    query = {
        "size": 2000,
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        },
        "_source": ["candidateid"]
    }

    # Initial search with retries
    for attempt in range(max_retries):
        try:
            res = requests.post(f"{url}{INDEX_NAME}/_search?scroll=5m", json=query, timeout=500)
            res.raise_for_status()  # Raises an exception for 4xx/5xx errors
            break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, etc.
                logging.warning(f"Error on initial search: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retries reached for initial search on batch: {batch}")
                return []  # Return empty list if all retries fail

    result = res.json()
    scroll_id = result.get("_scroll_id")
    hits = result.get("hits", {}).get("hits", [])
    candidateids = set(hit["_source"]["candidateid"] for hit in hits if "candidateid" in hit["_source"])

    # Scroll through remaining results with retries
    while hits:
        for attempt in range(max_retries):
            try:
                scroll_res = requests.post(
                    f"{url}_search/scroll",
                    json={"scroll": "5m", "scroll_id": scroll_id},
                    timeout=500
                )
                scroll_res.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logging.warning(f"Error during scroll: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Max retries reached during scroll for batch: {batch}")
                    return list(candidateids)  # Return what we’ve collected so far

        scroll_data = scroll_res.json()
        hits = scroll_data.get("hits", {}).get("hits", [])
        if not hits:
            break
        scroll_id = scroll_data.get("_scroll_id")
        candidateids.update(hit["_source"]["candidateid"] for hit in hits if "candidateid" in hit["_source"])

    return list(candidateids)

def verify_candidateids(cursor, candidateids):
    """Verify candidate IDs in batches."""
    verified = set()
    batch_size = 5000
    for i in range(0, len(candidateids), batch_size):
        batch = candidateids[i:i + batch_size]
        format_str = ",".join(["%s"] * len(batch))
        query = f"SELECT id FROM applicants WHERE id IN ({format_str}) AND is_verified=1"
        cursor.execute(query, tuple(batch))
        verified.update(row[0] for row in cursor.fetchall())
    return list(verified)

def insert_group_members(cursor, groupid, verified_ids):
    """Insert verified group members into the database."""
    values = [(candidateid, groupid) for candidateid in verified_ids]
    cursor.executemany("INSERT IGNORE INTO group_members (candidateid, groupid) VALUES (%s, %s)", values)

def mark_processed(groupid):
    """Mark a group as processed."""
    with insert_lock:
        with open(PROCESSED_FILE, "a") as f:
            f.write(f"{groupid}\n")

def process_group():
    """Process a group from the queue."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    while True:
        try:
            groupid = group_queue.get(timeout=5)
        except queue.Empty:
            break

        if groupid in processed_groups:
            group_queue.task_done()
            continue

        logging.info(f"Processing groupid: {groupid}")
        aliases = fetch_aliases(cursor, groupid)
        variations = set()
        for alias in aliases:
            if alias:
                variations.update(generate_variations(alias))

        logging.info(f"Found {len(aliases)} aliases and name(s)")
        logging.info(f"Created {len(variations)} variations")

        # Split variations into batches and search
        candidateids = set()
        batch_size = 50
        variations_list = list(variations)
        for i in range(0, len(variations_list), batch_size):
            batch = variations_list[i:i + batch_size]
            logging.info(f"Executing OpenSearch query with {len(batch)} conditions...")
            with opensearch_semaphore:
                batch_candidateids = search_batch(batch)
                candidateids.update(batch_candidateids)

        logging.info(f"Found {len(candidateids)} matching candidateids")
        verified_ids = verify_candidateids(cursor, list(candidateids))
        logging.info(f"{len(verified_ids)} verified results for groupid = {groupid}")

        insert_group_members(cursor, groupid, verified_ids)
        conn.commit()
        logging.info(f"Inserted verified results for groupid = {groupid} into table")
        mark_processed(groupid)

        group_queue.task_done()

    cursor.close()
    conn.close()

def main():
    """Main function to start processing."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM school")
    groupids = [row[0] for row in cursor.fetchall() if row[0] not in processed_groups]
    cursor.close()
    conn.close()

    for gid in groupids:
        group_queue.put(gid)

    threads = []
    for _ in range(100):
        t = threading.Thread(target=process_group)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
