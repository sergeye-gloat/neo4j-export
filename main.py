from neo4j import GraphDatabase
import json
from dotenv import load_dotenv
import os
import snowflake.connector
import tempfile
from neo4j.exceptions import ServiceUnavailable, SessionExpired
import time
import logging



logging.basicConfig(level=logging.INFO , format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)



BATCH_SIZE = 1000
load_dotenv(verbose=True)
neo4j_password = os.getenv('NEO4J_PASSWORD')
if not neo4j_password:
    raise ValueError("NEO4J_PASSWORD environment variable is not set.")
neo4j_url = "neo4j+s://fe95208c.databases.neo4j.io:7687"



def ingest_via_stage(data):
    retries = 0
    max_retries = 3
    retry_delay = 5  
    while retries < max_retries:
        try:
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
            )
            break
        except Exception as e:
            logger.info(f"Error connecting to Snowflake: {e}")
            retries += 1
            if retries >= max_retries:
                raise
            logger.info(f"Retrying connection ({retries}/{max_retries}) in {retry_delay} seconds...")
            time.sleep(retry_delay)
        
    cursor = conn.cursor()
    cursor.execute(f'USE WAREHOUSE  TRANSFORMING_DEV_WH ')
    cursor.execute(f'USE DATABASE  UNIFIED_EVENTS_STAGING ')
    cursor.execute(f'USE SCHEMA  EVENTS ')

    with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')  
        temp_filename = f.name

    try:
        cursor.execute("CREATE STAGE IF NOT EXISTS UNIFIED_EVENTS_DEV.EVENTS.my_json_stage")
        
        cursor.execute(f"PUT file://{temp_filename} @UNIFIED_EVENTS_DEV.EVENTS.my_json_stage AUTO_COMPRESS=TRUE")
        
        cursor.execute("""
            COPY INTO UNIFIED_EVENTS_DEV.EVENTS.neo4j_nodes (data)
            FROM @my_json_stage
            FILES = ('{}')
            FILE_FORMAT = (TYPE = 'JSON')
        """.format(os.path.basename(temp_filename) + '.gz'))
    finally:
        os.unlink(temp_filename)  # Clea
    cursor.close()
    conn.close()


# def write_to_snowflake_multiline(data):
#     # Connect to Snowflake using .env values
#     conn = snowflake.connector.connect(
#         user=os.getenv('SNOWFLAKE_USER'),
#         password=os.getenv('SNOWFLAKE_PASSWORD'),
#         account=os.getenv('SNOWFLAKE_ACCOUNT'),
#         warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
#         database=os.getenv('SNOWFLAKE_DATABASE'),
#         schema=os.getenv('SNOWFLAKE_SCHEMA'),
#     )
#     cursor = conn.cursor()
#     cursor.execute(f'USE WAREHOUSE  LOADING_WH ')

#     values_sql = ", ".join([f"(PARSE_JSON('{json.dumps(record)}'))" for record in data])
#     sql = f"INSERT INTO UNIFIED_EVENTS_DEV.EVENTS.neo4j_nodes(data) VALUES {values_sql}"
#     cursor.execute(sql)
#     print(f"Inserted {len(data)} records into Snowflake")
#     cursor.close()
#     conn.close()    
    
def write_to_snowflake(data):
    # Connect to Snowflake using .env values
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    cursor = conn.cursor()
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    cursor.execute(f'USE WAREHOUSE  LOADING_WH  ')

    # Optional: Create table if not exists
    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS company_terms (
    #     data VARIANT
    # )
    # """)

    # for record in data:
    #     json_str = json.dumps(record)
    #     cursor.execute(
    #         "INSERT INTO UNIFIED_EVENTS_DEV.EVENTS.neo4j_nodes(data) VALUES (PARSE_JSON(%s))",
    #         (json_str,)
    #     )
    for record in data:
        logger.debug(f"Processing record: {record}")
        json_str = json.dumps(record)
        cursor.execute(
            "INSERT INTO UNIFIED_EVENTS_DEV.EVENTS.neo4j_nodes(data) SELECT PARSE_JSON(%s)",
            (json_str,)
        )


    logger.info(f"Inserted {len(data)} records into Snowflake")


    cursor.close()
    conn.close()


def fetch_batch(session, skip: int, limit: int):
    query = """
    MATCH (n:CompanyTag)
    WHERE id(n) > -1
    RETURN n
    ORDER BY id(n)
    SKIP $skip LIMIT $limit
    """

    retries = 0
    max_retries = 3
    retry_delay = 5  
    while retries < max_retries:
        try:
            result = session.run(query, skip=skip, limit=limit)
            break
        except (ServiceUnavailable, SessionExpired, OSError) as e:
            logger.info(f"Error fetching batch: {e}")
            retries += 1
            if retries >= max_retries:
                raise
            logger.info(f"Connection error: {e}. Retrying ({retries}/{max_retries}) in {retry_delay} seconds...")
            time.sleep(retry_delay)

    def convert(val):
        if hasattr(val, "iso_format"):
            return val.iso_format()
        return val

    batch = []
    for record in result:
        node = record["n"]
        properties = {k: convert(v) for k, v in node.items()}
        enriched_node = {
            "element_id": node.element_id,
            "labels": list(node.labels),
            **properties
        }
        batch.append(enriched_node)

    return batch

def main():
    logger.info("Starting  neo4j-snapshoting!")
    print(os.getenv('NEO4J_PASSWORD'))
    driver = GraphDatabase.driver(neo4j_url, auth=("neo4j", os.getenv('NEO4J_PASSWORD')))
    logger.info(f"Driver created {driver}")
    with driver.session() as session:
        logger.info("Session created")
        skip = 0
        all_data = []
        while True:
            batch = fetch_batch(session, skip=skip, limit=BATCH_SIZE)
            if not batch:
                break
            all_data.extend(batch)
            logger.info(f"Fetched {len(batch)} records, total so far: {len(all_data)}")
            ingest_via_stage(batch)
            skip += BATCH_SIZE

        logger.debug(json.dumps(all_data, indent=2))

if __name__ == "__main__":
    main()
