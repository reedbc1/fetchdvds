# imports
import sqlite3
import importlib.resources
import fetch_items
import logging
import asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

########################################################
# Initialize sqlite3
########################################################

def create_con():
    # conect to database
    con = sqlite3.connect(".db")
    cur = con.cursor()

    ext_path = importlib.resources.files("sqlite_vector.binaries") / "vector"

    con.enable_load_extension(True)
    con.load_extension(str(ext_path))
    con.enable_load_extension(False)
    
    return (con, cur)

########################################################
# Bibs
########################################################

def bibs(con, cur):
    # create table if it doesn't exist
    cur.execute("CREATE TABLE IF NOT EXISTS bibs(id, title, publicationDate, coverUrl, editionId)")

    # fetch bibs with API
    api_data, api_ids = asyncio.run(fetch_items.fetch_all_bibs())

    # generate diff with bib table (insert/delete/unchanged)
    res = cur.execute("SELECT id FROM bibs").fetchall()
    db_ids = {id[0] for id in res}

    to_insert = api_ids - db_ids
    to_delete = db_ids - api_ids
    unchanged = api_ids & db_ids

    # log diff
    logger.info("bibs diff:")
    logger.info(f"to_insert: {len(to_insert)}")
    logger.info(f"to_delete: {len(to_delete)}")
    logger.info(f"unchanged: {len(unchanged)}")

    # insert records
    records_to_insert = [record for record in api_data if record[0] in to_insert]
    cur.executemany("INSERT INTO bibs VALUES(?, ?, ?, ?, ?)", records_to_insert)
    con.commit()  # Remember to commit the transaction after executing INSERT.

    # delete records
    placeholders = ','.join(['?' for _ in to_delete])
    query = f"DELETE FROM bibs WHERE id IN ({placeholders})"
    ids_to_delete = [id for id in to_delete]

    cur.execute(query, ids_to_delete)
    con.commit()

########################################################
# Editions
########################################################

def editions(con, cur):
    # create table if it doesn't exist
    cur.execute("CREATE TABLE IF NOT EXISTS editions(id, author, itemLanguage, subjects, summary)")

    # use bib table to generate diff with editions table
    res = cur.execute("SELECT editionId FROM bibs").fetchall()
    bib_e_ids = {r[0] for r in res}

    res = cur.execute("SELECT id from editions").fetchall()
    edition_ids = {r[0] for r in res}

    to_insert = bib_e_ids - edition_ids
    to_delete = edition_ids - bib_e_ids
    unchanged = bib_e_ids & edition_ids

    # log diff
    logger.info("editions diff:")
    logger.info(f"to_insert: {len(to_insert)}")
    logger.info(f"to_delete: {len(to_delete)}")
    logger.info(f"unchanged: {len(unchanged)}")

    # delete records in editions table not in bib table
    placeholders = ','.join(['?' for _ in to_delete])
    query = f"DELETE FROM editions WHERE id IN ({placeholders})"
    ids_to_delete = [id for id in to_delete]

    cur.execute(query, ids_to_delete)
    con.commit()

    # fetch editions for new records and add to editions table
    full_editions = asyncio.run(fetch_items.fetch_all_editions(to_insert))

    # insert records
    cur.executemany("INSERT INTO editions VALUES(?, ?, ?, ?, ?)", full_editions)
    con.commit()

########################################################
# Sync
########################################################

def sync(con, cur):
    logger.info("starting sync...")
    logger.info("################################")
    bibs(con, cur)
    logger.info("################################")
    editions(con, cur)
    logger.info("################################")

    logger.info("sync complete.")

########################################################
# Joining Tables
########################################################

def join_tables(con, cur):

    # drop pre-existing table
    cur.execute("DROP TABLE IF EXISTS records")
    cur.execute("CREATE TABLE records(id, title, author, publicationDate, itemLanguage, subjects, summary, coverUrl)")
    
    # join bibs and editions on editionId = id
    cur.execute("INSERT INTO records SELECT b.id, b.title, e.author, b.publicationDate, e.itemLanguage, e.subjects, e.summary, b.coverUrl FROM bibs b INNER JOIN editions e ON e.id = b.editionId;")
    con.commit()

########################################################
# Embeddings
########################################################

# get text to put into embeddings model
def get_collection(con, cur):
    # selects records where id is not in embeddings

    query = """
    SELECT
        id,
        title,
        author,
        publicationDate,
        itemLanguage,
        subjects,
        summary
    FROM records r
    WHERE NOT EXISTS (
        SELECT 1
        FROM embeddings e
        WHERE e.id = r.id
    );
    """
    
    cur.execute(query)
    records = cur.fetchall()

    field_names = ["title: ", ", author: ", ", publication date: ", ", lanugage: ", ", subjects: ", ", summary: "]
    collection = []

    for r in records:
        id = r[0]
        text = ""

        for i in range(len(field_names)):
            text += field_names[i] + r[i+1]

        collection.append((id, text))

    return collection

# create embeddings for individual record
async def create_embedding(client, id, text):
    response = await client.embeddings.create(
        input=text,
        model="text-embedding-3-small"
    )
    embedding = response.data[0].embedding
    return (id, str(embedding))

# get embeddings for all records
async def get_embeddings():
    con, cur = create_con()
    col = get_collection(con, cur)
    client = AsyncOpenAI()
    coroutines = [create_embedding(client, *record) for record in col]
    embeddings = await tqdm.gather(*coroutines)
    return embeddings

# generate embeddings and put into table
def embeddings_table(con, cur, embeddings):
    cur.execute("DROP TABLE IF EXISTS embeddings")
    cur.execute("CREATE TABLE IF NOT EXISTS embeddings(id, embedding BLOB)")
    cur.executemany("INSERT INTO embeddings VALUES(?, vector_as_f32(?))", embeddings)
    con.commit()

# putting it all together
def sync_embeddings(con, cur):
    embeddings = asyncio.run(get_embeddings())
    embeddings_table(con, cur, embeddings)

########################################################
# Similarity Search
########################################################

### Searching Embeddings ###
# embed query search
# cosine similarity with query vector and embeddings
# return x nearest neighbors

if __name__ == "__main__":
    con, cur = create_con()
    sync(con, cur)
    sync_embeddings(con, cur)
