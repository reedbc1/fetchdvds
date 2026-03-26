# imports
import sqlite3
import fetch_items
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

########################################################
# Initialize sqlite3
########################################################

def create_con():
    # connect to database
    con = sqlite3.connect(".db")
    cur = con.cursor()

    # create tables if they don't exist
    cur.execute("CREATE TABLE IF NOT EXISTS bibs(id, title, publicationDate, coverUrl, editionId)")
    cur.execute("CREATE TABLE IF NOT EXISTS editions(id, author, itemLanguage, subjects, summary)")
    return (con, cur)

########################################################
# Bibs
########################################################
def bibs(con, cur):
    # fetch bibs with API
    api_data, api_ids = fetch_items.fetch_all_bibs()

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

    # log number of remaining rows
    # num_rows = cur.execute("SELECT COUNT(*) FROM bibs").fetchone()[0]
    # expected_rows = len(unchanged) - len(to_delete) + len(to_insert)
    # logger.info(f"Insert/delete operations complete. {num_rows} row now in db")
    # logger.info(f"This should match # unchanged - # to delete + # to insert, which is {expected_rows}")

########################################################
# Editions
########################################################
def editions(con, cur):
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
    full_editions = fetch_items.fetch_all_editions(to_insert)

    # insert records
    cur.executemany("INSERT INTO editions VALUES(?, ?, ?, ?, ?)", full_editions)
    con.commit()

    # log number of remaining rows
    # num_rows = cur.execute("SELECT COUNT(*) FROM bibs").fetchone()[0]
    # expected_rows = len(unchanged) - len(to_delete) + len(to_insert)
    # logger.info(f"Insert/delete operations complete. {num_rows} row now in db")
    # logger.info(f"This should match # unchanged - # to delete + # to insert, which is {expected_rows}")

def sync():
    logger.info("starting sync...")
    con, cur = create_con()
    logger.info("################")
    bibs(con, cur)
    logger.info("################")
    editions(con, cur)
    logger.info("################")

    logger.info("sync complete.")

if __name__ == "__main__":
    sync()