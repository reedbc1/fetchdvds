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
    return (con, cur)
########################################################
# Bibs
########################################################
def bibs(con, cur):
    logger.info("syncing bibs table...")
    # fetch bibs with API
    api_data, api_ids = fetch_items.fetch_bibs()

    # generate diff with bib table (insert/delete/unchanged)
    res = cur.execute("SELECT id FROM bibs").fetchall()
    db_ids = {id[0] for id in res}

    num_rows = cur.execute("SELECT COUNT(*) FROM bibs").fetchone()[0]
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
    num_rows = cur.execute("SELECT COUNT(*) FROM bibs").fetchone()[0]
    expected_rows = len(unchanged) - len(to_delete) + len(to_insert)
    logger.info(f"Insert/delete operations complete. {num_rows} row now in db")
    logger.info(f"This should match # unchanged - # to delete + # to insert, which is {expected_rows}")

########################################################
# Editions
########################################################
def editions(con, cur):
    res = cur.execute("SELECT id FROM bibs").fetchall()
    db_ids = {id[0] for id in res}
    # use bib table to generate diff with editions table

    # delete records in editions table not in bib table

    # fetch editions for new records and add to editions table

if __name__ == "__main__":
    con, cur = create_con()
    bibs(con, cur)