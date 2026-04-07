import sqlite3
import sync_db

con = sqlite3.connect("items.db")
cur = con.cursor()

# Delete all from tables
def del_rows():
    cur.execute("DELETE FROM bibs WHERE 1=1;")
    cur.execute("DELETE FROM editions WHERE 1=1;")
    cur.execute("DELETE FROM embeddings WHERE 1=1")
    con.commit()
    print("All records deleted.")

# Select count of table
def select_count():
    b_count = cur.execute("SELECT COUNT(*) from bibs;").fetchone()
    e_count = cur.execute("SELECT COUNT(*) from editions;").fetchone()
    r_count = cur.execute("SELECT COUNT(*) FROM records;").fetchone()
    em_count = cur.execute("SELECT COUNT(id) FROM embeddings").fetchone()
    print(f"bib count: {b_count}\neditions count: {e_count}")
    print(f"records count: {r_count}")
    print(f"embeddings count: {em_count}")

def select(table):
    res = cur.execute(f"SELECT COUNT(DISTINCT id) FROM {table} GROUP BY id HAVING COUNT(id)>1")
    return res.fetchall()

def distinct_tables(table_name: str):
    # cur.execute("CREATE TEMP TABLE temp_ids AS SELECT DISTINCT id FROM embeddings;")

    cur.execute(f"CREATE TABLE new_table as SELECT DISTINCT * FROM {table_name};")
    cur.execute(f"DROP TABLE {table_name};")

    cur.execute(f"CREATE TABLE {table_name} AS SELECT DISTINCT * FROM new_table;")
    cur.execute("DROP TABLE new_table;")
    
    con.commit()

def remove_dup_em():
    ids_to_remove = cur.execute("SELECT id, count(id) FROM embeddings group by id having count(id) > 1;").fetchall()
    print(ids_to_remove)
    list_to_remove = [item[0] for item in ids_to_remove]
    tup_to_remove = tuple(list_to_remove)
    print(tup_to_remove)
    cur.execute(f"DELETE FROM embeddings WHERE id IN {tup_to_remove};")
    con.commit()

if __name__ == "__main__":
    # print(select())
    # print(select())
    # del_rows()
    # distinct_tables("records")
   
    # remove_dup_em()
    select_count()
    