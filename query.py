import sqlite3

con = sqlite3.connect(".db")
cur = con.cursor()

# Delete all from tables
def del_rows():
    cur.execute("DELETE FROM bibs WHERE 1=1;")
    cur.execute("DELETE FROM editions WHERE 1=1;")
    con.commit()
    print("All records deleted.")

# Select count of table
def select_count():
    b_count = cur.execute("SELECT COUNT(*) from bibs;").fetchone()
    e_count = cur.execute("SELECT COUNT(*) from editions;").fetchone()
    em_count = cur.execute("SELECT COUNT(*) FROM embeddings").fetchone()
    print(f"bib count: {b_count}\neditions count: {e_count}")
    print(f"embeddings count: {em_count}")
    

def select():
    res = cur.execute("SELECT * FROM embeddings")
    return res.fetchone()

if __name__ == "__main__":
    select_count()
    # del_rows()