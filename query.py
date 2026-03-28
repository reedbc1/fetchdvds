import sqlite3

con = sqlite3.connect(".db")
cur = con.cursor()

# Delete all from tables
cur.execute("DELETE FROM bibs WHERE 1=1;")
cur.execute("DELETE FROM editions WHERE 1=1;")
con.commit()
print("All records deleted.")

# Select count of table
b_count = cur.execute("SELECT COUNT(*) from bibs;").fetchone()
e_count = cur.execute("SELECT COUNT(*) from editions;").fetchone()
print(f"bib count: {b_count}\neditions count: {e_count}")