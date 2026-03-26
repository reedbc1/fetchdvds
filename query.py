import sqlite3

new_con = sqlite3.connect(".db")
new_cur = new_con.cursor()
res = new_cur.execute("SELECT id FROM bibs")
results = res.fetchall()
db_ids = {id[0] for id in results}
print(db_ids)