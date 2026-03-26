import sqlite3

new_con = sqlite3.connect(".db")
new_cur = new_con.cursor()

new_cur.execute("DELETE FROM bibs WHERE 1=1")

res = new_cur.execute("SELECT id FROM bibs")
results = res.fetchall()
new_con.commit()
print(results)
