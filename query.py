import sqlite3

new_con = sqlite3.connect(".db")
new_cur = new_con.cursor()

new_cur.execute("DROP TABLE IF EXISTS bibs")
new_con.commit()

new_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")

tables = new_cur.fetchall()
for table in tables:
    print(table[0])

# new_cur.execute("DELETE FROM bibs WHERE 1=1")

# res = new_cur.execute("SELECT id FROM bibs")
# results = res.fetchall()
# new_con.commit()
# print(results)
