import sqlite3

con = sqlite3.connect(".db")
cur = con.cursor()

result = cur.execute("SELECT COUNT(*) from editions;").fetchone()
print(result)