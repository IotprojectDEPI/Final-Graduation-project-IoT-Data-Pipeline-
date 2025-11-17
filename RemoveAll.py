import sqlite3

conn = sqlite3.connect("Readings.db")
cur = conn.cursor()

cur.execute("DELETE FROM Readings;")
conn.commit()
conn.close()
