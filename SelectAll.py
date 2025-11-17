import sqlite3

# Connect to the database
conn = sqlite3.connect("Readings.db")

# Create a cursor to execute SQL commands
cur = conn.cursor()

# Example: read all rows from a table
cur.execute("SELECT * FROM Readings")
rows = cur.fetchall()

for row in rows:
    print(row)

conn.close()
