import mysql.connector
import sys
import random

RANGES_SIZE = 8
VALUES_SIZE = int(sys.argv[4])

def main():
    user = sys.argv[1]
    port = sys.argv[2]
    db = sys.argv[3]

    connection = mysql.connector.connect(user=user,
                                         host="127.0.0.1",
                                         port=port,
                                         database=db)

    cursor = connection.cursor()
    cursor.execute("drop table if exists ranges;")
    cursor.execute("drop table if exists v;")
    cursor.execute("create table ranges (min int, max int, index (min), index (max));")
    for i in range(RANGES_SIZE):
        cursor.execute("insert into ranges values (%s, %s)", (100*i, 100*(i+1)))
    cursor.close()

    cursor = connection.cursor()
    cursor.execute("create table v (a int, index (a))")
    cursor.close()

    for i in range(VALUES_SIZE):
        cursor = connection.cursor()
        value = random.randint(1, RANGES_SIZE*100)
        cursor.execute("insert into v values (%s)", (value,))
        cursor.close()

    connection.commit()
    connection.close()

    exit(0)


main()
