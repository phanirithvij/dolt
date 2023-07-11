import mysql.connector
import sys
import random


def main():
    user = sys.argv[1]
    port = sys.argv[2]
    db = sys.argv[3]

    connection = mysql.connector.connect(user=user,
                                         host="127.0.0.1",
                                         port=port,
                                         database=db)

    cursor = connection.cursor()
    cursor.execute("select * from v left join ranges on (ranges.min <= v.a AND v.a < ranges.max)")
    print(len(cursor.fetchall()))
    cursor.close()
    connection.close()

    exit(0)


main()
