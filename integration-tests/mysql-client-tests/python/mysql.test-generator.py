import time
import datetime

import mysql.connector
import sys
import random

RANGES_SIZE = 8
VALUES_SIZE = int(sys.argv[4])

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def getRandomTimeStamp(start, end):
    start_epoch = int(time.mktime(time.strptime(start, TIME_FORMAT)))
    endEpoch = int(time.mktime(time.strptime(end, TIME_FORMAT)))
    randEpoch = random.randrange(start_epoch, endEpoch)

    return datetime.datetime.fromtimestamp(randEpoch).strftime()


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
    cursor.execute("create table ranges (min timestamp(6), max timestamp(6), index (min), index (max));")
    for i in range(RANGES_SIZE):
        cursor.execute("insert into ranges values (%s, %s)", (100*i, 100*(i+1)))
    cursor.close()

    cursor = connection.cursor()
    cursor.execute("create table v (a timestamp(6), index (a))")
    cursor.close()

    for i in range(VALUES_SIZE):
        cursor = connection.cursor()
        value = random.randint(1, RANGES_SIZE*100)
        cursor.execute("insert into v values (%s)", (value,))
        cursor.close()

    connection.commit()
    connection.close()

    exit(0)


if __name__ == "__main__":
    main()