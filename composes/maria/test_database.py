import mariadb
import sys


# Connext to MariaDB 
conn = mariadb.connect(
    user="maria_user",
    password="123456aA@",
    host="localhost",
    port="3306",
    database="lake_dev"
)

cur = conn.cursor()