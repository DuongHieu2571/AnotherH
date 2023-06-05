import psycopg2
from psycopg2 import IntegrityError, OperationalError
from util.db_config import get_db_info


class MyPostgreSQL:
    def __init__(self, filename, section):
        self.connection = None
        db_info = get_db_info(filename, section)
        try:
            self.connection = psycopg2.connect(**db_info)
            self.connection.autocommit = True
            print("Connect successful")
        except OperationalError:
            print("Error connecting to the database")

    def close(self):
        self.connection.close()
    
    def do_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()

    def check_exists(self, table, condition):
        query = f"SELECT 1 FROM {table} WHERE {condition} LIMIT 1;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return False
        else:
            cursor.close()
            return True
        
    def insert(self, table, values, column = None):
        try:
            if column != None:  # insert values by specific column
                query = f'INSERT INTO {table} {column} VALUES {values};'
            else:   # insert values to all columns
                query = f'INSERT INTO {table} VALUES {values};'
            print(query)
            cursor = self.connection.cursor()
            cursor.execute(query)   
            # self.connection.commit()
            cursor.close()
        except IntegrityError as e:
            return 'exist'

    def update(self, table,  set, condition):
        query = f"UPDATE {table} SET {set} WHERE {condition};"
        # print(query)
        cursor = self.connection.cursor()
        cursor.execute(query)   
        # self.connection.commit()
        cursor.close()

if __name__ == '__main__':
    import pandas as pd

    df = pd.read_csv('output_face_data.csv')
    df['timestamp']=df['timestamp'].astype(str)
    df['object_id']=df['object_id'].astype(str)
    df['confidence']=df['confidence'].astype(str)
    mydb = MyPostgreSQL(filename='config/db_info.ini', section='postgres-db')

    # check if exist si_id = 1 in database
    # print(mydb.check_exists(table='si', condition=f'si_id = {1}'))

    # # delete 6 from si
    # mydb.do_query('delete from si where si_id=6')
    
    insert_record = "INSERT INTO summary \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    insert_value = tuple(df.iloc[0].to_list())
    print(insert_value)

    with mydb.connection.cursor() as cur:
        cur.execute(insert_record, insert_value)

    # query table si
    mydb.do_query('select * from summary')

    # # # add 6 to si table
    # data = (6, 'e')
    # print(str(data))
    # if mydb.check_exists('si', f'si_id = {data[0]}'):
    #     print('exist')
    # else:
    #     mydb.insert('si', f"{str(data)}")

    # mydb.do_query('select * from si')
    # close connection to db
    mydb.close()