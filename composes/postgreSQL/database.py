import psycopg2
from psycopg2 import IntegrityError
from tqdm import tqdm

class MyPostgreSQL:
    def db_connect(self, db, user, password, host='localhost'):
        self.connection = None
        try:
            self.connection = psycopg2.connect(
                                database = db,
                                user = user,
                                password = password,
                                host = host
                            )
            self.connection.autocommit = True
            print("Connect successful")
        except:
            print("Error:")

    def close(self):
        self.connection.close()
        print('hehe')
    
    # def do_query(self, query):
    #     cursor = self.connection.cursor()
    #     cursor.execute(query)
    #     rows = cursor.fetchall()
    #     for row in rows:
    #         print(row)
    #     cursor.close()

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
        
    def insert(self, table=None,  values=None, column = None):
        try:
            if column != None:
                query = f'INSERT INTO {table} ('
                for item in column:
                    query += "\""+item+"\","
                query = query[:len(query)-1] + ')'
                query = f'{query} VALUES {values}'
            else:
                query = f'INSERT INTO {table} VALUES {values};'
            # print(query)
            cursor = self.connection.cursor()
            cursor.execute(query)   
            self.connection.commit()
            cursor.close()
        except IntegrityError:
            return

    def update(self, table,  set, condition):
        query = f"UPDATE {table} SET {set} WHERE {condition};"
        # print(query)
        cursor = self.connection.cursor()
        cursor.execute(query)   
        self.connection.commit()
        cursor.close()

if __name__ == '__main__':
    mydb = MyPostgreSQL()
    mydb.db_connect(db='lake_dev', user='postgres_user', password='123456aA@', host='localhost')

    # # # check if exist si_id = 1 in database
    # print(mydb.check_exists(table='event', condition=f"si_id = '1'"))

    # # query table si

    # string1 = "[\"Asian\", \"Female\", \"20-29\"]"
    # print(string1)
    # string2 = "[\"di\", \"di\", \"di\", \"di\", \"li\"]"
    # mydb.insert(
    #     table = "event",
    #     column = ("si_id", "groupp", "user_id", "source_id", "object_id", "bbox", 
    #               "confidence", "image_path", "time_stamp", "fair_face", "person_id", "dict"),
    #     values = ("string", "string", "string", "test", "123", "[1077, 180, 1323, 513]", 
    #               0.8105192184448242, "http://minio:9000/vss/s3%3A//vss/face/1682048755_test_0.jpg?AWSAccessKeyId=admin&Signature=Vo3i6U%2FCbSxoR6KzCJWf2VMRMSI%3D&Expires=1683609146", 
    #               "1683605546171", string1, string2, "[0.8942996263504028, 0.9111446738243103, 0.9411799907684326, 0.9483562707901001, 0.9508835077285767]")
    # )
    
    

    # list_x = ['dsfss', "ssdsfsfsd", 'sdsfsfsf']
    # def  list_to_stringlist(list):
    #     string_list = "["
    #     for i, x in enumerate(list):
    #         print("string_list: ", string_list)
    #         print("x: ", x)
    #         string_list += f"\"{x}\""
    #         if i == len(list)-1: 
    #             break
    #         string_list += ", "
    #     string_list += "]"
    #     return string_list
    
    # string_list = list_to_stringlist(list=list_x)
    # print(string_list)
    import pandas as pd

    df = pd.read_csv('/media/dh/VHT/ETL_worker/output_face_data.csv')
    df['timestamp']=df['timestamp'].astype(str)
    df['object_id']=df['object_id'].astype(str)
    df['confidence']=df['confidence'].astype(str)

    # with mydb.connection.cursor() as cur:
    #     cur.execute('delete from event')
    #     print('delete done!')

    # insert_record = "INSERT INTO event \
    # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    # for i in tqdm(df.index):
    #     insert_value = tuple(df.iloc[i].to_list())

    #     with mydb.connection.cursor() as cur:
    #         cur.execute(insert_record, insert_value)

    with mydb.connection.cursor() as cur:
        cur.execute("select si_id, source_id from event where object_id = '0'")
        rows = cur.fetchall()
        for row in rows:
            print(row)
  
    mydb.close()
