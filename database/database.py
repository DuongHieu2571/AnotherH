from re import template
import re
import pymysql
import time
from string import Template

from se import ConfigSingleStore
from helpers.logging import Logger
from data_model.model import FaceFeatureAdd, FaceFeatureEdit, FaceFeatureSearchIn, FaceFeatureSearchOut, FaceFeatureUpdate


class Database:
    def __init__(self, host: str, port: str, user: str, password: str, database: str):
        self.host = host 
        self.port = port 
        self.user = user 
        self.password = password 
        self.database = database 
        
        self.ssdb_conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=True,
        )
        
        self.conn.begin()
        cursor = self.conn.cursor()
        # cursor.execute('set global max_allowed_packet=671088864')
        cursor.execute('set global max_allowed_packet=1073741824')
        cursor.close()
        self.conn.commit() 

    @property
    def conn(self):
        if not self.ssdb_conn.open:
            Logger.info("Going to reconnect")
            self.ssdb_conn.ping(reconnect=True)
        return self.ssdb_conn

    def add_with_face_features(self, ffa: FaceFeatureAdd):
        self.conn.begin()
        cursor = self.conn.cursor()
        # for info
        sql = f"""INSERT INTO person (person_id, msnv, username, fullName, group_id, createDate, lastModified)
            VALUES ('{ffa.person_id}', '{ffa.msnv}', '{ffa.username}', '{ffa.fullName}', '{ffa.group_id}', '{ffa.createDate}', '{ffa.lastModified}')"""        
        cursor.execute(sql)

        # for feature
        sql = """INSERT INTO face_feature 
            (image_id, feature_vec, is_mask, path_feature, path_image, bucket, timestamp, person_id, group_id) 
            VALUES """
        template = Template(f"""('$image_id', UNHEX('$feature_vec'), '$is_mask', '$path_feature', '$path_image', 
                                '$bucket', '$timestamp', '{ffa.person_id}', '{ffa.group_id}')""")
        if ffa.list_feature:
            for ffwp in ffa.list_feature:
                val_str = template.substitute(**ffwp.dict())
                sql += val_str + ","
            # complete the syntax
            sql = sql[:-1] + ";"
            cursor.execute(sql)

        cursor.close()
        self.conn.commit() 
        return 0

    # def edit_with_face_features(self, ffe:FaceFeatureEdit):
    #     # get current ids
    #     sql = f"""SELECT id FROM face_feature WHERE image_id={ffe.image_id}"""
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         ids = cursor.fetchall()
    #         Logger.info(f"ids to edit: {ids}")
    #     # then update them with new features
    #     updated_count = 0
    #     for id, feat in zip(ids, ffe.list_feature):
    #         sql = f"""UPDATE face_feature SET feature_vector=UNHEX('{feat.feature_vec}'), is_mask={feat.is_mask} WHERE id={id}"""
    #         with self.conn.cursor() as cursor:
    #             updated_count += cursor.execute(sql)
    #     return updated_count

    def update_and_add_face_features(self, ffu:FaceFeatureUpdate):
        self.conn.begin()
        cursor = self.conn.cursor()

        sql_p = f"""UPDATE person SET lastModified='{ffu.lastModified}' 
                WHERE person_id='{ffu.person_id}' """
        cursor.execute(sql_p)

        # for feature
        sql = """INSERT INTO face_feature 
            (image_id, feature_vec, is_mask, path_feature, path_image, bucket, timestamp, person_id, group_id) 
            VALUES """
        template = Template(f"""('$image_id', UNHEX('$feature_vec'), '$is_mask', '$path_feature', '$path_image', 
                                '$bucket', '$timestamp', '{ffu.person_id}', '{ffu.group_id}')""")
        if ffu.list_feature:
            for ffwp in ffu.list_feature:
                val_str = template.substitute(**ffwp.dict())
                sql += val_str + ","
            # complete the syntax
            sql = sql[:-1] + ";"
            cursor.execute(sql)

        cursor.close()
        self.conn.commit() 
        return 0

    def get_group_id(self, person_id):
        # get person
        sql_p = f"""SELECT group_id FROM person WHERE person_id='{person_id}'"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_p)
            group_id = cursor.fetchone()
            if group_id:
                group_id = group_id[0]
            return group_id
        
    def check_person_existed(self, person_id):
        is_existed = False
        sql = (f"""SELECT * FROM person p WHERE p.person_id = '{person_id}'""")
        with self.conn.cursor() as cursor:
            nrows = cursor.execute(sql)
            # db_res = cursor.fetchone()
            if nrows > 0:
                is_existed = True
        return is_existed
        
    def edit_with_face_features(self, ffe:FaceFeatureEdit):
        # transaction mode to deal with more than 1 tables
        self.conn.begin()
        cursor = self.conn.cursor()
        # delete
        if ffe.list_remove_image_id:
            self.remove_image_ids(ffe.list_remove_image_id)

        is_existed = self.check_person_existed(person_id=ffe.person_id)
        if is_existed:
            # update
            current_group_id = self.get_group_id(person_id=ffe.group_id)
            sql_p = f"""UPDATE person SET msnv='{ffe.msnv}', username='{ffe.username}', fullName='{ffe.fullName}', group_id='{ffe.group_id}', 
                    createDate='{ffe.createDate}', lastModified='{ffe.lastModified}' 
                    WHERE person_id='{ffe.person_id}' """
            cursor.execute(sql_p)
            
            if current_group_id != ffe.group_id:
                sql_p = f"""UPDATE face_feature SET group_id='{ffe.group_id}' WHERE person_id='{ffe.person_id}' """
                cursor.execute(sql_p)   
        else:
            # for info
            sql = f"""INSERT INTO person (person_id, msnv, username, fullName, group_id, createDate, lastModified)
                VALUES ('{ffe.person_id}', '{ffe.msnv}', '{ffe.username}', '{ffe.fullName}', '{ffe.group_id}', '{ffe.createDate}', '{ffe.lastModified}')"""        
            cursor.execute(sql)
            
        # for feature
        if ffe.list_feature:
            sql = """INSERT INTO face_feature 
                (image_id, feature_vec, is_mask, path_feature, path_image, bucket, timestamp, person_id, group_id) 
                VALUES """
            template = Template(f"""('$image_id', UNHEX('$feature_vec'), '$is_mask', '$path_feature', '$path_image', 
                                    '$bucket', '$timestamp', '{ffe.person_id}', '{ffe.group_id}')""")
            for ffwp in ffe.list_feature:
                val_str = template.substitute(**ffwp.dict())
                sql += val_str + ","
            # complete the syntax
            sql = sql[:-1] + ";"
            cursor.execute(sql)
        
        # finish
        cursor.close()
        self.conn.commit()        
        return 0
         
    def edit_info(self, ffe:FaceFeatureEdit):
        self.conn.begin()
        cursor = self.conn.cursor()
        sql = f"""UPDATE person 
                SET msnv='{ffe.msnv}', username='{ffe.username}', fullName='{ffe.fullName}', 
                    group_id='{ffe.group_id}', lastModified='{ffe.lastModified}'                    
                WHERE person_id='{ffe.person_id}'"""
            
        res = cursor.execute(sql)
        # finish
        cursor.close()
        self.conn.commit()  
        return res

    def remove_image_ids(self, image_ids: list[str]):
        self.conn.begin()
        cursor = self.conn.cursor()
        sql = f"""DELETE FROM face_feature WHERE image_id IN (%s)"""
        params = [s for s in image_ids]
        
        res = cursor.execute(sql, params)
        # finish
        cursor.close()
        self.conn.commit()  
        return res

    def remove_person_ids(self, person_ids: list[str]):        
        # transaction mode to deal with 2 tables
        self.conn.begin()
        sql_f = f"""DELETE FROM face_feature WHERE person_id IN (%s)"""
        params_f = [s for s in person_ids]
        # Logger.info(f"params_f: {params_f}")
        sql_p = f"""DELETE FROM person WHERE person_id IN (%s)"""
        params_p = params_f

        with self.conn.cursor() as cursor:
            cursor.execute(sql_f, params_f)
            cursor.execute(sql_p, params_p)
        cursor.close()
        res = self.conn.commit()
        return res
    
    def remove_all_in_group(self, group_id: str):        
        # transaction mode to deal with 2 tables
        self.conn.begin()
        sql_f = f"""DELETE FROM face_feature WHERE group _id = '{group_id}'"""
        # Logger.info(f"params_f: {params_f}")
        sql_p = f"""DELETE FROM person WHERE group_id = '{group_id}'"""

        with self.conn.cursor() as cursor:
            cursor.execute(sql_f)
            cursor.execute(sql_p)
        cursor.close()
        res = self.conn.commit()
        return res

    def search_by_face_features(self, ffs: FaceFeatureSearchIn) -> list[FaceFeatureSearchOut]:
        search_con = pymysql.connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password,
                                    database=self.database,
                                    autocommit=True,
                                    )
        if not search_con.open:
            Logger.info("Going to reconnect")
            search_con.ping(reconnect=True)
        cursor = search_con.cursor()
        cursor.execute('set global max_allowed_packet=1073741824')
        # template = Template(f"""SELECT f.person_id, p.username, p.fullName, DOT_PRODUCT(feature_vec, UNHEX('$feature_vec')) as similarity 
        #     FROM face_feature f
        #     JOIN person p ON f.person_id=p.person_id
        #     WHERE f.is_mask=$is_mask AND f.group_id IN ({str(ffs.group_ids)[1:-1]})
        #     ORDER BY similarity DESC
        #     LIMIT {ffs.top_k}""")
        template = Template(f"""SELECT f.person_id, p.username, p.fullName, f.group_id, DOT_PRODUCT(feature_vec, UNHEX('$feature_vec')) as similarity 
            FROM face_feature f
            JOIN person p ON f.person_id=p.person_id
            WHERE f.group_id IN ({str(ffs.group_ids)[1:-1]})
            ORDER BY similarity DESC
            LIMIT {ffs.top_k}""")
        results = []
        for feat in ffs.list_feature:
            sql = template.substitute(feature_vec=feat.feature_vec, is_mask=feat.is_mask)
            # Logger.info(f"search_face_features: {sql}")
            with search_con.cursor(pymysql.cursors.DictCursor) as cursor:
                nrows = cursor.execute(sql)
                db_res = cursor.fetchall()
                person_ids = []
                similarities = []
                usernames = []
                fullNames = []
                group_ids = []
                for r in db_res:
                    person_ids.append(r['person_id'])
                    similarities.append(r['similarity'])
                    usernames.append(r['username'])
                    fullNames.append(r['fullName'])
                    group_ids.append(r['group_id'])
                
                feat_res = FaceFeatureSearchOut(person_ids=person_ids, similarities=similarities, 
                                                usernames=usernames, fullNames=fullNames, group_ids=group_ids)
                results.append(feat_res)
                cursor.close()
        return results

    def count_by_group_id(self, group_id:str) -> int:
        sql = (f"""SELECT p.username FROM person p WHERE p.group_id = '{group_id}'""")
        with self.conn.cursor() as cursor:
            nrows = cursor.execute(sql)
            # db_res = cursor.fetchall()
            return nrows

    def get_max_customer_by_group_id(self, group_id:str) -> int:
        max_id = 0
        sql = (f"""SELECT p.username FROM person p WHERE p.group_id = '{group_id}'""")
        with self.conn.cursor() as cursor:
            nrows = cursor.execute(sql)
            db_res = cursor.fetchall()
            if int(nrows) <= 0:
                return max_id
            for username in db_res:
                id = int(re.findall('\d+', username[0])[-1])
                # id = int(username[0].split("_")[-1])
                if id > max_id:
                    max_id = id
            return max_id

    def count_aisample(self, person_id):
        sql = (f"""SELECT f.image_id FROM face_feature f WHERE f.person_id = '{person_id}'""")
        with self.conn.cursor() as cursor:
            nrows = cursor.execute(sql)
            # db_res = cursor.fetchall()
            return nrows
        
    def get_person_by_username(self, username):
        # get person
        sql_p = f"""SELECT * FROM person WHERE username='{username}'"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_p)
            person = cursor.fetchone()
            return person
        
    def get_lastModified_by_person_id(self, person_id):
        # get lastModified
        sql_p = f"""SELECT lastModified FROM person WHERE person_id='{person_id}'"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_p)
            lastModified = cursor.fetchone()
            if lastModified:
                lastModified = lastModified[0]
            return lastModified
    
    def get_images_id_by_person_id(self, person_id) -> list:
        # get lastModified
        sql_p = f"""SELECT image_id FROM face_feature WHERE person_id='{person_id}'"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_p)
            images_id = cursor.fetchall()
            if images_id:
                images_id = list(images_id)
            else: images_id = []
            return images_id
        
    def get_features_by_person_id(self, person_id):
        sql = (f"""SELECT * FROM face_feature f WHERE f.person_id = '{person_id}'""")
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            features = cursor.fetchall()
            return features

    def check_if_existed(self,feature_vector,threshold):
        sql = f"""SELECT COUNT(f.person_id) as total 
            FROM face_feature f
            WHERE DOT_PRODUCT(f.feature_vec, UNHEX('{feature_vector}')) > {threshold} 
            """
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchone()
            return res[0]

    def add_to_face_features(self, person_id, msnv, username, fullName, group_id, createDate, lastModifield, feature_vector):
        self.conn.begin()
        cursor = self.conn.cursor()
        # for info
        sql = f"""INSERT INTO person (person_id, msnv, username, fullName, group_id, createDate, lastModified)
            VALUES ('{person_id}', '{msnv}', '{username}', '{fullName}', '{group_id}', '{createDate}', '{lastModifield}')"""        
        cursor.execute(sql)

        # for feature
        sql = f"""INSERT INTO face_feature 
            (image_id, feature_vec, is_mask, path_feature, path_image, bucket, timestamp, person_id, group_id) 
            VALUES ('{person_id}', UNHEX('{feature_vector}'), '0', '', '', 
                                '', '', '{person_id}', '{group_id}')"""
        cursor.execute(sql)
        cursor.close()
        self.conn.commit() 
        return 0


SingleStore = SingleStoreDAO(
    ConfigSingleStore.SINGLESTORE_HOST,
    ConfigSingleStore.SINGLESTORE_PORT,
    ConfigSingleStore.SINGLESTORE_USER,
    ConfigSingleStore.SINGLESTORE_PASS,
    ConfigSingleStore.SINGLESTORE_DBNAME
)
