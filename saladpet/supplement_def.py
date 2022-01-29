import pandas as pd 
import pymysql 
import pymysql.cursors
import numpy as np

df = pd.DataFrame()
df_update = {}
insert_column = ()
update_column = [] 

def sql_connection ():
    ssh_host = '3.36.21.112'
    ssh_port = '21'
    conn_resource = pymysql.connect(host='3.36.21.112', user='root', password='ckUdC17yqltr', db='resource')
    conn_testserver = pymysql.connect(host='3.36.21.112', user='root', password='ckUdC17yqltr', db='saladpet')

    cursor_resource = conn_resource.cursor() #dictionary 형태로 결과 반환 원해 -> pymysql.cursors.Dictcursor을 ()에 넣어줌.
    cursor_testserver = conn_testserver.cursor()


## df_to_sql 함수 : dataframe 형태를 sql에 upsert 할 수 있게 해줌 (단, 칼럼명이 sql의 칼럼명과 동일해야 함!)
def df_to_sql (df, table_name, primary_key) : # df = 넣을 dataframe, table_name = sql 테이블 이름, column = df와 동일한 순서의 칼럼, primary_key = PK or unique key로 on duplicate key 시에 사용하지 않는 것
    column = df.columns.values.tolist()
    insert_column = tuple(column)
    update_column = column
    for i in range (0,len(primary_key)):
        column.remove(update_column[i])
    word_1 = "insert into %s (" %table_name 
    word_2 = "on duplicate key update" 

    for l in range(0,len(update_column)):
        if l < len(update_column)-1 :
            word_2 = word_2 + " "+ update_column[l] + " = %s,"
        if l == len(update_column)-1 :
            word_2 = word_2 + " " + update_column[l] + " = %s"

    for s in range(0,len(insert_column)):
        if s < len(insert_column)-1 :
            word_1 = word_1 + " "+ insert_column[s] + ","
        if s == len(insert_column)-1 :
            word_1 = word_1 + " "+ insert_column[s] + " ) "
    word_1 = word_1 + "VALUES (" + "%s,"*(len(insert_column)-1) + "%s)"
    print (word_1 + word_2)

    for i in range (0,len(df)) :
        a = df.iloc[i] # index = i 인 df를 series로 변환 
        insert_data = a.to_dict() # series를 사전 형태로 변환 - insert 사전 목록 
        update_data = a.to_dict() 
        for key in primary_key:
            if key in update_data:
                del update_data[key] # series를 사전 형태로 변환 - update 사전 목록 
        data = list(insert_data.values()) + list(update_data.values()) #insert+update 목록 리스트 
        sql = word_1 + word_2
        cursor_resource.execute(sql,data)
        conn_resource.commit()
        i = i+1
    print ("finish insert %s" %table_name )
    print ("넣은 row 수 총 %s 개" %i ) 


def df_to_sql_saladpet (df, table_name, primary_key) : # df = 넣을 dataframe, table_name = sql 테이블 이름, column = df와 동일한 순서의 칼럼, primary_key = PK or unique key로 on duplicate key 시에 사용하지 않는 것
    column = df.columns.values.tolist()
    insert_column = tuple(column)
    update_column = column
    for i in range (0,len(primary_key)):
        column.remove(update_column[i])
    word_1 = "insert into %s (" %table_name 
    word_2 = "on duplicate key update" 

    for l in range(0,len(update_column)):
        if l < len(update_column)-1 :
            word_2 = word_2 + " "+ update_column[l] + " = %s,"
        if l == len(update_column)-1 :
            word_2 = word_2 + " " + update_column[l] + " = %s"

    for s in range(0,len(insert_column)):
        if s < len(insert_column)-1 :
            word_1 = word_1 + " "+ insert_column[s] + ","
        if s == len(insert_column)-1 :
            word_1 = word_1 + " "+ insert_column[s] + " ) "
    word_1 = word_1 + "VALUES (" + "%s,"*(len(insert_column)-1) + "%s)"
    print (word_1 + word_2)

    for i in range (0,len(df)) :
        a = df.iloc[i] # index = i 인 df를 series로 변환 
        insert_data = a.to_dict() # series를 사전 형태로 변환 - insert 사전 목록 
        update_data = a.to_dict() 
        for key in primary_key:
            if key in update_data:
                del update_data[key] # series를 사전 형태로 변환 - update 사전 목록 
        data = list(insert_data.values()) + list(update_data.values()) #insert+update 목록 리스트 
        sql = word_1 + word_2
        cursor_resource.execute(sql,data)
        conn_resource.commit()
        i = i+1
    print ("finish insert %s" %table_name )
    print ("넣은 row 수 총 %s 개" %i ) 
