#!/usr/bin/env python
# coding: utf-8

# In[1]:


import config
import pandas as pd
import os
from sqlalchemy import create_engine
from io import BytesIO
import requests
from urllib.parse import urlencode
import urllib
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError
import json
from datetime import datetime, date, timedelta
import locale
from time import sleep
import shutil
import gc
import turbodbc
from turbodbc import connect
from pandas.api.types import is_string_dtype
import numpy as np
import pyodbc
import warnings



# In[2]:


host_mysql = config.host_mysql
port_mysql = config.port_mysql
user_mysql = config.db_mysql_login
password_mysql = config.db_mysql_pass

db_name = config.db_name

host_mssql = config.host_mssql
port_mssql = config.port_mssql
user_mssql = config.db_mssql_login
password_mssql = config.db_mssql_pass


# In[ ]:


def get_mssql_connection(db_name):
    try:
        engine = connect(driver="SQL Server", server=host_mssql, port=port_mssql, database=db_name)
        if engine:
            print('Все ок. Подключились!')
            return engine
    except:
        print('Что-то пошло не так')


# In[ ]:


# создаем функцию для удаления таблицы в БД
# если скрипт требует полной перезаписи данных, то сначала удаляем таблицу в БД с помощью этой функции
# а затем сохраняем таблицу с новыми данными

def dropTable(db_name, table_name):
    conn = get_mssql_connection(db_name)
    cursor = conn.cursor()

    sql = f"""IF EXISTS(SELECT *
              FROM   [dbo].{table_name})
      DROP TABLE [dbo].{table_name}"""
    
    cursor.execute(sql)
    conn.commit()
    conn.close()
    cursor.close() 
    
    print(f'Таблица: {table_name} успешно удалена в БД: {db_name}')
    print('#' * 10)


# In[ ]:


# создаем таблицы через Быструю загрузку и определяем тип данных для каждого поля 
# на входе наша функция принимает
# - название таблицы, под которым она будет записана в БД
# - список названий полей с типом данных 
# - тип таблицы (video / banner) - от этого зависит кол-во полей
# - флаг (create / drop) - создать таблицу с нуля / удалить старую таблицу и создать таблицу заново

def createDBTable(db_name, table_name, vars_list, flag='create'):
    # conn = get_mssql_connection(db_name)
    
    # cursor = conn.cursor()

    if flag=='drop':
        try:
            dropTable(db_name, table_name)
        except:
            print(f'Таблицы {table_name} не существует в БД {db_name}')
            
    conn = get_mssql_connection(db_name)
    cursor = conn.cursor()
        
    vars_string = ', '.join(str(elem) for elem in vars_list)
    
    try:
        sql =  f"""
             IF NOT EXISTS 
         (SELECT * FROM sysobjects 
         WHERE id = object_id(N'[dbo].[{table_name}]') AND 
         OBJECTPROPERTY(id, N'IsUserTable') = 1) 
         CREATE TABLE [dbo].[{table_name}] (
            {vars_string}
         )
    """
    
        cursor.execute(sql)
        conn.commit()

    
    except:
        print(f'Ошибка в файле {table_name}')
        print(exception)
    
    conn.close()
    cursor.close()    
    print(f'Пустая таблица {table_name} успешно создана в БД {db_name}')


# In[ ]:


# заливаем таблицы в БД
# функция на входе принимает датаФрейм с данными и название таблицы, в которую записать данные

def downloadTableToDB(db_name, table_name, df):
    conn = get_mssql_connection(db_name)
    cursor = conn.cursor()
        
    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}') 
    
    
    try:
    
        values = [np.ma.MaskedArray(df[col].values, pd.isnull(df[col].values)) for col in df.columns]
        colunas = '('
        colunas += ', '.join(df.columns)
        colunas += ')'
    
        val_place_holder = ['?' for col in df.columns]
        sql_val = '('
        sql_val += ', '.join(val_place_holder)
        sql_val += ')'
    
        sql = f"""
        INSERT INTO {table_name} {colunas}
        VALUES {sql_val}
        """
    
        cursor.executemanycolumns(sql, values)
        conn.commit()
    
        
    #         df.drop(df.index, inplace=True)
        print(f'Данные добавлены в БД: {db_name}, таблица: {table_name}')
    
    except:
        conn.close()
        cursor.close()
        print(f'Ошибка в файле {table_name}')
        print(exception)
        
    
    conn.close()
    cursor.close()    
        
         
    finish_time = datetime.now()
    print(f'Скрипт отработал {finish_time}')
    
    print(f'Время выполнения задачи: {finish_time - start_time}')
    print(f'Загрузка завершена. Данные успешно добавлены в БД: {db_name}')
    print('#' * 50)
    print()


# In[ ]:


def get_mssql_table(db_name, table_name):
    conn = get_mssql_connection(db_name)
    cursor = conn.cursor()
    
    query = f'SELECT * FROM {table_name}'
    
    warnings.simplefilter(action='ignore', category=UserWarning)
    df = pd.read_sql(query, conn)
     
    conn.close()
    
    print('Загрузка завершена успешно')
    return df


# In[ ]:


def get_mysql_connection(db_name):
    try:
        engine = create_engine(f'mysql+pymysql://{user_mysql}:{password_mysql}@{host_mysql}:{port_mysql}/{db_name}', echo=False)
        if engine:
            print('Все ок. Подключились!')
            return engine
    except:
        print('Что-то пошло не так')


# In[ ]:





# In[ ]:


def get_mysql_full_dict_table(db_name, table_name):
    engine = get_mysql_connection(db_name)
    
    df = pd.read_sql_query(f'SELECT * FROM {table_name}', engine)
    engine.dispose()
    print('Данные загружены')
    return df


# In[ ]:





# In[ ]:





# In[ ]:


# table_name = 'accounts'
# df = get_full_dict_table(db_name, table_name)


# In[ ]:


# get_mssql_connection(db_name)

