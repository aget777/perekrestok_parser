#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from db_funcs import *
from normalize_funcs import *
from perekrestok_mediaplan_parser import get_base_mediaplan, merge_source_type_id, merge_full_acc_id, get_end_of_week
import config
import numpy as np
from datetime import date
from datetime import datetime

db_name = config.db_name

# Факт по источникам для План-Факта
media_fact_link = config.media_fact_link


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)


# In[3]:


def main_reload_fact_table(media_fact_link):
    # функция забирает факт по УРЛ ссылке
    # приводит в поряддок названия полей, типы данных, добавляет НДС
    # и возвращает датаФрейм
    df = get_base_mediaplan(media_fact_link, report='fact')
    df = df.rename(columns={'Дата отчета': 'report_date', 'leads': 'convs', 'reaches': 'reach'})
    
     # забираем справочник Источников
    # добавляем ИД источников к Фактам
    df = merge_source_type_id(df)

    # забираем справочник Аккаунтов
    # добавляем ИД аккаунтов к Фактам
    df = merge_full_acc_id(df)

    # приводим даты к формату ДатаВремя
    df['date_start'] = df['date_start'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df['date_start'] = pd.to_datetime(df['date_start'])
    df['date_finish'] = df['date_finish'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df['date_finish'] = pd.to_datetime(df['date_finish'])
    df['report_date'] = df['report_date'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df['report_date'] = pd.to_datetime(df['report_date'])
    df['end_of_week'] = df['report_date'].apply(get_end_of_week)
    # считаем общее кол-во дней во Флайте
    df['days_in_flight'] = ((df['date_finish'] - df['date_start']).dt.days) + 1


     # пересоздаем пустую таблицу Справочников в БД
    media_fact_table = config.media_fact_table #'media_fact_table'
    # создаем общий список названий полей и типов данных 
    # этот список передаем в БД MSSQL для создания новой таблицы
    media_fact_table_vars_lst = config.media_fact_table_vars_lst
    createDBTable(db_name, media_fact_table, media_fact_table_vars_lst, flag='drop')

    # нормализуем типы данных
    media_fact_table_int_lst = config.media_fact_table_int_lst
    media_fact_table_float_lst = config.media_fact_table_float_lst
    df = normalize_columns_types(df, media_fact_table_int_lst, media_fact_table_float_lst)

    # записываем в БД MSSQL Факт
    downloadTableToDB(db_name, media_fact_table, df)
    # return df


# In[4]:


# main_reload_fact_table(media_fact_link)


# In[5]:


# df = main_reload_fact_table(media_fact_link)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




