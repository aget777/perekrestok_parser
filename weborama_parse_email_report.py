#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import os
from get_file_from_email import *
from db_funcs import *
from normalize_funcs import *
import config
import numpy as np
from datetime import date
from datetime import datetime
from perekrestok_mediaplan_parser import merge_source_type_id, merge_full_acc_id, get_end_of_week, get_report_date
# import perekrestok_mediaplan_parser

file_path = config.email_file_path
db_name = config.db_name

keywords_list = ['Weborama_Standart_Weekly']  #['X5_Perekrestok_Geo', 'Weborama_Standart_Weekly']



geo_file_name = 'X5_Perekrestok_Geo.xlsx'
weekly_file_name = 'Weborama_Standart_Weekly.xlsx'


# In[ ]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)


# In[ ]:


# функция для отпределения Типа РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Типа
# если вхождений нет, вернет others
def parse_type_category(row, flag='type_'):
    if flag in row:
        start_index = row.find(flag)
        cut_row = row[start_index+len(flag):]
        end_index = cut_row.find('|')
        return cut_row[:end_index] if end_index!=-1 else cut_row
    else:
        return 'other'


# In[ ]:





# In[ ]:


# функция для отпределения Продукта РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Продукта
# если вхождений нет, вернет others
def get_product_from_insertion(insertion):
    if '_cvp1_' in insertion:
        return 'cvp1'
    if '_cvp2_' in insertion:
        return 'cvp2'
    if '_ge_' in insertion:
        return 'ge'
    return 'other'


# In[ ]:


def get_merge_items(df):
    # парсим Тип РК
    df['type'] = df['insertion'].apply(parse_type_category, flag='type_')
    # парсим название Категории
    df['category'] = df['insertion'].apply(parse_type_category, flag='format_')
    # парсим название Продукта
    df['product'] = df['insertion'].apply(get_product_from_insertion)
    # парсим название флайта
    df['flight'] = df['insertion'].apply(lambda x: x.split('_igronik_media_')[1] if '_igronik_media_' in x else 'other')
    # создаем общее название РК
    df['weborama_camp_name'] = df['flight'] + '|format_' + df['category'] + '|type_' + df['type']
    
    # добавляем ИД источников к основному файлу
    df = merge_source_type_id(df)
    # забираем справочник Аккаунтов
    # добавляем ИД аккаунтов к Медиаплану
    df = merge_full_acc_id(df)

    df['source_type_id'] = df['source_type_id'].fillna(0)
    df['source_type_id'] = df['source_type_id'].astype('int64')
    
    
    df['main_acc_id'] = df['main_acc_id'].fillna(0)
    df['main_acc_id'] = df['main_acc_id'].astype('int64')
    
    # добавляем инфо из справочника Кампаний
    # дата начала и окончания флайта, внутриенний ИД РК и номер флайта
    weborama_camp_dict = config.weborama_camp_dict #'weborama_camp_dict'
    camp_dict_df = get_mssql_table(db_name, weborama_camp_dict)
    
    df = df.merge(camp_dict_df[['weborama_key_camp', 'inner_campaign_id', 'date_start', 'date_finish', 'flight_name']], 
                    how='left', left_on='weborama_key_camp', right_on='weborama_key_camp')
    

        # Если в ежедневных отчетах есть РК, которые не нашли сопоставляения со справочником, мы удаляем такие строки
    # df = df.dropna(subset='inner_campaign_id')
    df['inner_campaign_id'] = df['inner_campaign_id'].fillna(0)
    df['date_start'] = df['date_start'].fillna('01.01.2025')
    df['date_finish'] = df['date_finish'].fillna('01.01.2025')
    df['flight_name'] = df['flight_name'].fillna('no_name')
    
    # df[~df['inner_campaign_id'].isna()]
    # приводим даты к формату ДатаВремя
    df['date_start'] = df['date_start'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df['date_start'] = pd.to_datetime(df['date_start'])
    df['date_finish'] = df['date_finish'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df['date_finish'] = pd.to_datetime(df['date_finish'])
    
    # приводим к формату Дата
    df['date'] = pd.to_datetime(df['date'])
    # считаем общее кол-во дней во Флайте
    df['days_in_flight'] = ((df['date_finish'] - df['date_start']).dt.days) + 1
    # считаем кол-во дней до окончания флайта
    df['rest_days'] = ((df['date_finish'] - df['date']).dt.days) + 1
    
    # получаем конец недели, чтобы определить дату отчета
    df['end_of_week'] = df['date'].apply(get_end_of_week)
    # определяем дату отчета (либо конец недели, либо окончание периода)
    df['report_date'] = df.apply(get_report_date, axis=1)

        # нормализуем типы данных
    int_lst = ['campaign_id', 'site_offer_id', 'insertion_id', 'impressions', 'clicks', \
               'reach_cum', 'video_views_25', 'video_views_50',\
              'video_views_75', 'video_views_100', 'viewable_views', 'source_type_id', 'main_acc_id',\
              'inner_campaign_id', 'days_in_flight', 'rest_days']
    df = normalize_columns_types(df, int_lst)

    return df


# In[ ]:


def get_weborama_standart_weekly(file_name):
    df = pd.read_excel(os.path.join(file_path, file_name), sheet_name='DataView')
    df = df[df['Campaign ID'] != 10]
    df = df[df['Project']=='X5_Perekrestok']
    df = df.rename(columns={'Date': 'date', 'Campaign ID': 'campaign_id', 'Campaign': 'campaign_name', 'Site/Offer ID': 'site_offer_id',
                            'Site/Offer': 'source',
                           'Insertion ID': 'insertion_id', 'Insertion': 'insertion', 'Project ID': 'project_id', 'Project': 'account_name',
                           'Imp.': 'impressions', 'Clicks': 'clicks', 'Reach imp.': 'reach_cum', 'Progress 25': 'video_views_25',
                           'Progress 50': 'video_views_50', 'Progress 75': 'video_views_75', 'Progress 100': 'video_views_100',
                           'Vid. MRC viewable': 'vid_mrc_viewable', 'Views': 'views'})
    
    df['viewable_views'] = df['vid_mrc_viewable'] + df['views'] # т.к. РК может относится либо к Баннеру, либо к Видео и НЕ может 
    # содержать показы одновременно в этих двух полях. Создаем поле, которое суммируем видимые показы
    df['source'] = df['source'].str.replace('vk', 'vk_ads')
    
    df = df.drop(['vid_mrc_viewable', 'views', 'project_id'], axis=1) #удаляем лишние поля
    
    int_lst = ['impressions', 'clicks', 'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75',
                  'video_views_100', 'viewable_views'] # создаем список названий колонок с числами, чтобы за один раз их преобразовать
    df[int_lst] = df[int_lst].fillna(0)
    
    df = normalize_columns_types(df, int_lst)

    df = get_merge_items(df)
    return df


# In[ ]:


def get_weborama_db_report(df):
    weborama_report_table = config.weborama_report_table
    weborama_database_df = get_mssql_table(db_name, weborama_report_table)
    # из данных БД оставляем только нужные поля
    weborama_database_df = weborama_database_df[['date', 'campaign_id', 'campaign_name', 'site_offer_id', 'source',
           'insertion_id', 'insertion', 'account_name', 'impressions', 'clicks',
           'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75',
           'video_views_100', 'viewable_views', 'type', 'category', 'product',
           'flight', 'weborama_camp_name', 'source_type_id', 'main_acc_id',
           'weborama_key_camp', 'inner_campaign_id', 'date_start', 'date_finish',
           'flight_name', 'days_in_flight', 'rest_days', 'end_of_week',
           'report_date']]
    
    # объединяем БД и excel в одну таблицу
    df = pd.concat([weborama_database_df, df])

    return df


# In[ ]:


# функция берет отчет xlsx за новый день из папки
# забирает таблицу Веборамы из БД
# добавляет к ней xlsx отчет
# считает нужные даты и пересчитывает Прирост охвата в разбивке по insertions_id и текущему флайту

def update_weborama_weekly(df, replace='False'):
    # вот эти правила расчет прироста по дням используем
    # ТОЛЬКО в том случае, если выгрузка из Веборамы в разбивке за несколько дней!
    # # Забираем текущую таблицу Веборама викли из БД
    if replace=='True':
        df = get_weborama_db_report(df)

    # Блок расчета прироста охвата по дням
    # приводим ИД РК из веборамы к строке
    df['insertion_id'] = df['insertion_id'].astype('str')
    # сортируем датаФрейм и сбрасываем индекс
    df = df.sort_values(['date', 'insertion_id', 'flight_name'])
    df = df.reset_index(drop='True')
    
    # находим первую дату, где значение охвата больше 0
    df['first_not_blank'] = df[df['reach_cum'] > 0].groupby(['insertion_id', 'flight_name'])['date'].transform('min')
    # заполняем пропуски
    df['first_not_blank'] = df['first_not_blank'].fillna('0')
    df['reach_cum'] = df['reach_cum'].astype('int64')
    # если в какие-то дни нет накопительного охвата, заполняем их значениями из последнего
    # имеющегося накопительного итога
    df['reach_not_blank'] = df.groupby(['insertion_id', 'flight_name'])['reach_cum'].cummax()

    # # вот эти правила расчет прироста по дням используем
    # # ТОЛЬКО в том случае, если выгрузка из Веборамы в разбивке за несколько дней!
    # # считаем прирост охвата по каждой кампании в разбивке по дням
    if replace=='True':
        df['increment'] = np.where((df['first_not_blank']!=0)&(df['date']==df['first_not_blank']), 
                                   df['reach_not_blank'], df['reach_not_blank'] - df.groupby(['insertion_id', 'flight_name'])['reach_not_blank'].shift(1))
    else:
        # Если выгрузка из Веборамы ЗА 1 ДЕНЬ
        # то там отображается именно ПРИРОСТ ОХВАТА, поэтому просто приравниваю 
        df['increment'] = df['reach_cum']
        
    # если где-то в приросте охвата встречается NaN, заполняем его 0
    df['increment'] = df['increment'].fillna(0)
    df = df.drop(['first_not_blank'], axis=1)
    
    # нормализуем типы данных
    weborama_report_int_lst = config.weborama_report_int_lst
    df = normalize_columns_types(df, weborama_report_int_lst)

    # Ежедневный отчет Веборама weborama_report_table
    weborama_report_table = config.weborama_report_table #'weborama_report_table'

    # # вот эти правила расчет прироста по дням используем
    # # ТОЛЬКО в том случае, если выгрузка из Веборамы в разбивке за несколько дней!
    # создаем общий список названий полей и типов данных 
    if replace=='True':
        weborama_report_table_vars_lst = config.weborama_report_table_vars_lst
        # # пересоздаем пустую таблицу Справочников в БД
        createDBTable(db_name, weborama_report_table, weborama_report_table_vars_lst, flag='drop')

    # записываем данные в БД
    downloadTableToDB(db_name, weborama_report_table, df)


# In[ ]:


def main_weborama_parse_email_report(keywords_list):
    # сохраняем файлы из почты в локальную папку
    for keyword in keywords_list:
        get_file_from_email(keyword)
    
    # Получаем список файлов
    files_list = os.listdir(file_path)
    for file_name in files_list:
        if '.xlsx' in file_name:
            if 'weekly' in file_name.lower():
                print('Файл найден')
                df = get_weborama_standart_weekly(file_name)
                update_weborama_weekly(df)
                os.remove(os.path.join(file_path, file_name))


# In[ ]:





# In[ ]:


# main_weborama_parse_email_report(keywords_list)


# In[ ]:


# files_list = os.listdir(file_path)
# for file_name in files_list:
#     if '.xlsx' in file_name:
#         if 'weekly' in file_name.lower():
#             print('Файл найден')
#             target_file_name = file_name
#             df = get_weborama_standart_weekly(target_file_name)
#             update_weborama_weekly(df) #update_weborama_weekly(df, replace='True')
#             os.remove(os.path.join(file_path, file_name))


# In[ ]:


def rewrite_weborama_db_weekly_report():
    # забираем таблицу из БД
    weborama_report_table = config.weborama_report_table
    df = get_mssql_table(db_name, weborama_report_table)

    # оставляем только нужные поля
    df = df[['date', 'campaign_id', 'campaign_name', 'site_offer_id', 'source', 'insertion_id', 'insertion', 'account_name', 'impressions', 'clicks',
        'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75', 'video_views_100', 'viewable_views','increment', 'reach_not_blank']]

    # добавляем инфо из справочников
    df = get_merge_items(df)

     # нормализуем типы данных
    weborama_report_int_lst = config.weborama_report_int_lst
    df = normalize_columns_types(df, weborama_report_int_lst)

    # Ежедневный отчет Веборама weborama_report_table
    weborama_report_table = config.weborama_report_table #'weborama_report_table'
    weborama_report_table_vars_lst = config.weborama_report_table_vars_lst
    # # пересоздаем пустую таблицу Справочников в БД
    createDBTable(db_name, weborama_report_table, weborama_report_table_vars_lst, flag='drop')

    
    # записываем данные в БД
    downloadTableToDB(db_name, weborama_report_table, df)


# In[ ]:


# rewrite_weborama_db_weekly_report()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


def get_weborama_regions_table(file_name):
    df = pd.read_excel(os.path.join(file_path, file_name), sheet_name='DataView')
    df = df.rename(columns={'Date': 'date', 'Campaign ID': 'campaign_id', 'Campaign': 'campaign_name', 'Division2': 'region_name', 
                        'Imp.': 'impressions', 'U.U.': 'reach', 'Clicks': 'clicks'})
    df = df.drop(['Division2 ID', 'U.clicks'], axis=1)
    df['campaign_name'] = df['campaign_name'].str.lower()
    df['region_name'] = df['region_name'].str.lower()
    
    return df


# In[ ]:





# In[ ]:




