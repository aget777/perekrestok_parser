#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from get_file_from_email import *
from db_funcs import *
from normalize_funcs import *
import config
import numpy as np
from datetime import date
from datetime import datetime
from perekrestok_mediaplan_parser import *

file_path = config.email_file_path
db_name = config.db_name

keywords_list = ['Weborama_Standart_Weekly']  #['X5_Perekrestok_Geo', 'Weborama_Standart_Weekly']



geo_file_name = 'X5_Perekrestok_Geo.xlsx'
weekly_file_name = 'Weborama_Standart_Weekly.xlsx'


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)


# In[3]:


# функция для отпределения Типа РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Типа
# если вхождений нет, вернет others
def get_type_from_insertion(insertion):
    if 'type_cross-stream' in insertion:
        return 'cross-stream'
    if 'type_videobanner' in insertion:
        return 'videobanner'
    if 'type_in-stream' in insertion:
        return 'in-stream'
    if 'type_multiroll' in insertion:
        return 'multiroll'
    if 'type_banner' in insertion:
        return 'banner'
    if 'type_universal' in insertion:
        return 'universal'
    if 'type_promobanner' in insertion:
        return 'promobanner'
    if 'type_main' in insertion:
        return 'main'
    if 'type_shopable-ads' in insertion:
        return 'shopable-ads'
    if 'type_multiformat' in insertion:
        return 'multiformat'
    if 'type_rewarded-video' in insertion:
        return 'rewarded-video'
    return 'other'


# In[4]:


# функция для отпределения Категории РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Категории
# если вхождений нет, вернет others
def get_category_from_insertion(insertion):
    if 'format_olv' in insertion:
        return 'olv'
    if 'format_banner' in insertion:
        return 'banner'
    return 'other'


# In[5]:


# функция для отпределения Продукта РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Продукта
# если вхождений нет, вернет others
def get_product_from_insertion(insertion):
    if 'cpv1' in insertion:
        return 'CVP1'
    if 'cpv2' in insertion:
        return 'CVP2'
    if 'ge' in insertion:
        return 'GE'
    return 'other'


# In[6]:


def get_weborama_standart_weekly(file_name):
    df = pd.read_excel(os.path.join(file_path, file_name), sheet_name='DataView')
    df = df[df['Campaign ID'] != 10]
    df = df[df['Project']=='X5_Perekrestok']
    df = df.rename(columns={'Date': 'date', 'Campaign ID': 'campaign_id', 'Campaign': 'campaign_name', 'Site/Offer ID': 'site_offer_id',
                            'Site/Offer': 'source',
                           'Insertion ID': 'insertion_id', 'Insertion': 'insertion', 'Project ID': 'project_id', 'Project': 'account_name',
                           'Imp.': 'impressions', 'Clicks': 'clicks', 'Reach imp.': 'reach_increment', 'Progress 25': 'video_views_25',
                           'Progress 50': 'video_views_50', 'Progress 75': 'video_views_75', 'Progress 100': 'video_views_100',
                           'Vid. MRC viewable': 'vid_mrc_viewable', 'Views': 'views'})
    
    df['viewable_views'] = df['vid_mrc_viewable'] + df['views'] # т.к. РК может относится либо к Баннеру, либо к Видео и НЕ может 
    # содержать показы одновременно в этих двух полях. Создаем поле, которое суммируем видимые показы
    df['source'] = df['source'].str.replace('vk', 'vk_ads')
    
    df = df.drop(['vid_mrc_viewable', 'views', 'project_id'], axis=1) #удаляем лишние поля
    
    int_lst = ['impressions', 'clicks', 'reach_increment', 'video_views_25', 'video_views_50', 'video_views_75',
                  'video_views_100', 'viewable_views'] # создаем список названий колонок с числами, чтобы за один раз их преобразовать
    df[int_lst] = df[int_lst].fillna(0)
    
    df = normalize_columns_types(df, int_lst)

    # парсим Тип РК
    df['type'] = df['insertion'].apply(get_type_from_insertion)
    # парсим название Категории
    df['category'] = df['insertion'].apply(get_category_from_insertion)
    # парсим название Продукта
    df['product'] = df['insertion'].apply(get_product_from_insertion)
    # парсим название флайта
    df['flight'] = df['insertion'].apply(lambda x: x.split('_igronik_media_')[1] if '_igronik_media_' in x else 'other')
    # создаем общее название РК
    df['weborama_camp_name'] = df['flight'] + '|format_' + df['category'] + '|type_' + df['type']

    
    return df


# In[7]:


def append_new_camp_to_dict(df):
    # забираем справочник Источников
    table_name = 'weborama_camp_dict'
    camp_dict_df = get_mssql_table(db_name, table_name)
    camp_dict_df = camp_dict_df[['weborama_key_camp', 'inner_campaign_id']]
    
    check_df = df[['weborama_camp_name', 'flight', 'type', 'category', 'product', 'source_type_id', 'main_acc_id', 'weborama_key_camp']]
    # удалаяем дубликаты
    check_df = check_df.drop_duplicates(['weborama_key_camp'])

    check_df = pd.merge(check_df, camp_dict_df, how='left', left_on='weborama_key_camp', right_on='weborama_key_camp')
    # оставляем записи, которые НЕ нашли сопоставления
    check_df = check_df[check_df['inner_campaign_id'].isna()]
    
    if check_df.empty:
        return 

    # удаляем дубликаты и оставляем только нужные поля
    check_df = check_df.drop_duplicates(['weborama_key_camp'])
    # Переприсваиваем ИД аккаунтов
    max_campaign_id = camp_dict_df['inner_campaign_id'].max()+1 # забираем максимальный ИД из справояника MSSQL
    ids_list = [i for i in range(max_campaign_id, len(check_df)+max_campaign_id)]
    check_df = check_df.reset_index(drop='True')
    check_df['id'] = pd.Series(ids_list)

    check_df = check_df.drop('inner_campaign_id', axis=1)
    check_df = check_df.rename(columns={'id': 'inner_campaign_id'})
    
    int_lst = ['source_type_id', 'main_acc_id', 'inner_campaign_id']
    check_df = normalize_columns_types(check_df, int_lst)
    
    # записываем справочник Кампаний в БД
    table_name = 'weborama_camp_dict'
    downloadTableToDB(db_name, table_name, check_df)


# In[ ]:





# In[9]:


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
                os.remove(os.path.join(file_path, file_name))
                
                # обновляем справочник источников
                # т.к. в выгрузке Веборама Викли есть источники, которые НЕ указаны в Медиапдане
                update_source_dict(df)
                # обновляем общий справочник аккаунтов из MySQL
                update_full_accounts_dict()
                # если появились новые аккаунты, то записываем их в справочник
                append_new_accs_to_dicts(df)
                
                # добавляем ИД источников к основному файлу
                df = merge_source_type_id(df)
                # забираем справочник Аккаунтов
                # добавляем ИД аккаунтов к Медиаплану
                df = merge_full_acc_id(df)
# если появились новые РК, добавляем их в справочник
                append_new_camp_to_dict(df)
                # нормализуем типы данных
                int_lst = ['campaign_id', 'site_offer_id', 'insertion_id', 'impressions', 'clicks', \
                           'reach_increment', 'video_views_25', 'video_views_50',\
                          'video_views_75', 'video_views_100', 'viewable_views', 'source_type_id', 'main_acc_id']
                df = normalize_columns_types(df, int_lst)
                # записываем в БД MSSQL медиаплан с разбивкой по дням
                table_name = 'weborama_report_table'
                downloadTableToDB(db_name, table_name, df)


# In[ ]:





# In[32]:


# main_weborama_parse_email_report(keywords_list)


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




