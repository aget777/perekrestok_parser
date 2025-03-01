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


# основной медиаплан
media_plan_link = config.media_plan_link

# тестовый медиаплан
# media_plan_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSPSSCV4yFUi2OS2whv92EwEIfKqPOIEHvb4DvvwJcJ28ftiwD7cVe_28bfLhZGhnzcjfwNF-UjhePj/pub?gid=0&single=true&output=csv'

db_name = config.db_name


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)


# In[ ]:





# In[3]:


# функция забирает медиаплан по УРЛ ссылке
# приводит в поряддок названия полей, типы данных, добавляет НДС
# и возвращает датаФрейм
def get_base_mediaplan(media_plan_link):
    media_plan_df = pd.read_csv(media_plan_link)
    
    # приводим названия полей к общему стандарту
    media_plan_df = media_plan_df.rename(columns={'DateStart': 'date_start', 'DateFinish': 'date_finish', 'Флайт': 'flight',
        'Направление рк': 'weborama_camp_name', 'Источник': 'source', 'Тип закупки': 'rotation_type', 
        'Показы': 'impressions', 'Клики': 'clicks', 'Лиды': 'leads',
        'Расход до НДС': 'costs_without_nds', 'Охват': 'reaches', 'Просмотры': 'views'})

    # меняем названия источников, чтобы они совпадали с БД MySQL
    media_plan_df['source'] = media_plan_df['source'].str.replace('vk', 'vk_ads')

    # приводим в порядок типы данных
    int_lst = ['impressions', 'clicks', 'leads', 'reaches', 'views']
    float_lst = ['costs_without_nds']
    
    media_plan_df = normalize_columns_types(media_plan_df, int_lst, float_lst)

    # добавляем расчет НДС
    media_plan_df['costs_nds'] = media_plan_df['costs_without_nds'] * 1.2
    media_plan_df['account_name'] = 'x5_perekrestok'
# из названия РК достаем название общего флайта на текущем периоде
    media_plan_df['flight_name'] = media_plan_df['weborama_camp_name'].apply(lambda x: x[x.find('_202')-2: x.find('_202')+5])
    return media_plan_df


# In[4]:


# Функция обновления справочников Источников в БД MSSQL
# Если в медиаплане появились новые источники, то перезаписываем таблицу Справочников
def update_source_dict(media_plan_df):
    table_name = 'full_source_types'
    df_sources = get_mssql_table(db_name, table_name)
    # забираем список уникальных источников из справочника MSSQL
    df_sources_unique = list(df_sources['utm_source_metrika'].unique())
    
    # забираем список уникальных источников из Медиаплана
    media_plan_sources = list(media_plan_df['source'].unique())
    
    # оставляем названия Истоников, которых нет в БД MSSQL
    media_plan_sources = list(set(media_plan_sources) - set(df_sources_unique))
# если в списке есть новые источники, то запускаем блок добавления
    if len(media_plan_sources) > 0:
        max_source_id = df_sources['id'].max() # забираем максимальный ИД из справояника MSSQL
        col_names = list(df_sources.columns)
        d = []
        
        for source in media_plan_sources:
            max_source_id += 1
            
            d.append({'id': max_source_id,
                'name': source,
                'ak_source_name': source,
                'sign': source,
                'utm_source_main': source,
                'source_engine': source,
                'utm_source_metrika': source})
        
        df = pd.DataFrame(d)
        downloadTableToDB(db_name, table_name, df)
        df_sources = pd.concat([df_sources, df])# добавляем к исходному справочнику новые данные

    # return df_sources


# In[5]:


# Функция проверяет БД MySQL и пересоздает основной справочник аккаунтов в MSSQL
def update_full_accounts_dict():
    # Сначала Забираем общий справочник Аккаунтов из MYSQL
    # там могли появиться новые аккаунты Директ, ВК и тд
    table_name = 'accounts'
    df_accounts = get_mysql_full_dict_table(db_name, table_name)
    
    # Забираем справочник источников, чтобы добавить название источника в справочник Аккаунтов
    table_name = 'source_types'
    df_sources = get_mysql_full_dict_table(db_name, table_name)
    df_sources = df_sources.drop(['created_at', 'updated_at'], axis=1)
    df_sources = df_sources.rename(columns={'id': 'source_type_id'})
    
    df_accounts = df_accounts[['id', 'source_type_id', 'account_name', 'account_id', 'acc_id_flag']]
    df_accounts['weborama_account_name'] = 'x5_perekrestok'
    # добавляем к справочнику Аккаунтов названия Источников
    df_accounts = df_accounts.merge(df_sources[['source_type_id', 'utm_source_metrika']], how='left', 
                                    left_on='source_type_id', right_on='source_type_id')
    df_accounts = df_accounts.rename(columns={'utm_source_metrika': 'source'})
    
    
    # забираем из БД MSSQL существующий справочник аккаунтов Веборама
    table_name = 'weborama_accounts'
    df_weborama_accounts = get_mssql_table(db_name, table_name)
    df_weborama_accounts = df_weborama_accounts.sort_values('source_type_id')

    # Переприсваиваем ИД аккаунтов
    max_account_id = df_accounts['id'].max()+1 # забираем максимальный ИД из справояника MSSQL
    # создаем нумерацию новых аккаунтов, начиная с последней записи из общего справочника аккаунтов
    ids_list = [i for i in range(max_account_id, len(df_weborama_accounts)+max_account_id)]
    # добавляем новое поле к таблице аккаунтов из Веборамы
    df_weborama_accounts['id'] = pd.Series(ids_list)
    # добавляем ведущий ноль к ИД аккаунта
    df_weborama_accounts['acc_id_flag'] = df_weborama_accounts['id'].apply(lambda x: '0' + str(x) if len(str(x))<2 else str(x))
    # добавляем поле с названием аккаунта, чтобы сошлись таблицы 
    df_weborama_accounts['weborama_account_name'] = df_weborama_accounts['account_name']
    # создаем итоговый датаФрейм для записи с Общим справочником аккаунтов
    df_union_accounts = pd.concat([df_accounts, df_weborama_accounts])
    # Общий Справочник Аккаунтов
    # создаем общий список названий полей и типов данных 
    # этот список передаем в БД MSSQL для создания новой таблицы
    
    # Общий Справочник Аккаунтов
    # создаем общий список названий полей и типов данных 
    # этот список передаем в БД MSSQL для создания новой таблицы
    db_vars_str = [
                'id smallint',
                'source_type_id smallint',
                'account_name nvarchar(100)',
                'account_id smallint',
                'acc_id_flag nvarchar(3)',
                'weborama_account_name nvarchar(100)',
                'source nvarchar(100)']
    # записываем новую таблицу в БД
    table_name = 'full_accounts_dict'
    
    createDBTable(db_name, table_name, db_vars_str, flag='drop')
    downloadTableToDB(db_name, table_name, df_union_accounts)


# In[6]:


# функция добавляения новых аккаунтов в Общий справочник аккаунтов и в справочник аккаунтов Веборама
def append_new_accs_to_dicts(media_plan_df):
    check_media_plan_df = media_plan_df[['source', 'account_name', 'weborama_camp_name']]
    
    # забираем справочник Источников
    table_name = 'full_source_types'
    df_sources = get_mssql_table(db_name, table_name)
    
    # добавляем в Медиаплан ИД Источников
    check_media_plan_df = check_media_plan_df.merge(df_sources[['id', 'utm_source_metrika']], 
                                                    how='left', left_on='source', right_on='utm_source_metrika')
    check_media_plan_df = check_media_plan_df.rename(columns={'id': 'source_type_id'})
    
    
    # Забираем Общий справочник Аккаунтов
    table_name = 'full_accounts_dict'
    df_accounts = get_mssql_table(db_name, table_name)
    df_accounts = df_accounts[['id', 'source_type_id', 'weborama_account_name']]
    
    check_media_plan_df = check_media_plan_df.merge(df_accounts, how='left', left_on=['source_type_id', 'account_name'], 
                                        right_on=['source_type_id', 'weborama_account_name'])
    # оставляем записи, которые НЕ нашли сопоставления
    check_media_plan_df = check_media_plan_df[check_media_plan_df['id'].isna()]
    if check_media_plan_df.empty:
        return 
    # удаляем дубликаты и оставляем только нужные поля
    check_media_plan_df = check_media_plan_df[['source_type_id', 'account_name']].drop_duplicates()
    
    # Переприсваиваем ИД аккаунтов
    max_account_id = df_accounts['id'].max()+1 # забираем максимальный ИД из справояника MSSQL
    ids_list = [i for i in range(max_account_id, len(check_media_plan_df)+max_account_id)]
    check_media_plan_df = check_media_plan_df.reset_index(drop='True')
    check_media_plan_df['id'] = pd.Series(ids_list)
    # добавляем ведущий ноль к ИД аккаунта
    check_media_plan_df['acc_id_flag'] = check_media_plan_df['id'].apply(lambda x: '0' + str(x) if len(str(x))<2 else str(x))
    check_media_plan_df['weborama_account_name'] = check_media_plan_df['account_name']
    
     # забираем из БД MSSQL существующий справочник аккаунтов Веборама
    table_name = 'weborama_accounts'
    df_weborama_accounts = get_mssql_table(db_name, table_name)
    max_account_id = df_weborama_accounts['account_id'].max()+1 # забираем максимальный ИД из справояника MSSQL
    ids_list = [i for i in range(max_account_id, len(check_media_plan_df)+max_account_id)]
    check_media_plan_df = check_media_plan_df.reset_index(drop='True')
    check_media_plan_df['account_id'] = pd.Series(ids_list)
    
    # приводим в порядок типы данных
    int_lst = ['source_type_id', 'account_id', 'id']
    check_media_plan_df = normalize_columns_types(check_media_plan_df, int_lst)
    
    # создаем отдельный датаФрейм для заливки аккаунтов Веборама
    weborama_accs_df = check_media_plan_df[['source_type_id', 'account_name', 'account_id']]
    # Заливаем новые аккаунты в Общий справочник
    table_name = 'full_accounts_dict'
    downloadTableToDB(db_name, table_name, check_media_plan_df)
    # Заливаем новые аккаунты в справочник аккаунтов Веборама
    table_name = 'weborama_accounts'
    downloadTableToDB(db_name, table_name, weborama_accs_df)


# In[7]:


# создаем функцию, которая перезаписывает справочник кампаний Веборама
def update_weborama_camp_dict(media_plan_df):
    # формируем датаФрейм для справочника кампаний
    camp_dict_df = media_plan_df[['weborama_camp_name', 'flight', 'type', 'category', 'product', 'source_type_id', 'source', 
                              'main_acc_id', 'weborama_key_camp', 'date_start', 'date_finish', 'flight_name']]
    # удалаяем дубликаты
    camp_dict_df = camp_dict_df.drop_duplicates(['weborama_key_camp'])
    
    # пронумеруем строки по порядку - это будет внутренний ИД кампании
    camp_dict_df =camp_dict_df.sort_values('main_acc_id')
    camp_dict_df['inner_campaign_id'] = np.arange(len(camp_dict_df))

    # Справочник Кампаний
    # этот список передаем в БД MSSQL для создания новой таблицы
    
    db_vars_str = [
            'weborama_camp_name nvarchar(200)',
            'flight nvarchar(100)',
            'type nvarchar(100)',
            'category nvarchar(100)',
            'product nvarchar(100)',
            'source_type_id smallint',
            'source nvarchar(100)',
            'main_acc_id smallint',
            'weborama_key_camp nvarchar(200)',
            'inner_campaign_id smallint',
            'date_start nvarchar(10)',
            'date_finish nvarchar(10)',
            'flight_name nvarchar(10)']
    
    # создаем пустую таблицу cправочник Кампаний в БД
    table_name = 'weborama_camp_dict'
    createDBTable(db_name, table_name, db_vars_str, flag='drop')

    int_lst = ['source_type_id', 'main_acc_id', 'inner_campaign_id']
    camp_dict_df = normalize_columns_types(camp_dict_df, int_lst)

    # записываем справочник Кампаний в БД
    downloadTableToDB(db_name, table_name, camp_dict_df)


# In[8]:


# создаем функцию, чтобы разбить Медиаплан по дням
def parse_mediaplan_by_days(media_plan_df):
    # создаем пустой датаФрейм, в который сохраним разбивки медиаплана по дням по каждой строке Медиаплана
    media_plan_by_days = pd.DataFrame()
    for i in range(len(media_plan_df)):
        # Забираем одну строку из датаФрейма
        df = media_plan_df.iloc[[i]]
        # приводим даты к формату ДатаВремя
        start_date = datetime.date(df['date_start'].iloc[0])
        end_date = datetime.date(df['date_finish'].iloc[0])
        # print(start_date)
        calendar_df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])
        calendar_df['end_of_week'] = calendar_df['date'].apply(get_end_of_week)
        # передаем общие характеристики Кампании
        calendar_df['flight'] = df['flight'].iloc[0]
        calendar_df['product'] = df['product'].iloc[0]
        calendar_df['category'] = df['category'].iloc[0]
        calendar_df['type'] = df['type'].iloc[0]
        calendar_df['weborama_camp_name'] = df['weborama_camp_name'].iloc[0]
        calendar_df['source'] = df['source'].iloc[0]
        calendar_df['rotation_type'] = df['rotation_type'].iloc[0]
    # Передаем ИД источника, аккаунта и ключ для связи кампаний
        calendar_df['source_type_id'] = df['source_type_id'].iloc[0]
        calendar_df['main_acc_id'] = df['main_acc_id'].iloc[0]
        calendar_df['weborama_key_camp'] = df['weborama_key_camp'].iloc[0]
        calendar_df['flight_name'] = df['flight_name'].iloc[0]
        # формируем разбивку показателей на каждый отдельный день
        calendar_df['impressions_plan'] = df['impressions_plan'].iloc[0]
        calendar_df['clicks_plan'] = df['clicks_plan'].iloc[0]
        calendar_df['convs_plan'] = df['convs_plan'].iloc[0]
        calendar_df['costs_without_nds_plan'] = df['costs_without_nds_plan'].iloc[0]
        calendar_df['costs_nds_plan'] = df['costs_nds_plan'].iloc[0]
        calendar_df['reach_plan'] = df['reach_plan'].iloc[0]
        calendar_df['views_plan'] = df['views_plan'].iloc[0]
        calendar_df['date_start'] = df['date_start'].iloc[0]
        calendar_df['date_finish'] = df['date_finish'].iloc[0]
        calendar_df['rest_days'] = ((calendar_df['date_finish'] - calendar_df['date']).dt.days) + 1
        calendar_df['days_in_flight'] = df['days_in_flight'].iloc[0]
        # определяем дату отчета (либо конец недели, либо окончание периода)
        calendar_df['report_date'] = calendar_df.apply(get_report_date, axis=1)
        # добавляем в общий датаФрейм
        media_plan_by_days = pd.concat([media_plan_by_days, calendar_df])
    # Медиаплан в разбивке по дням
    # создаем общий список названий полей и типов данных 
    # этот список передаем в БД MSSQL для создания новой таблицы
    
    db_vars_str = [
                'date nvarchar(10)',
                'flight nvarchar(100)',
                'flight_name nvarchar(10)',
                'product nvarchar(100)',
                'category nvarchar(100)',
                'type nvarchar(100)',
                'weborama_camp_name nvarchar(200)',
                'source nvarchar(100)',
                'rotation_type nvarchar(10)',
                'source_type_id smallint',
                'main_acc_id smallint',
                'weborama_key_camp nvarchar(200)',
                'impressions_plan float',
                'clicks_plan float',
                'convs_plan float',
                'costs_without_nds_plan float',
                'costs_nds_plan float',
                'reach_plan float',
                'views_plan float',
                'date_start nvarchar(10)',
                'date_finish nvarchar(10)',
                'end_of_week nvarchar(10)',
                'rest_days smallint',
                'days_in_flight smallint',
                'report_date nvarchar(10)'
    ]


    # пересоздаем пустую таблицу Справочников в БД
    table_name = 'weborama_plan_table'
    createDBTable(db_name, table_name, db_vars_str, flag='drop')
    # нормализуем типы данных
    int_lst = ['source_type_id', 'main_acc_id', 'rest_days', 'days_in_flight']
    float_lst = ['impressions_plan', 'clicks_plan', 'convs_plan', 'costs_without_nds_plan', 'costs_nds_plan', 'reach_plan', 'views_plan']
    media_plan_by_days = normalize_columns_types(media_plan_by_days, int_lst, float_lst)

    # записываем в БД MSSQL медиаплан с разбивкой по дням
    table_name = 'weborama_plan_table'
    downloadTableToDB(db_name, table_name, media_plan_by_days)
        


# In[9]:


def merge_source_type_id(media_plan_df):
    # забираем справочник Источников
    # добавляем ИД источников к Медиаплану
    table_name = 'full_source_types'
    df_sources = get_mssql_table(db_name, table_name)
    # добавляем в Медиаплан ИД Источников
    media_plan_df = media_plan_df.merge(df_sources[['id', 'utm_source_metrika']], how='left', left_on='source', right_on='utm_source_metrika')
    media_plan_df = media_plan_df.rename(columns={'id': 'source_type_id'})
    media_plan_df = media_plan_df.drop('utm_source_metrika', axis=1)

    return media_plan_df


# In[10]:


def merge_full_acc_id(media_plan_df):
    # забираем справочник Аккаунтов
    # добавляем ИД аккаунтов к Медиаплану
    table_name = 'full_accounts_dict'
    df_accounts = get_mssql_table(db_name, table_name)
    df_accounts = df_accounts[['id', 'source_type_id', 'weborama_account_name']]
    media_plan_df = media_plan_df.merge(df_accounts, how='left', left_on=['source_type_id', 'account_name'], 
                                            right_on=['source_type_id', 'weborama_account_name'])
    
    media_plan_df = media_plan_df.drop('weborama_account_name', axis=1)
    media_plan_df = media_plan_df.rename(columns={'id': 'main_acc_id'})

    # формируем ключ для Кампаний
    # по этому ключу будем объединять данные в дашборде
    media_plan_df['weborama_key_camp'] = media_plan_df['source_type_id'].astype('str') + '_' + media_plan_df['main_acc_id'].astype('str') \
    +  '_' + media_plan_df['weborama_camp_name']
    return media_plan_df


# In[11]:


# функцию, которая определяет конец недели
def get_end_of_week(date):
    start = date - timedelta(days=date.weekday())
    end = (start + timedelta(days=6)).strftime('%Y-%m-%d')
    return end


# In[12]:


# создаем функцию, которая определяет дату отчета
# если конец недели меньше окончания периода, то дата отчета равна концу недели
# иначе равна концу периода
def get_report_date(row):
    if row['date_finish'] > pd.to_datetime(row['end_of_week']):
        return row['end_of_week']
    return row['date_finish'].date()


# In[13]:


def main_mediaplan_parse_func(media_plan_link):
    # загружаем Медиаплан из Гугл докс и проводим первичную обработку
    media_plan_df = get_base_mediaplan(media_plan_link)
    
    # если в Медиаплане появились новые источники
    # то обрабатываем их и записываем в БД MSSQL
    # если нет, то просто возвращаем справочник источников
    update_source_dict(media_plan_df)

    # обновляем общий справочник аккаунтов из MySQL
    update_full_accounts_dict()
    # обновляем общий справочник аккаунтов в MSSQL
    append_new_accs_to_dicts(media_plan_df)

    # забираем справочник Источников
    # добавляем ИД источников к Медиаплану
    media_plan_df = merge_source_type_id(media_plan_df)

    # забираем справочник Аккаунтов
    # добавляем ИД аккаунтов к Медиаплану
    media_plan_df = merge_full_acc_id(media_plan_df)
    # обновляем справочник рекламных кампаний
    update_weborama_camp_dict(media_plan_df)

    # приводим даты к формату ДатаВремя
    media_plan_df['date_start'] = media_plan_df['date_start'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    media_plan_df['date_start'] = pd.to_datetime(media_plan_df['date_start'])
    media_plan_df['date_finish'] = media_plan_df['date_finish'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    media_plan_df['date_finish'] = pd.to_datetime(media_plan_df['date_finish'])

    # считаем общее кол-во дней во Флайте
    media_plan_df['days_in_flight'] = ((media_plan_df['date_finish'] - media_plan_df['date_start']).dt.days) + 1

    # считаем каждый показатель План в день
    media_plan_df['impressions_plan'] = (media_plan_df['impressions'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['clicks_plan'] = (media_plan_df['clicks'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['convs_plan'] = (media_plan_df['leads'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['costs_without_nds_plan'] = (media_plan_df['costs_without_nds'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['costs_nds_plan'] = (media_plan_df['costs_nds'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['reach_plan'] = (media_plan_df['reaches'] / media_plan_df['days_in_flight']).astype('float64').round(2)
    media_plan_df['views_plan'] = (media_plan_df['views'] / media_plan_df['days_in_flight']).astype('float64').round(2)

    # обновляем таблицу фактов Медиаплан в разбивке по дням
    parse_mediaplan_by_days(media_plan_df)


# In[15]:


# main_mediaplan_parse_func(media_plan_link)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# # загружаем Медиаплан из Гугл докс и проводим первичную обработку
# media_plan_df = get_base_mediaplan(media_plan_link)


# In[ ]:


# # если в Медиаплане появились новые источники
# # то обрабатываем их и записываем в БД MSSQL
# # если нет, то просто возвращаем справочник источников
# update_source_dict(media_plan_df)


# In[ ]:


# update_full_accounts_dict()


# In[ ]:


# append_new_accs_to_dicts(media_plan_df)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# df_weborama_accounts = media_plan_df[['source_type_id', 'source', 'account_name']]
# df_weborama_accounts = df_weborama_accounts.drop_duplicates(['source_type_id', 'account_name'])
# df_weborama_accounts = df_weborama_accounts[df_weborama_accounts['source_type_id'] !=1]
# df_weborama_accounts = df_weborama_accounts[df_weborama_accounts['source_type_id'] !=4]
# df_weborama_accounts = df_weborama_accounts.sort_values('source_type_id')
# df_weborama_accounts = df_weborama_accounts.reset_index(drop='True')

# # Переприсваиваем ИД аккаунтов
# # max_account_id = df_accounts['id'].max()+1 # забираем максимальный ИД из справояника MSSQL
# # создаем нумерацию новых аккаунтов, начиная с последней записи из общего справочника аккаунтов
# ids_list = [i for i in range(1, len(df_weborama_accounts)+1)]
# # добавляем новое поле к таблице аккаунтов из Веборамы
# df_weborama_accounts['id'] = pd.Series(ids_list)


# In[ ]:


# file_name = 'weborama_accounts.xlsx'
# df_weborama_accounts.to_excel(os.path.join(file_path, file_name))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# # приводим даты к формату ДатаВремя
# media_plan_df['date_start'] = media_plan_df['date_start'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
# media_plan_df['date_start'] = pd.to_datetime(media_plan_df['date_start'])
# media_plan_df['date_finish'] = media_plan_df['date_finish'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
# media_plan_df['date_finish'] = pd.to_datetime(media_plan_df['date_finish'])

# # считаем общее кол-во дней во Флайте
# media_plan_df['days_in_flight'] = ((media_plan_df['date_finish'] - media_plan_df['date_start']).dt.days) + 1


# In[ ]:


# # считаем каждый показатель План в день
# media_plan_df['impressions_plan'] = (media_plan_df['impressions'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['clicks_plan'] = (media_plan_df['clicks'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['convs_plan'] = (media_plan_df['leads'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['costs_without_nds_plan'] = (media_plan_df['costs_without_nds'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['costs_nds_plan'] = (media_plan_df['costs_nds'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['reach_plan'] = (media_plan_df['reaches'] / media_plan_df['days_in_flight']).astype('float64').round(2)
# media_plan_df['views_plan'] = (media_plan_df['views'] / media_plan_df['days_in_flight']).astype('float64').round(2)


# In[ ]:


# parse_mediaplan_by_days(media_plan_df)


# In[ ]:




