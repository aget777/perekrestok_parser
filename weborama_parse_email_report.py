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
from perekrestok_mediaplan_parser import merge_source_type_id, merge_full_acc_id, get_end_of_week, get_report_date
from time import sleep
# import perekrestok_mediaplan_parser

file_path = config.email_file_path
db_name = config.db_name

keywords_list = ['Weborama_Standart_Weekly', 'X5_Perekrestok_Geo']  #['X5_Perekrestok_Geo', 'Weborama_Standart_Weekly']
reports_lst = ['weekly', 'geo']


# geo_file_name = 'X5_Perekrestok_Geo.xlsx'
# weekly_file_name = 'Weborama_Standart_Weekly.xlsx'


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
def parse_type_category(row, flag='type_'):
    if flag in row:
        start_index = row.find(flag)
        cut_row = row[start_index+len(flag):]
        end_index = cut_row.find('|')
        return cut_row[:end_index] if end_index!=-1 else cut_row
    else:
        return 'other'


# In[ ]:





# In[4]:


# функция для отпределения Продукта РК
# на вход принимает 1 поле insertion из датаФрейма и возвращает название Продукта
# если вхождений нет, вернет others
def get_product_from_insertion(insertion):
    if 'cvp1_' in insertion:
        return 'cvp1'
    if 'cvp2_' in insertion:
        return 'cvp2'
    if 'ge_' in insertion:
        return 'ge'
    return 'other'


# In[5]:


# создаем функцию, которая в себе оьъединяет ВСЕ вызовы для парсинга Типа-Категории-Продукта-Флайта
# Передаем в нее ДатаФрейм и тип отчета
# т.к. для отчета raw нужно получить отдельно название файла
def get_parse_items(df, report='raw'):
    # парсим Тип РК
    df['type'] = df['insertion'].apply(parse_type_category, flag='type_')
    # парсим название Категории
    df['category'] = df['insertion'].apply(parse_type_category, flag='format_')
    # парсим название Продукта
    df['product'] = df['insertion'].apply(get_product_from_insertion)
    # парсим название флайта
    df['flight'] = df['insertion'].apply(lambda x: x.split('_igronik_media_')[1] if '_igronik_media_' in x else 'other')
    
    if report=='raw':
        df['flight_name'] = df['flight'].apply(lambda x: x[x.find('_2025')-2:] if '_2025' in x else '')
        
    # создаем общее название РК
    df['weborama_camp_name'] = df['flight'] + '|format_' + df['category'] + '|type_' + df['type']

    return df


# In[6]:


# с помощью этой функции мы добавляем к отчету Веборама след. поля
# Названия Источников / Аккаунтов / Дату начала - окончания флайта / Кол-во дней до конца флайта и тд

def get_merge_items(df):
        
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


# In[7]:


# Функкция для парсинга ежедневного отчета Веборама викли
# Обрабатываем файл для общего сырого отчета Веборама и отдельно для Перекрестка(с учетом фильтрации)

def get_weborama_standart_weekly(file_name, replace='False', report='raw'):
    df = pd.read_excel(os.path.join(file_path, file_name), sheet_name='DataView')
    if report=='weekly':
        # для БД Перекресток отфильтровываем строки по условию
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

    # создаем список названий колонок с числами, чтобы за один раз их преобразовать
    if report=='raw':
        int_lst = ['impressions', 'clicks', 'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75',
                  'video_views_100', 'viewable_views', 'vid_mrc_viewable', 'views', 'project_id']
    
    if report=='weekly':
        df = df.drop(['vid_mrc_viewable', 'views', 'project_id'], axis=1) #удаляем лишние поля
        int_lst = ['impressions', 'clicks', 'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75',
                      'video_views_100', 'viewable_views'] 
    # приводим в порядок строки и числа (убираем лишние символы, заменяем nan, приводим к нужному типу данных
    df[int_lst] = df[int_lst].fillna(0)
    df = normalize_columns_types(df, int_lst)
    # добавляем Тип, Категория, Продукт, Флайт и тд.
    df = get_parse_items(df, report)
    
    if report=='raw':
        update_weborama_weekly(df, replace, report)
    if report=='weekly':
        df = get_merge_items(df)
        update_weborama_weekly(df, replace, report)


# In[ ]:





# In[40]:


# получить все записи из БД
# Чтобы потом к ним применить правила для пересчета охвата, который идет НАКОПИТЕЛЬНЫМ итогом в файле Эксель
def get_weborama_db_report(df, report='raw'):
    # Базовый список названий полей, который подходит для всех отчетов
    base_cols_list = ['date', 'campaign_id', 'campaign_name', 'site_offer_id', 'source',
               'insertion_id', 'insertion', 'project_id', 'account_name', 'impressions', 'clicks',
               'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75',
               'video_views_100', 'viewable_views', 'type', 'category', 'product',
               'flight', 'weborama_camp_name', 'flight_name']
    # список названий полей для еженедельного отчета Веборама
    weekly_cols_list = ['source_type_id', 'main_acc_id',
               'weborama_key_camp', 'inner_campaign_id', 'date_start', 'date_finish',
                'days_in_flight', 'rest_days', 'end_of_week', 'report_date']
    
    if report=='raw':
        db_name = config.db_x5_name
        weborama_report_table = config.weborama_raw_report_table
        weborama_database_df = get_mssql_table(db_name, weborama_report_table)
        # из данных БД оставляем только нужные поля
        weborama_database_df = weborama_database_df[base_cols_list]
        
        # объединяем БД и excel в одну таблицу
        df = pd.concat([weborama_database_df, df])
    if report=='weekly':
        # формируем итоговый список полей для еженедельного отчета
        weekly_cols_list = list(set(base_cols_list + weekly_cols_list))
        weekly_cols_list.remove('project_id')
        db_name = config.db_name
        weborama_report_table = config.weborama_report_table
        weborama_database_df = get_mssql_table(db_name, weborama_report_table)
        # из данных БД оставляем только нужные поля
        weborama_database_df = weborama_database_df[weekly_cols_list]
        
        # объединяем БД и excel в одну таблицу
        df = pd.concat([weborama_database_df, df])

    return df


# In[9]:


# функция берет отчет xlsx за новый день из папки
# забирает таблицу Веборамы из БД
# добавляет к ней xlsx отчет
# считает нужные даты и пересчитывает Прирост охвата в разбивке по insertions_id и текущему флайту
# repor weekly - raw
 
def update_weborama_weekly(df, replace='False', report='raw'):
    # вот эти правила расчет прироста по дням используем
    # ТОЛЬКО в том случае, если выгрузка из Веборамы в разбивке за несколько дней!
    
    # Если в БД будут значения ОХВАТА в перемешку - один файл загрузли из файла эксель, в котором выгрузка за несколько дней
    # второй файл только за 1 день, то эта логика работать НЕ БУДЕТ
    # т.к. мы не можем определить, где охват считается накопительным итогом, а где просто за 1 день
    # поэтом ее включаем только при первой загрузке с файлом НАКОПИТЕЛЬНЫМ итогом
    # # Забираем текущую таблицу Веборама викли из БД
    
    if replace=='True':
        df = get_weborama_db_report(df, report)

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

    # формируем список названий полей для нормализации числовых значений
    if report=='raw':
        # нормализуем типы данных
        db_name = config.db_x5_name
        weborama_report_int_lst = config.weborama_raw_report_int_lst
        
    if report=='weekly':
        db_name = config.db_name
        # нормализуем типы данных
        weborama_report_int_lst = config.weborama_report_int_lst
        
    # Ежедневный отчет Веборама weborama_report_table  - Называет одинаково во всех БД
    weborama_report_table = config.weborama_report_table #'weborama_report_table'   
    
    df = normalize_columns_types(df, weborama_report_int_lst)
    # # вот эти правила расчет прироста по дням используем
    # # ТОЛЬКО в том случае, если выгрузка из Веборамы в разбивке за несколько дней!
    # создаем общий список названий полей и типов данных 
    if replace=='True':
        if report=='raw':
            db_name = config.db_x5_name
            weborama_raw_report_table_vars_lst = config.weborama_raw_report_table_vars_lst
            # # пересоздаем пустую таблицу Справочников в БД
            createDBTable(db_name, weborama_report_table, weborama_raw_report_table_vars_lst, flag='drop')
            
        if report=='weekly':
            db_name = config.db_name
            weborama_report_table_vars_lst = config.weborama_report_table_vars_lst
            # # пересоздаем пустую таблицу Справочников в БД
            createDBTable(db_name, weborama_report_table, weborama_report_table_vars_lst, flag='drop')

    # записываем данные в БД
    downloadTableToDB(db_name, weborama_report_table, df)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[10]:


# для Отфильтрованного отчета 
# Функция перезаписи даты начала и окночания флайта и тд.
# для сырых данных ее нет смысла использовать, т.к. там НЕТ ничего из Медиаплана
def rewrite_weborama_db_weekly_report():
    # забираем таблицу из БД
    weborama_report_table = config.weborama_report_table
    df = get_mssql_table(db_name, weborama_report_table)

    # оставляем только нужные поля
    df = df[['date', 'campaign_id', 'campaign_name', 'site_offer_id', 'source', 'insertion_id', 'insertion', 'account_name', 'impressions', 'clicks',
        'reach_cum', 'video_views_25', 'video_views_50', 'video_views_75', 'video_views_100', 'viewable_views','increment', 'reach_not_blank']]
    # добавляем Тип, Категория, Продукт, Флайт и тд.
    df = get_parse_items(df, report='weekly')
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


# In[11]:


# rewrite_weborama_db_weekly_report()


# In[ ]:





# In[12]:


# Справочник Регионов сейчас хранится в гугл доксе
# Функция парсит этот гугл докс и выполняет 2 действия
# - каждый день перезаписывает справочник Регионов в Общей БД и БД Перекресток
# - создает датаФрейм с уникаьными названиями регионов Веборама, которые сейчас есть в справочнике
# если в Гео отчете Веборама появится регион, которого НЕТ в справочнике мы узнаем, т.к. для него в Гео отчете запишем значение no_name
def get_regions_dict(df, db_name):
    # забираем справочник регионов из гугл докса
    regions_link = config.regions_link
    df = pd.read_csv(regions_link)
    # убираем пустые строки
    df = df[df['name'] != '']
    # нормализуем типы данных
    weborama_regions_dict_int_lst = config.weborama_regions_dict_int_lst
    df = normalize_columns_types(df, weborama_regions_dict_int_lst)

    # Создаем / Перезаписываем справочник Регионов в общую БД Х5 и отдельно в БД Перекресток
    weborama_regions_dist = config.weborama_regions_dist # название справочника Регионов
    weborama_regions_dict_vars_lst = config.weborama_regions_dict_vars_lst
    # пересоздаем пустую таблицу Справочников в БД
    createDBTable(db_name, weborama_regions_dist, weborama_regions_dict_vars_lst, flag='drop')
    # записываем данные в БД
    downloadTableToDB(db_name, weborama_regions_dist, df)
    # создаем список уникальных регионов Веборама
    # далее его объеденим с таблицей Веборама Гео репорт
    # чтобы посмотреть - есть ли пропуски
    # возможно в отчете Веборама есть регионы НЕ добавленные в справочник
    unique_weborama_regions = df[['weborama_region_name']].drop_duplicates().reset_index(drop='True')
    
    return unique_weborama_regions


# In[13]:


# основная функция для обработки Гео отечта
# она работает и для Полного отчета и отдельно для Перекрестка
# на вход принимает 2 параметра 
# - путь к эксель файлу
# - тип отчета raw / weekly
def get_weborama_regions_table(file_name, report='raw'):
    df = pd.read_excel(os.path.join(file_path, file_name), sheet_name='DataView')
    # приводим названия полей к единому стандарту
    # поле Campaign': 'insertion' - специально так сделали, чтобы не менять логику в функции get_parse_items
    # после ее выполнения переименуем обратно в campaign
    df = df.rename(columns={'Date': 'date', 'Campaign ID': 'campaign_id', 'Campaign': 'insertion', 'Division2': 'region_name', 
                            'Division2 ID': 'region_id', 'Imp.': 'impressions', 'U.U.': 'increment', 'Clicks': 'clicks', 'U.clicks': 'u_clicks'})
    
    # приводим в порядок строки и числа (убираем лишние символы, заменяем nan, приводим к нужному типу данных
    df_int_lts = ['campaign_id', 'region_id', 'impressions', 'increment', 'clicks', 'u_clicks']
    df = normalize_columns_types(df, df_int_lts)
    # добавляем Тип, Категория, Продукт, Флайт и тд.
    df = get_parse_items(df, report='geo')
    df['flight_name'] = df['insertion'].apply(lambda x: x[x.find('_2025')-2:] if '_2025' in x else '')

    df = df.rename(columns={'insertion': 'campaign_name'})
    # в зависимости от отчета оперделяем название БД для записи
    if report=='raw':
        db_name = config.db_x5_name
    if report=='geo':
        db_name = config.db_name
    # вызываем функцию для перезаписи справочника регионов и получения уникальных регионов Веборама
    unique_weborama_regions = get_regions_dict(df, db_name)

    df = df.merge(unique_weborama_regions, how='left', left_on=['region_name'], right_on=['weborama_region_name'])
    # если в ежедневном отчете Гео встречается регион, которого нет в справочнике
    # то присваиваем ему название no_name
    df['weborama_region_name'] = df['weborama_region_name'].fillna('no_name')

    # Создаем / Перезаписываем справочник Регионов в общую БД Х5 и отдельно в БД Перекресток
    weborama_geo_report = config.weborama_geo_report # название справочника Регионов
    weborama_geo_report_int_lst = config.weborama_geo_report_int_lst
    df = normalize_columns_types(df, weborama_geo_report_int_lst)
    # отфильтровываем строки для БД Перекресток
    if report=='geo':
        df = df[df['campaign_id'] != 10]

    weborama_geo_report = config.weborama_geo_report # название ежедневного отчета Веборама Гео
    # записываем данные в БД
    downloadTableToDB(db_name, weborama_geo_report, df)


# In[14]:


# report - weekly / raw | geo / raw
# Основная функция для сохранения отчета из почту в 
# - папку storage(хранилище файлов) 
# - и в папку weekly(после парсинга файл удаляется)
# функия на вход принимает 
# keywords_list - список ключевых слов, по которым ищем нужные письма на почте
# reports_lst - список типов отчетов weekly / geo
# replace - при первой записи в БД для расчета прироста охвата по дням, если в эксель он считается накопительным итогом
# по умолчанию 1 эксель=1 день -  в таком случае охват идет Приростом
# если в одном файле excel несколько дней replace='True'
def main_weborama_parse_email_report(keywords_list, reports_lst, replace='False'):
    # сохраняем файлы из почты в локальную папку
    for keyword in keywords_list:
        get_file_from_email(keyword)

    sleep(30)
    print('Забираем отчеты из папки')
    for report in reports_lst:
        # Получаем список файлов
        # file_path = config.email_file_path
        file_path = os.path.join(config.email_file_path, report)
        files_list = os.listdir(file_path)
        for file_name in files_list:
            if '.xlsx' in file_name:
                file = os.path.join(file_path, file_name)
                if 'weekly' in file_name.lower():
                    print('Найден файл Weekly')
                    # записываем данные в Общую БД
                    get_weborama_standart_weekly(file, replace='False', report='raw') # если в одном файле excel несколько дней replace='True'
                    # Записываем данные в БД Перекресток
                    get_weborama_standart_weekly(file, replace='False', report=report) 
                    os.remove(os.path.join(file_path, file_name))
                if 'geo' in file_name.lower():
                    print('Найден файл Geo')
                    # записываем данные в Общую БД
                    get_weborama_regions_table(file, report='raw')
                    # Записываем данные в БД Перекресток
                    get_weborama_regions_table(file, report=report)
                    os.remove(os.path.join(file_path, file_name))


# In[15]:


# main_weborama_parse_email_report(keywords_list, reports_lst, replace='False')


# In[42]:


# for report in reports_lst:
#     # Получаем список файлов
#     # file_path = config.email_file_path
#     file_path = os.path.join(config.email_file_path, report)
#     files_list = os.listdir(file_path)
#     for file_name in files_list:
#         if '.xlsx' in file_name:
#             file = os.path.join(file_path, file_name)
#             if 'weekly' in file_name.lower():
#                 print('Найден файл Weekly')
#                 get_weborama_standart_weekly(file, replace='False', report='raw') # если в одном файле excel несколько дней replace='True'
#                 get_weborama_standart_weekly(file, replace='False', report=report) # если в одном файле excel несколько дней replace='True'
#                 os.remove(os.path.join(file_path, file_name))
#             if 'geo' in file_name.lower():
#                 print('Найден файл Geo')
#                 get_weborama_regions_table(file, report='raw')
#                 get_weborama_regions_table(file, report=report)
#                 os.remove(os.path.join(file_path, file_name))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




