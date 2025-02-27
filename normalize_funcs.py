#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

np.object = np.object_


# In[ ]:


# функция для нормализации данных - приводим в нижний регистр, заполняем пропуски и округляем до 2-х знаков после запятой
# принимает на вход:
# - датаФрейм
# - список из названий полей с типом данных Int (по умолчанию пустой список)
# - список из названий полей с типом данных Float (по умолчанию пустой список)

def normalize_columns_types(df, int_lst=list(), float_lst=list()):
    varchar_lst = list(df.columns) #df.loc[:,df.dtypes==np.object].columns # Через всторенный метод находим поля с текстовыми данными
    varchar_lst = list(set(varchar_lst) - set(int_lst) - set(float_lst)) # исключаем из списка с текстовыми данными поля Int и Float
    df[varchar_lst] = df[varchar_lst].apply(lambda x: x.astype('str').str.lower().str.strip())
    # Обрабатываем поля с типом данных Int
    df[int_lst] = df[int_lst].fillna('0')
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', ''))
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('float64').astype('int64'))
    # Обрабатываем поля с типом данных Float
    df[float_lst] = df[float_lst].fillna('0.0')
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', '').str.replace('р.', ''))
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('float64').round(2))
    
# возвращаем нормализованный датаФрейм
    return df

