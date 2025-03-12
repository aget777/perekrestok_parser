#!/usr/bin/env python
# coding: utf-8

# In[7]:


import imaplib
import email
from email.header import decode_header
import base64
from bs4 import BeautifulSoup
import re
import config
import os
import sys
from datetime import datetime
from datetime import timedelta

file_path_storage = config.files_storage
file_path = config.email_file_path
# keywords_list = ['X5_Perekrestok_Geo', 'Weborama_Standart_Weekly']


# In[ ]:





# In[5]:


def get_connection():
    imap_server = "imap.mail.ru"
    mail_pass = config.mail_pass
    username = config.username

    imap = imaplib.IMAP4_SSL(imap_server)
    imap.login(username, mail_pass)
    return imap


# In[ ]:





# In[ ]:





# In[4]:


def get_file_from_email(keyword):

    imap = get_connection()
    imap.select("INBOX")
    
    result, data = imap.search(None, f'(HEADER Subject "{keyword}")')
    if not data[0]:
        return sys.exit('Exiting the program')
    target_mail_id = data[0].split()[-1] # если несколько писем, то забираем последнее
    
    # если нужного письма нет, то выходим из программы
    if not target_mail_id:
        return sys.exit('Exiting the program')
    
    result, data = imap.fetch(target_mail_id, '(RFC822)') # забираем содержимое письма
    raw_email = data[0][1] # содержимое в закодированном виде
    
    try:
      email_message = email.message_from_string(raw_email)	
    except TypeError:
        email_message = email.message_from_bytes(raw_email)

    print ("--- нашли письмо от: ",email.header.make_header(email.header.decode_header(email_message['From'])))
    for part in email_message.walk():
        # проходим по содержимому письма
        if "application" in part.get_content_type():	    
            filename = part.get_filename()
            # создаем заголовок
            # filename=str(email.header.make_header(email.header.decode_header(filename)))
            # на всякий случай, если заголовка нет, то присваимваем свой
            # if not(filename): 
            #     filename = "weborama_report_X5_Perekrestok_Geo.xlsx"
            curr_date = (datetime.now().date()  - timedelta(days=1)).strftime('%Y_%m_%d')
            filename = keyword + '_' + str(curr_date) + '.xlsx'
            print (f'---- нашли вложение {filename}')
            fp = open(os.path.join(file_path, filename), 'wb')
            fp.write(part.get_payload(decode=1))
            fp.close
            
            fp2 = open(os.path.join(file_path_storage, filename), 'wb')
            fp2.write(part.get_payload(decode=1))
            fp2.close
            
            print ("-- удаляем письмо");
            imap.store(target_mail_id, '+FLAGS', '(\Deleted)')  
            imap.expunge()

    imap.close()
    imap.logout()


# In[ ]:


# get_file_from_email(keyword)


# In[ ]:


# for keyword in keywords_list:
#     get_file_from_email(keyword)


# In[ ]:





# In[ ]:





# In[ ]:




