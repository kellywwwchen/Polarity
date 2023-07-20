import os
import re
import pickle
import sqlite3
import numpy as np
import pandas as pd

from tqdm import tqdm
from time import strptime
from dateutil import parser


PATH = '/home/kellychen/Polarity/ptt-web-crawler/PttWebCrawler'
PICKLE_FILE = 'all_ptt_2020_2022_test.pickle'
DB_FILE = 'gossiping_2020_2022.db'

print(f'===== Read the pickle: {PICKLE_FILE} =====')
with open(os.path.join(PATH, PICKLE_FILE), 'rb') as f:
    all_ptt_pickle = pickle.load(f)

print(f'===== Remove the error =====')
# 移除 24個 dict 內容為 {'error': 'invalid url'}
rm_error_all_ptt_list = [i for i in all_ptt_pickle['articles'] if len(i) == 10]
all_ptt_df = pd.DataFrame.from_records(rm_error_all_ptt_list)
all_ptt_df['date'] = all_ptt_df['date'].astype(str)
all_ptt_df['article_title'] = all_ptt_df['article_title'].astype(str)

# 更改文章日期格式 e.g. Fri Jan  7 12:39:36 2022 -> 2022-01-07 12:39:22
def transform_article_datetime(r):
    year_regex = re.compile(r'^\d{4}')
    month_regex = re.compile(r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec')
    day_regex = re.compile(r'^(0?[1-9]|[12][0-9]|3[01])')
    time_regex = re.compile(r'\d{2}:\d{2}:\d{2}$')
    if any(year_regex.search(x) for x in r.split()) and any(month_regex.search(x) for x in r.split()) and any(month_regex.search(x) for x in r.split()) and any(time_regex.search(x) for x in r.split()): # 是否存在 date and time
        post_year = list(filter(year_regex.search, r.split()))[0]  # yyyy
        post_month = str(strptime(list(filter(month_regex.search, r.split()))[0],'%b').tm_mon)  # mm
        post_day = list(filter(day_regex.search, r.split()))[0]  #dd
        post_time = list(filter(time_regex.search, r.split()))[0] # HH:MM:SS
        post_datetime = ' '.join([post_year, post_month, post_day, post_time])  # 'yyyy mm dd HH:MM'
        return parser.parse(post_datetime)  # 自動日期時間格式: 'yyyy-mm-dd HH:MM:00' 
    else:
        return np.nan
    
# 更改留言日期&IP格式 e.g. 42.75.154.235 01/07 12:40 -> IP:42.75.154.235 / datetime: 2022-01-07 12:40:00
def transform_push_ip_datetime(r, push_year):
    date_regex = re.compile(r'^\d{2}/\d{2}$')
    time_regex = re.compile(r'\d{2}:\d{2}$')
    ip_regex = re.compile(r'^\d{1,9}\.\d{1,9}\.\d{1,9}\.\d{1,9}$')
    try:
        if any(date_regex.search(x) for x in r.split()) and any(time_regex.search(x) for x in r.split()): # 是否存在 date and time
            push_date = '/'.join([push_year, list(filter(date_regex.search, r.split()))[0]])  # yyyy/mm/dd
            push_time = list(filter(time_regex.search, r.split()))[0]  # HH:MM
            push_datetime = parser.parse(' '.join([push_date, push_time]))  # 自動日期時間格式: 'yyyy-mm-dd HH:MM:00'
        else:
            push_datetime = np.nan
    except:
        push_datetime = np.nan  
         
    if any(ip_regex.search(x) for x in r.split()):     # 是否存在 ip 
        push_ip = list(filter(ip_regex.search, r.split()))[0]
    else:
        push_ip = np.nan
    
    return push_datetime, push_ip    
    
politician = ['陳時中', '郭台銘', '柯文哲', '蔡英文', '韓國瑜']
def calculate_politician(r):
    if any(name in r['article_title'] for name in politician) or any(name in r['content'] for name in politician):
        title_counter = [r['article_title'].count(name) for name in politician]
        content_counter = [r['content'].count(name) for name in politician]
        total_counter = np.sum([title_counter, content_counter], axis=0).tolist()
        politician_count = dict(zip(politician, total_counter))
        max_showup = [k for k, v in politician_count.items() if v == max(politician_count.values())]
        # max_showup = politician[counter.index(max(counter))]
        return str(total_counter), str(max_showup)
    else:
        return '0', '0'    

print(f'===== Processing the date format =====')
all_ptt_df['date'] = all_ptt_df['date'].apply(transform_article_datetime)
# 保留 2020~2022
m1 = (all_ptt_df['date'] < '2020-01-01')
m2 = (all_ptt_df['date'] >= '2023-01-01')
all_ptt_df = all_ptt_df[~m1&~m2].sort_values(['date'])
all_ptt_df.drop_duplicates(subset=['article_id'], inplace=True, ignore_index=True)
print(f'===== Calculating the politician =====')
all_ptt_df['politician_count'], all_ptt_df['main_politician'] = zip(*all_ptt_df.apply(calculate_politician, axis=1))


# Connect to the SQLite database
conn = sqlite3.connect(os.path.join(PATH, DB_FILE))
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE article (
        article_id VARCHAR PRIMARY KEY,
        article_title TEXT,
        author TEXT,
        board TEXT,
        content TEXT,
        date DATETIME,
        ip VARCHAR,
        url TEXT,
        politician_count BLOB,
        main_politician BLOB,
        all_count INTEGER,
        boo INTEGER, 
        count INTEGER, 
        neutral INTEGER, 
        push INTEGER
    )
''')

cursor.execute('''
    CREATE TABLE comments (
        push_content TEXT, 
        push_tag TEXT, 
        push_userid VARCHAR, 
        push_time DATETIME, 
        push_user_ip VARCHAR,
        a_id VARCHAR,
        FOREIGN KEY (a_id) REFERENCES article(article_id)
    )
''')

error = {}
# 560369
for i, article_id in tqdm(enumerate(all_ptt_df['article_id'].unique())):
    try:
        # 無留言
        if len(all_ptt_df['messages'][i]) == 0:
            tmp1 = all_ptt_df.loc[[i]]
            # 推/噓/→ 統計 0 cols = ['all', 'boo', 'count', 'neutral', 'push']
            tmp2 = pd.DataFrame([[np.nan]*5+[article_id]], columns = ['all_count', 'boo', 'count', 'neutral', 'push', 'article_id'])
            tmp2 = pd.merge(tmp1, tmp2, on='article_id')
            tmp2.drop(['message_count', 'messages'], axis=1, inplace=True)
            tmp2.to_sql('article', conn, if_exists = 'append', index = False)
            del tmp1, tmp2
        else:
            # 根據留言數量展開列數量
            tmp1 = all_ptt_df.loc[[i]]
            # 展開每篇 推/噓/→ 統計 cols = ['all', 'boo', 'count', 'neutral', 'push']
            tmp2 = pd.DataFrame.from_records([tmp1.squeeze()['message_count']])
            tmp2['article_id'] = article_id
            tmp2 = tmp2.rename({'all':'all_count'}, axis=1)
            tmp2 = pd.merge(tmp1, tmp2, on='article_id')
            tmp2.drop(['message_count', 'messages'], axis=1, inplace=True)
            tmp2.to_sql('article', conn, if_exists = 'append', index = False)
            # === 留言的年份 ===
            # 留言沒有年份，所以僅能依照發布文章的年份對照。如果文章沒有日期，則統一先給予年份：1900
            if pd.isna(tmp1.loc[i]['date']):
                push_year = '1900'
            else:
                push_year = str(tmp1.loc[i]['date'].year)
            # 展開每則留言 cols = ['push_content', 'push_tag', 'push_userid', 'push_user_ip']
            tmp3 = pd.DataFrame.from_records(all_ptt_df.loc[i]['messages'])
            tmp3['push_time'], tmp3['push_user_ip'] = zip(*tmp3['push_ipdatetime'].apply(lambda x:transform_push_ip_datetime(x, push_year)))
            tmp3['a_id'] = article_id
            tmp3.drop(['push_ipdatetime'], axis=1, inplace=True)
            tmp3.to_sql('comments', conn, if_exists = 'append', index = False)
            del tmp1, tmp2, tmp3
    except Exception as e:
        error[i] = article_id
        print(f'index: {i}, article id: {article_id}')
        print(e)
        pass

# Commit the changes and close the connection
conn.commit()
conn.close()