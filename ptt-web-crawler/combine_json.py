import os
import json
import glob
import pickle
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# PATH = '/Users/hsiaoshihchen/NCCU/Polarity/ptt-web-crawler/data/crawler_test'
PATH = '/home/kellychen/Polarity/ptt-web-crawler/PttWebCrawler'

all_ptt_json = glob.glob(os.path.join(PATH, 'data', '*.json'))
all_ptt_json.sort()  # 一次性排序
data = {}  # 主字典
error_page = []

def process_json(json_file):
    try:
        with open(json_file, 'r') as f:
            tmp = json.load(f)
        return tmp['articles']
    except (FileNotFoundError, json.JSONDecodeError) as e:
        error_page.append(json_file)
        print(f'Error: {json_file}')
        return []

# 先讀取第一頁(11104)為主dict，方便後續加進來
with open(all_ptt_json[0], 'r') as f:
    data = json.load(f)

# 處理剩餘頁面
with ThreadPoolExecutor() as executor:
    future_to_file = {executor.submit(process_json, json_file): json_file for json_file in all_ptt_json[1:]}

    for future in tqdm(as_completed(future_to_file)):
        json_file = future_to_file[future]
        try:
            tmp = future.result()
            data['articles'].extend(tmp)
            # print(f'{json_file} Done.')
        except Exception as e:
            pass  
# error        
with ThreadPoolExecutor() as executor:
    future_to_file = {executor.submit(process_json, json_file): json_file for json_file in error_page}

    for future in tqdm(as_completed(future_to_file)):
        json_file = future_to_file[future]
        try:
            tmp = future.result()
            data['articles'].extend(tmp)
            # print(f'{json_file} Done.')
        except Exception as e:
            pass    

with open(os.path.join(PATH, 'all_ptt_2020_2022_test.pickle'), 'wb') as f:
    pickle.dump(data, f)

with open(os.path.join(PATH, 'error_ptt_2020_2022.pickle'), 'wb') as f:
    pickle.dump(error_page, f)    