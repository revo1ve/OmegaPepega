import pandas as pd
import requests

r = requests.post('http://127.0.0.1:5000/api/configure',
                  json={'role': 'accountant',
                        'urls': {'http://static.feed.rbc.ru/rbc/logical/footer/news.rss': 'article__text',
                                 'https://ria.ru/export/rss2/archive/index.xml': 'article__text',
                                 'https://www.vedomosti.ru/rss/news': 'paragraph__text',
                                 'https://www.bfm.ru/news.rss': 'current-article'}})

assert r.status_code == 200
print('Configured!')

base_fn = 'news_base.csv'
base_old = pd.read_csv(base_fn, encoding='utf-8')

r = requests.get('http://127.0.0.1:5000/api/reload')

base_new = pd.read_csv(base_fn, encoding='utf-8')

assert r.status_code == 200
print(f'Reloaded! {base_old.shape[0]} -> {base_new.shape[0]}')

r = requests.get('http://127.0.0.1:5000/api/analyze?datefrom=2022-10-06&dateto=2022-10-09&ntrends=25')

assert r.status_code == 200
print(r.json()['top_news'])
print(r.json()['trends'])
