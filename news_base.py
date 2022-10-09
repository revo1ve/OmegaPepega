import datetime
import os
import pandas as pd
from datetime import timedelta, datetime
from parsers import RSSParser


class NewsBase(object):
    def __init__(self, filename):
        self.filename = filename
        if os.path.exists(self.filename):
            self.df = pd.read_csv(self.filename, encoding='utf-8')
            self.df['date'] = pd.to_datetime(self.df.date, utc=True)
            self.df = self.df.loc[self.df.date > (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d')]
            self.df = self.df.sort_values(by='date')
        else:
            self.df = None
        self.width = timedelta(hours=6)

    def reload_from_disk(self):
        if os.path.exists(self.filename):
            self.df = pd.read_csv(self.filename, encoding='utf-8')

        else:
            raise ValueError()

    def validate(self, date_from, date_to):
        if not os.path.exists(self.filename):
            return False

        self.reload_from_disk()

        df = self.df

        period_df = df.loc[(df.date >= date_from) & (df.date <= date_to)]

        c_date = date_from

        while c_date + self.width < date_to:
            if period_df.loc[(period_df.date >= c_date) & (period_df.date <= c_date+self.width)].shape[0] == 0:
                return False

        return True

    def renew_from_rss(self):
        with open('urls.txt', 'r') as f:
            urls = f.readlines()
            urls = {l.split(' <> ')[0]: l.split(' <> ') for l in urls}

        rss_parser = RSSParser()

        dfs = list()

        for url, class_name in urls.items():
            dfs.append(rss_parser.get_articles_data(url, class_name))
            print(f'Processed: {url}')

        if self.df is not None:
            dfs.append(self.df)

        self.df = pd.concat(dfs)
        self.df['date'] = pd.to_datetime(self.df.date, utc=True)
        self.df = self.df.drop_duplicates(['date', 'title'])
        self.df = self.df.sort_values(by='date')
        self.df.to_csv(self.filename, encoding='utf-8', index=False)

