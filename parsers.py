from datetime import datetime
import requests as rq
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
from dateutil import parser


class BuhParser:
    '''
    Класс для парсинга новостей с сайта 'www.buhgalteria.ru'
    '''
    news_page = 'https://www.buhgalteria.ru'
    
    def get_urls(self, date_to: str):
        '''
        Функция для итеративного получения ссылок на статьи с новостями с текущей даты до указанной.
        Ссылки берутся с главной новостной страницы 'www.buhgalteria.ru/news'.
        Параметры:
            date_to (str): Крайняя дата для выгрузки ссылок в формате [%dd.%mm%.yyyy].
        Возвращает:
            Generator[str]: Генератор ссылок.
        '''
        date_to = datetime.strptime(date_to, '%d.%m.%Y')
        
        urls = []
        page_number = 1
        article_date_in_range = True
        
        while article_date_in_range:
            r = rq.get((self.news_page + '/news/?PAGEN_1={}').format(page_number))
            soup = bs(r.text, features='lxml')
            
            for article in soup.find('div', {'class': 'articles'}).find_all('article'):
                article_date = datetime.strptime(article.find('span').text, '%d.%m.%Y')
                
                if article_date < date_to:
                    article_date_in_range = False
                    break
                
                article_url = self.news_page + article.find('h3').find('a')['href']
                article_date_str = article_date.strftime('%d.%m.%Y')
                
                to_append = article_url, article_date_str
                yield to_append
                
            page_number += 1
    
    def get_article_data(self, url: str):
        '''
        Функция для получения заголовка и текста статьи по ссылке.
        Параметры:
            url (str): Ссылка на страницу с новостью.
        Возвращает:
            str: Заголовок статьи.
            str: Текст статьи.
        '''
        r = rq.get(url)
        soup = bs(r.content, features='lxml')
        
        article = soup.find('div', {'class': 'article'})
        
        header = article.header.h1.text
        
        text = ' '.join(p.text.replace('\xa0', ' ').strip() for p in article.find('div', {'class': 'text'}).find_all('p', recursive=False))
        to_remove = 'Следите за нашими новостями  Telegram, «ВКонтакте»'
        text = text[:-len(to_remove)] if text.endswith(to_remove) else text
        text = text.strip()
        
        return header, text
    
    def get_data(self, date_to: str):
        '''
        Функция для получения данных о новостях за период в формате pd.DataFrame.
        Параметры:
            date_to (str): Крайняя дата для выгрузки ссылок в формате [%dd.%mm%.yyyy].
        Возвращает:
            pd.DataFrame: Таблица с данными о новостях (дата, заголовок, текст).
        '''
        data = []
        
        for url, date in self.get_urls(date_to):
            header, text = self.get_article_data(url)
            data.append((date, header, text))
            
        return pd.DataFrame(data, columns=['date', 'title', 'text'])


class RSSParser:
    '''
    Класс для парсинга новостей из RSS-каналов.
    '''
    def get_articles_info(self, rss_url: str):
        '''
        Функция для итеративного получения ссылок, дат публикаций и заголовков всех статей с новостями из RSS-канала.
        Параметры:
            rss_url (str): Ссылка на RSS-канал.
        Возвращает:
            Generator[str, datetime.datetime, str]: Генератор ссылок, дат и заголовков.
        '''
        r = rq.get(rss_url, headers={'User-Agent': 'Chrome/50.0.2661.102'})
        soup = bs(r.content, features='xml')
        
        for item in soup.find_all('item'):
            article_url = item.link.text
            article_date = parser.parse(item.pubDate.text)
            article_title = item.title.text
            
            yield article_url, article_date, article_title
            
    def get_article_text(self, article_url, class_name):
        '''
        Функция для получения всего текста из статьи.
        Параметры:
            article_url (str): Ссылка на статью.
            class_name (str): Название класса html-тега, в дочерних тегах которого хранится нужная информация.
        Возвращает:
            str: Текст статьи.
        '''
        r = rq.get(article_url, headers={'User-Agent': 'Chrome/50.0.2661.102'})
        soup = bs(r.content, features='lxml')
        
        return ' '.join([re.sub(r'\s+', ' ', element.text) for element in soup.find_all(True, {'class': class_name})])
            
    def get_articles_data(self, rss_url, class_name):
        '''
        Функция для получения данных о всех новостях RSS-канала в формате pd.DataFrame.
        Параметры:
            rss_url (str): Ссылка на RSS-канал.
            class_name (str): Название класса html-тега, в дочерних тегах которого хранится нужная информация.
        Возвращает:
            pd.DataFrame: Таблица с данными о новостях (дата, заголовок, текст).
        '''
        data = []
        for article_url, article_date, article_title in self.get_articles_info(rss_url):
            print(f'Дата обрабатываемой новости: {article_date}', end='\r')
            data.append((article_date, article_title, self.get_article_text(article_url, class_name)))

        return pd.DataFrame(data, columns=['date', 'title', 'text'])