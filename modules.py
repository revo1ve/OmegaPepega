from rutermextract import TermExtractor
from datetime import datetime
from tqdm import tqdm
import numpy as np
from sklearn.linear_model import LinearRegression
import math
from rake_nltk import Rake
import fasttext
from sklearn.metrics.pairwise import cosine_distances
import jieba
from gensim import corpora, models, similarities


def extract_terms(text):
    t_extr = TermExtractor()

    return [t.normalized for t in t_extr(text, limit=7)]


def get_freqs_for_term(data, aterm):
    return [term for aterms in data.terms for term in aterms].count(aterm)


def get_topics_dynamic(data, date_from, date_to, bins=24):
    cdata = data.loc[(data.date > date_from) & (data.date < date_to)]

    date_to = datetime.strptime(date_to, '%Y-%m-%d')
    date_from = datetime.strptime(date_from, '%Y-%m-%d')

    unique_terms = np.unique([term for aterms in cdata.terms for term in aterms])

    bin_width = (date_to - date_from) / bins

    dynamic_matrix = np.zeros((len(unique_terms), bins))

    for i, term in tqdm(enumerate(unique_terms)):
        bin_start = date_from.replace(tzinfo=cdata.iloc[0].date.tzinfo)
        bin_end = bin_start + bin_width

        for j in range(1, bins + 1):
            bin_start += bin_width
            bin_end += bin_width

            dynamic_matrix[i, j - 1] = get_freqs_for_term(cdata.loc[(cdata.date > bin_start) &
                                                                    (cdata.date <= bin_end)], term)

    return dynamic_matrix, unique_terms


def get_time_series_trend(data):
    X = np.array(range(len(data))).reshape((-1, 1))

    trend = LinearRegression().fit(X, data).predict(X)

    return round(math.atan((trend[-1] - trend[0]) / len(data)) * (180 / math.pi), 3)


def extract_thesis(text):
    rake = Rake()

    rake.extract_keywords_from_text(text)
    keywords = rake.get_ranked_phrases()

    return keywords


def make_keywords(fasttext_model):
    key_words_accountant = """законодательсво, нормативный акт, контрагент, деньги, счёт, работа, зарплата, бухгалтерия, человек, отчёт, профессия, учёт, бумаги, кредит, считать, калькулятор, должность, дебет, финансы, банк, экономист, баланс, работник, женщина, документы, кассир, цифры, счёты, расчёт, контора, расход, кабинет, касса, компьютер, математика, отчётность, фирма, экономика, офис, актив, налоги, подпись, программа, счетовод"""
    key_words_director = """мужчина, работа, фирма, приказ, зарплата, компания, предприятие, должность, завод, офис, деньги, организация, генеральный директор, заместитель, документы, ответственность, магазин, президент, директор предприятия, подчинённые, увольнение, бизнес, указ, фабрика, совещание, менеджер, зам, выговор, отчёт, дело, наказание, собрание, работник, повышение, указание, главный директор, дирекция, карьера, сотрудник, финансы, эффективность, рост, масштабирование, потребности клиента, санкции"""

    key_words_vect = {'accountant': [], 'director': []}

    for profession, words in zip(['accountant', 'director'], [key_words_accountant, key_words_director]):
        for word in words.split(', '):
            key_words_vect[profession].append(fasttext_model.get_sentence_vector(word).reshape((1, 50)))

    return key_words_vect


def get_definition_belonging_to_role(key_words_in_sentence, fasttext_model, key_words_vect):
    for word_in_sentence in key_words_in_sentence:
        word_in_sentence_vec = fasttext_model.get_sentence_vector(word_in_sentence).reshape((1, 50))
        res_cosine_distances = {'accountant': 0, 'director': 0}

        for words, profession in zip(key_words_vect.values(), key_words_vect.keys()):
            sum_cosine_distances = 0
            for word in words:
                sum_cosine_distances += cosine_distances(word, word_in_sentence_vec)[0][0]

            res_cosine_distances[profession] += sum_cosine_distances / len(words)

    return res_cosine_distances


def get_most_popular(x, term_fr_rank):
    rel = 0
    if len(x) == 0:
        return 0
    for t in x:
        rel += term_fr_rank[t]
    return rel / len(x)


def get_coef(d, date_to):
    d = 1 / math.sqrt((date_to.timestamp() - datetime.timestamp(d)) / 500000 + 1)
    return d


def get_term_ranks_dict(df):
    term_fr_rank = {}

    def get_term_ranks(x):
        for t in x['terms']:
            keys = term_fr_rank.keys()
            if t not in keys:
                term_fr_rank[t] = (x['time_coef'], 1)
            else:
                term_fr_rank[t] = (term_fr_rank[t][0] + x['time_coef'], term_fr_rank[t][1] + 0.7)

    df.apply(get_term_ranks, axis=1)

    term_fr_rank = {k: v[0] / v[1] for k, v in term_fr_rank.items()}
    term_fr_rank = {k: v for k, v in sorted(term_fr_rank.items(), key=lambda item: item[1], reverse=True)}

    return term_fr_rank


def filter_by_popularity(df, n=10):

    return df.nlargest(n, 'rank')


def filter_similar(texts):
    length = len(texts)
    cross_text = [([word for word in jieba.cut(i)], [[word for word in jieba.cut(j)] for j in texts if j != i]) for i in
                  texts]

    cross_dictionary = [corpora.Dictionary(i[1]) for i in cross_text]

    def cross(i, cross_dictionary):
        dictionary = cross_dictionary[i]
        feature_cnt = len(dictionary.token2id.keys())
        corpus = [dictionary.doc2bow(text) for text in cross_text[i][1]]
        vector = dictionary.doc2bow(cross_text[i][0])
        tfidf = models.TfidfModel(corpus)
        index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)
        sim = index[tfidf[vector]]
        return [value for value in sim]

    sim_matrix = [cross(i, cross_dictionary) for i in range(length)]
    for i in range(len(sim_matrix)):
        sim_matrix[i].insert(i, 1)

    shape_matrix = len(sim_matrix)

    similar_texts = []
    for i in range(shape_matrix):
        for j in range(i):
            if i == j:
                continue
            if (sim_matrix[i][j] + sim_matrix[j][i]) / 2 > 0.65:
                similar_texts.append(j)

    return [text for i, text in enumerate(texts) if i not in similar_texts]
