from flask import Flask, request, jsonify
from dateutil.parser import parse
from news_base import NewsBase
from modules import get_topics_dynamic, extract_terms, get_time_series_trend, get_most_popular, get_coef, \
    get_term_ranks_dict, filter_by_popularity, filter_similar
import fasttext


app = Flask(__name__)
_role = None
configure_required_fields = ['urls', 'role']
analyze_required_fields = ['dateto', 'datefrom']
base_fn = 'news_base.csv'
fasttext_model = fasttext.load_model('fasttext_l50.bin')


@app.route('/api/configure', methods=['POST'])
def configure_api():
    if any([f not in request.json for f in configure_required_fields]):
        return 'Bad request', 400

    urls = request.json['urls']

    with open('urls.txt', 'w+') as f:
        f.seek(0)

        f.writelines([f'{url} <> {class_name}\n' for url, class_name in urls.items()])

    _role = request.json['role']

    return 'Successful', 200


@app.route('/api/reload', methods=['GET'])
def reload_news():
    news_dbase = NewsBase(base_fn)

    news_dbase.renew_from_rss()

    return 'Successful', 200


@app.route('/api/analyze', methods=['GET'])
def analyze():
    if any([f not in request.args for f in analyze_required_fields]):
        return 'Bad request', 400

    date_from = parse(request.args.get('datefrom'))
    date_to = parse(request.args.get('dateto'))
    if 'ntrends' not in request.args:
        ntrends = 10
    else:
        ntrends = int(request.args.get('ntrends'))

    news_dbase = NewsBase(base_fn).df

    news_dbase = news_dbase.loc[(news_dbase.date >= date_from.strftime('%Y-%m-%d')) &
                                (news_dbase.date <= date_to.strftime('%Y-%m-%d'))]

    news_dbase['terms'] = news_dbase.title.apply(extract_terms)

    dynamic_matrix, unique_terms = get_topics_dynamic(news_dbase, date_from.strftime('%Y-%m-%d'),
                                                      date_to.strftime('%Y-%m-%d'))

    d = dict(zip(range(dynamic_matrix.shape[0]), dynamic_matrix.sum(axis=1)))
    d = [k for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True)[:ntrends]]

    trends = dict()

    for i in d:
        trends[unique_terms[i]] = get_time_series_trend(dynamic_matrix[i])

    news_dbase['time_coef'] = news_dbase.date.apply(lambda x: get_coef(x, date_to))

    term_tf_rank = get_term_ranks_dict(news_dbase)

    news_dbase['rank'] = news_dbase.terms.apply(lambda x: get_most_popular(x, term_tf_rank))

    top_n_news = filter_by_popularity(news_dbase).title
    top_n_news = filter_similar(top_n_news)

    return jsonify({'trends': trends, 'top_news': top_n_news}), 200


if __name__ == '__main__':
    app.run()
