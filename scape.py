import bs4 as bs
import urllib.request
import pandas as pd
from multiprocessing import Pool
import json
import datetime


def get_article_body(href):
    url = "https://www.tagesschau.de" + href
    sauce = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(sauce, features="html.parser")
    script = soup.find('script', attrs={'type': 'application/ld+json'})
    data = json.loads(script.text)
    return data["articleBody"]

def filter_all(headlines, short_headlines, short_text, links):
    indices = [i for i, x in enumerate(links) if x.startswith("/")]
    headlines = [headlines[i] for i in indices]
    short_headlines = [short_headlines[i] for i in indices]
    short_text = [short_text[i] for i in indices]
    links = [links[i] for i in indices]
    return headlines, short_headlines, short_text, links

def get_article_bodies_multiprocessing(links):
    with Pool(10) as p:
        return p.map(get_article_body, links)

def load_content(date):
    # url format url + ?datum=2018-07-01
    url = "https://www.tagesschau.de/archiv?datum=" + date
    sauce = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(sauce, features="html.parser")
    content = soup.find('div', attrs={'id': 'content'})
    return content.findChildren("div", attrs={'class': 'copytext-element-wrapper__vertical-only'})

def get_metadata(children):
    headlines = [child.find('span', attrs={'class': 'teaser-right__headline'}).text for child in children[2:]]
    short_headlines = [child.find('span', attrs={'class': 'teaser-right__labeltopline'}).text for child in children[2:]]
    # dates = [child.find('div', attrs={'class': 'teaser-right__date'}).text for child in children[2:]]
    short_text = [child.find('p', attrs={'class': 'teaser-right__shorttext'}).text for child in children[2:]]
    links = [child.find('a', attrs={'class': 'teaser-right__link'})['href'] for child in children[2:]]
    return headlines, short_headlines, short_text, links


def get_articles(date):
    children = load_content(date)
    content = get_metadata(children)
    headlines, short_headlines, short_text, links = filter_all(*content)
    articles = get_article_bodies_multiprocessing(links)
    df = pd.DataFrame({'date': [date]*len(headlines), 'headline': headlines, 'short_headline': short_headlines, 'short_text': short_text, 'article': articles, 'link': links})
    return df


def generate_dates_reverse_chronologically():
    today = datetime.date.today()
    delta = datetime.timedelta(days=1)
    while today >= datetime.date(2023, 1, 1):
        yield today.strftime("%Y-%m-%d")
        today -= delta


if __name__ == "__main__":
    all_df = []
    for date in generate_dates_reverse_chronologically():
        print(date)
        all_df.append(get_articles(date))
