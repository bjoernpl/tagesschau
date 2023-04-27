import bs4 as bs
import urllib.request
import pandas as pd
from multiprocessing import Pool
import json
import datetime
import argparse
from tqdm import tqdm


def get_article_bodies_multiprocessing(links):
    with Pool(8) as p:
        return p.map(get_article_body, links)


def get_article_body(href):
    """Get the article body from the href of the article. The HTML contains a script tag with the article body in JSON format.

    Arguments: 
        href {str} -- The href of the article

    Returns:
        str -- The article body"""
    try:
        url = "https://www.tagesschau.de" + href
        sauce = urllib.request.urlopen(url).read()
        soup = bs.BeautifulSoup(sauce, features="html.parser")
        scripts = soup.findAll('script', attrs={'type': 'application/ld+json'})
        for script in scripts:
            data = json.loads(script.text)
            if data["@type"] == "NewsArticle":
                return data["articleBody"]
        return None
    except:
        return None


def filter_all(headlines, short_headlines, short_text, links):
    """Filter out all articles that are not from the tagesschau.de domain.

    Arguments:
        headlines {list} -- List of headlines
        short_headlines {list} -- List of short headlines
        short_text {list} -- List of short texts
        links {list} -- List of links

    Returns:
        list -- List of filtered headlines
        list -- List of filtered short headlines
        list -- List of filtered short texts
        list -- List of filtered links
    """
    indices = [i for i, x in enumerate(links) if x.startswith("/")]
    headlines = [headlines[i] for i in indices]
    short_headlines = [short_headlines[i] for i in indices]
    short_text = [short_text[i] for i in indices]
    links = [links[i] for i in indices]
    return headlines, short_headlines, short_text, links


def load_content(date):
    """Load the content of the tagesschau.de archive for a given date.

    Arguments:
        date {str} -- The date in the format YYYY-MM-DD

    Returns:
        list -- List of children of the content div
    """
    # url format url + ?datum=2018-07-01
    url = "https://www.tagesschau.de/archiv?datum=" + date
    sauce = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(sauce, features="html.parser")
    content = soup.find('div', attrs={'id': 'content'})
    return content.findChildren(
        "div", 
        attrs={'class': 'copytext-element-wrapper__vertical-only'}
    )


def find_for_all(children, attr, value):
    """Find the value of an attribute for all children of a given list of children.

    Arguments:
        children {list} -- List of children
        attr {str} -- The attribute to find
        value {str} -- The value of the attribute

    Returns:
        list -- List of values
    """
    out = []
    for child in children[2:]:
        val = child.find(attr, attrs={'class': value})
        if val is not None:
            if attr == "a":
                out.append(val['href'])
            else:
                out.append(val.text)
        else:
            out.append("")
    return out


def get_metadata(children):
    """Get the metadata of the articles from the children of the content div.

    Arguments:
        children {list} -- List of children of the content div

    Returns:
        list -- List of headlines
        list -- List of short headlines
        list -- List of short texts
        list -- List of links
    """
    headlines = find_for_all(children, "span", "teaser-right__headline")
    short_headlines = find_for_all(children, "span", "teaser-right__labeltopline")
    short_text = find_for_all(children, "p", "teaser-right__shorttext")
    links = find_for_all(children, "a", "teaser-right__link")
    return headlines, short_headlines, short_text, links


def get_articles(date):
    """Get all articles from a given date.

    Arguments:
        date {str} -- The date in the format YYYY-MM-DD

    Returns:
        pd.DataFrame -- DataFrame with the columns date, headline, short_headline, short_text, article, link
    """
    children = load_content(date)
    content = get_metadata(children)
    headlines, short_headlines, short_text, links = filter_all(*content)
    articles = get_article_bodies_multiprocessing(links)
    df = pd.DataFrame({
        'date': [date]*len(headlines), 
        'headline': headlines,
        'short_headline': short_headlines, 
        'short_text': short_text, 
        'article': articles, 
        'link': links})
    return df


def generate_dates(start_date=datetime.date(2018, 1, 1), end_date=datetime.date.today()):
    """Generate a reverse chronological list of dates in the format YYYY-MM-DD.

    Keyword Arguments:
        start_date {datetime.date} -- The start date (default: {datetime.date(2018, 1, 1)})
        end_date {datetime.date} -- The end date (default: {datetime.date.today()})

    Yields:
        str -- The date in the format YYYY-MM-DD
    """
    day = end_date
    delta = datetime.timedelta(days=1)
    while day >= start_date:
        yield day.strftime("%Y-%m-%d")
        day -= delta


def n_days_between(start_date, end_date):
    """Calculate the number of days between two dates.

    Arguments:
        start_date {datetime.date} -- The start date
        end_date {datetime.date} -- The end date

    Returns:
        int -- The number of days
    """
    return (end_date - start_date).days + 1


def save(df, filename):
    """Save the DataFrame to a pickle or csv file.

    Arguments:
        df {pd.DataFrame} -- The DataFrame
        filename {str} -- The filename
    """
    if filename.endswith(".pkl"):
        df.to_pickle(filename)
    elif filename.endswith(".csv"):
        df.to_csv(filename, sep="\t", index=False)
    else:
        df.to_pickle(filename + ".pkl")
        raise ValueError("Unknown file format, saved as pickle file.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Scrape the tagesschau.de archive.'
    )
    parser.add_argument(
        '--start_date', 
        type=str, 
        default="2018-01-01",
        help='The start date in the format YYYY-MM-DD'
    )
    parser.add_argument(
        '--end_date', 
        type=str, 
        default=datetime.date.today().strftime("%Y-%m-%d"),
        help='The end date in the format YYYY-MM-DD'
    )
    parser.add_argument(
        '--output', 
        type=str,
        default="tagesschau.csv", 
        help='The output file'
    )
    args = parser.parse_args()
    start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()

    all_df = []
    n_days = n_days_between(start_date, end_date)
    day_gen = enumerate(generate_dates(start_date=start_date, end_date=end_date))
    for i, date in tqdm(day_gen, total=n_days, desc="Scraping days"):
        try:
            all_df.append(get_articles(date))
        except:
            print(f"Error on date {date}")
        # save every 100 days
        if i % 100 == 0:
            save(pd.concat(all_df), args.output)

    save(pd.concat(all_df), args.output)
    print(f"Done - saved to {args.output}")
