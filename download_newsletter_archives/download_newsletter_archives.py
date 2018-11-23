#!/usr/bin/env python
"""Download newsletter archives

Version 0.1
2018-10-10
"""
import re
import os
from os.path import dirname
import sys
import urllib
import argparse
import time
from io import BytesIO
from contextlib import contextmanager

import logging
# 0 NOTSET
# 10 DEBUG
# 20 INFO
# 30 WARN, WARNING
# 40 ERROR
# 50 CRITICAL, FATAL

from sqlalchemy import (
    Text, Integer,
    ForeignKey,
    Column, TEXT, create_engine, or_
)
from sqlalchemy.orm import relationship
from sqlalchemy.schema import MetaData
from sqlalchemy.ext.declarative import declarative_base

import requests
from bs4 import BeautifulSoup
import newspaper
import pdfminer
import slate


this_dir = dirname(os.path.abspath(__file__))
sys.path.append(dirname(dirname(this_dir)))

from chromatic_news.dbutils import Base, create_tables, pkey
# from chromatic_news.dbutils import drop_tables

from chromatic_news.download_newsletter_archives.config import (
    connstr, engine, logger, default_logging_level
)

log_levels = sorted([
    (a, getattr(logging, a)) for a in dir(logging) if a.upper()==a and isinstance(getattr(logging, a),int)
], key=lambda l: l[1])
log_level_dict = {name: integer for name, integer in log_levels}
if logger is None:
    logger = logging.getLogger(__name__)

if engine is None:
    engine = create_engine(connstr)
schema_name = 'chromatic'
SABase = declarative_base(
    metadata=MetaData(
        bind=engine,
        schema=schema_name,
    ),
)
ignore_domains_file = os.path.join(this_dir, 'ignore_domains.txt')


class Counter:
    requests_successful = 0
    requests_total = 0


@contextmanager
def timer():
    runtime = dict()
    start = time.time()
    yield runtime
    total_seconds = time.time() - start
    runtime['seconds'] = total_seconds


# check memory location equality using
# "if somevar is empty_response"
empty_response = object()


def modify_get_request(func, interactive=False, timeout_seconds=10, update_ignore_domains_on_403=False, ignore_domains=[]):
    def new_requests_get(*args, **kwargs):
        url = args[0]
        logging.info('requesting {}'.format(url))

        if interactive:
            input('ready to request \'{}\'? '.format(url))

        with timer() as runtime:
            try:
                Counter.requests_total += 1
                resp = func(*args, timeout=timeout_seconds, **kwargs)
                Counter.requests_successful += 1
            except requests.exceptions.ReadTimeout:
                logging.info("requesting '{}' took longer than the {} timeout seconds".format(url, timeout_seconds))
                return empty_response
            except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.MissingSchema,
                    requests.exceptions.SSLError,
                    requests.exceptions.TooManyRedirects
            ) as e:
                print(type(e), e)
                if interactive:
                    logging.info(str(e))
                    input("failed to request '{}'; using empty response ".format(url))
                return empty_response
            except Exception as e:
                print('Unhandled Exception:', type(e), e)
                raise

        if update_ignore_domains_on_403 and resp.status_code == 403:
            url_domain = netloc(url)
            ignore_domains.append(url_domain)
            with open(ignore_domains_file, 'a') as fa:
                print(url_domain, file=fa)

        total_seconds = runtime['seconds']
        logging.info("requesting '{}' took {:.2f} seconds".format(
            url, total_seconds
        ))
        return resp

    # try to make the new requests.get as similar
    # as possible to the original requests.get
    for attr in dir(func):
        try:
            setattr(new_requests_get, attr, getattr(func, attr))
        except:
            pass
    return new_requests_get


def load_ignore_domains():
    ignore_domains = list()
    with open(ignore_domains_file, 'r') as fr:
        for line in fr:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            ignore_domains.append(line.lower())
    return ignore_domains


def filter_urls_by_ignore_domains(urls, ignore_domains):
    filtered_urls = list()
    for url in urls:
        keep = True
        for ignore_domain in ignore_domains:
            if ignore_domain in url.lower():
                keep = False
                break
        if keep:
            filtered_urls.append(url)
    return filtered_urls


def pdf_bytes_to_content_string(bytes_content):
    bio = BytesIO()

    # can't do
    # bytes_content = bytes_content.replace(b'\x00', b'')
    # to fix the \x00 error because:
    # pdfminer.pdfparser.PDFSyntaxError: stream with no endstream

    bio.write(bytes_content)
    bio.seek(0)
    # note that this is a very cpu-intensive
    # line: parsing a pdf.
    try:
        pdf = slate.PDF(bio)
    except pdfminer.psparser.PSEOF:
        return None
    return pdf.text()


@contextmanager
def temporary_log_level(level_during, level_after):
    """
    the main purpose of this is to silence the thousands of
    logging messages created by slate and PDFMiner
    """
    logging.disable(level_during-10)
    yield
    logging.basicConfig(level=level_after)


extract_url_re = re.compile(r'https?://[^ ]+')
def clean_urls(all_urls):
    """
    input:  ['https://google.com', 'http https://banana.com asdf', 'mailto:bob@gmail.com']
    output: ['https://google.com', 'https://banana.com',                                 ]
    """
    cleaned_urls = list()
    for url in all_urls:
        urls = extract_url_re.findall(url)
        if not urls:
            continue
        cleaned_urls.append(urls[0])
    return cleaned_urls


video_audio_formats = 'mpg mpeg mp4 wav mp3 aicc m4a aiff m4p m4v'.split()


def filter_out_image_urls(urls):
    keep_urls = list()
    for url in urls:
        extension = urllib.parse.urlparse(url).path.split('.')[-1].lower()

        if extension in ('png', 'jpg', 'bmp', 'jpeg', 'tiff'):
            continue

        is_video_or_audio = False
        if len(extension) <= 5:
            for fmt in video_audio_formats:
                if fmt in extension:
                    is_video_or_audio = True
                    break
        if is_video_or_audio:
            continue

        keep_urls.append(url)
    return keep_urls


class Webpage:
    """Core functionality for all webpages
    """
    def ensure_full_html_and_bs(self, sess):
        url = self.url
        if self.url is None:
            url = self.discovery_url
        if self.full_html is None:
            resp = requests.get(url)
            self.status = resp.status_code
            self.full_html = resp.content.decode()
            if self.url is None:
                self.url = resp.url

        self.bs = BeautifulSoup(self.full_html, 'html.parser')
        sess.commit()


class NewsletterArchive(SABase, Base, Webpage):
    __tablename__ = 'newsletter_archives'
    nlaid = pkey('nlaid')
    url = Column('url', TEXT)
    full_html = Column('full_html', TEXT)
    status = Column('status', Integer)

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.url))

    __repr__ = __str__

    def extract_newsletter_urls(self):
        base_url = urllib.parse.urlparse(self.url)
        base_url = '{}://{}'.format(base_url.scheme, base_url.netloc)
        for a in self.bs.find_all('a'):
            url = a.attrs['href']

            if url.startswith('/'):
                url = urllib.parse.urljoin(base_url, url)

            urls = extract_url_re.findall(url)
            if not urls:
                continue
            yield urls[0]

class Newsletter(SABase, Base, Webpage):
    __tablename__ = 'newsletters'
    nlid = pkey('nlid')
    url = Column('url', TEXT)
    discovery_url = Column('discovery_url', TEXT)
    full_html = Column('full_html', TEXT)

    nlaid = Column('nlaid', Integer, ForeignKey('newsletter_archives.nlaid'))
    newsletter_archive = relationship('NewsletterArchive', backref='newsletters')

    def __init__(self, discovery_url, newsletter_archive):
        """Create a new Newsletter instance

        It is expected that this is being created because the
        corresponding url doesn't exist in the database yet.
        """
        # remember that a new Newsletter object/record is only created
        # if one doesn't already exist for the given url, which is
        # why we immediately make a requests.get
        resp = requests.get(discovery_url)

        self.full_html = resp.content.decode()
        self.status = resp.status_code
        self.url = resp.url

        self.discovery_url = discovery_url
        self.nlaid = newsletter_archive.nlaid

        self.bs = BeautifulSoup(self.full_html, 'html.parser')

    @classmethod
    def ensure_and_get_newsletter(cls, sess, newsletter_url, newsletter_archive):
        newsletter = sess.query(cls).filter(or_(
            cls.discovery_url==newsletter_url,
            cls.url==newsletter_url
        )).one_or_none()

        if newsletter is None:
            newsletter = cls(newsletter_url, newsletter_archive)
            # need to set html and text for new records, but we
            # also need to call ensure_full_html_and_bs again
            # below for existing records that don't have their
            # bs set yet..
            newsletter.ensure_full_html_and_bs(sess)
            sess.add(newsletter)
            sess.commit()
        newsletter.ensure_full_html_and_bs(sess)
        return newsletter

    def extract_article_urls(self, ignore_domains=list()):
        all_urls = [
            a.attrs['href']
            for a in self.bs.find_all('a')
            if 'href' in a.attrs
        ]
        all_urls = clean_urls(all_urls)

        # only consider valid domains. e.g. ignore "mailto:..." links.
        filtered_urls = sorted(set([
            u for u in filter_urls_by_ignore_domains(all_urls, ignore_domains)
            # confirm that it's a url with a real domain
            if netloc(u)
        ]))
        filtered_urls = filter_out_image_urls(filtered_urls)
        return filtered_urls

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.url))
    __repr__ = __str__


class Article(SABase, Base, Webpage):
    __tablename__ = 'articles'
    aid = pkey('aid')
    discovery_url = Column('discovery_url', TEXT)

    url = Column('url', Text)
    full_text = Column('full_text', Text)
    full_html = Column('full_html', TEXT)
    title = Column('title', TEXT)
    status = Column('status', Integer)

    nlid = Column('nlid', Integer, ForeignKey('newsletters.nlid'))
    newsletter = relationship('Newsletter', backref='articles')

    @staticmethod
    def __get_url_fulltext_fullhtml_title_statuscode(url):
        resp = requests.get(url)
        if resp is empty_response or not resp.content:
            return None

        is_pdf = (
            'Content-Type' in resp.headers
            and 'application/pdf' in resp.headers['Content-Type']
        )

        if is_pdf:
            level_during = logging.WARNING
            with timer() as runtime, temporary_log_level(level_during, default_logging_level):
                full_html = full_text = pdf_bytes_to_content_string(resp.content)
            # pdfminer.psparser.PSEOF
            if full_html is None:
                return None
            full_text = full_text.replace(b'\x00'.decode(), '')
            full_html = full_html.replace(b'\x00'.decode(), '')

            total_seconds = runtime['seconds']
            if total_seconds > 10:
                logger.info("pdf conversion for url '{}' took {:.2f} seconds".format(url, total_seconds))

            title = os.path.basename(url)
        else:
            article = newspaper.Article(url, fetch_images=False)
            # apparently, newspaper3k is smart when it comes
            # to encodings..
            article.download(input_html=resp.content)
            article.parse()
            full_text = article.text.replace('\x00', '')
            full_html = article.html.replace('\x00', '')
            title = article.title.replace('\x00', '')
        return resp.url, full_text, full_html, title, resp.status_code

    @classmethod
    def create_new_article(cls, sess, discovery_url, newsletter, manual=False):
        contents = cls.__get_url_fulltext_fullhtml_title_statuscode(discovery_url)
        if contents is None:
            return
        url, full_text, full_html, title, status_code = contents

        self = cls()
        self.nlid = newsletter.nlid
        self.discovery_url = discovery_url
        self.status = status_code

        self.full_text = full_text
        self.url = url
        self.full_html = full_html
        self.title = title

        sess.add(self)
        sess.commit()
        return self

    @classmethod
    def ensure_and_get_article(cls, sess, discovery_url, newsletter):
        articles = sess.query(cls).filter(or_(
            cls.url==discovery_url,
            cls.discovery_url==discovery_url,
        )).all()
        if articles:
            article = articles[0]
        else:
            article = cls.create_new_article(sess, discovery_url, newsletter)
            if article is None:
                return None
            sess.add(article)
            sess.commit()
        return article

    def __str__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.url,
        )
    __repr__ = __str__


def ensure_base_sources_in_db(sess, urls):
    do_query = lambda url: sess.query(NewsletterArchive).filter(NewsletterArchive.url == url).one_or_none()
    for url in urls:

        nla = do_query(url)

        if nla is None:
            nla = NewsletterArchive()
            nla.url = url
            sess.add(nla)
            sess.commit()
    return sess.query(NewsletterArchive).filter(
        NewsletterArchive.url.in_(urls)
    )


def netloc(url):
    return urllib.parse.urlparse(url).netloc


def read_newsletter_archive_urls(filepath=None):
    if filepath is None:
        filepath = os.path.join(this_dir, 'newsletter_archive_urls.txt')
    with open(filepath, 'r') as fr:
        for line in fr:
            line = line.strip()
            if line.startswith('#'):
                continue
            if not line:
                continue
            yield line


def convert_log_level_to_int(level):
    if isinstance(level, int) and level % 10 == 0:
        return level

    if level.isnumeric():
        level = int(level)

    elif isinstance(level, str):
        level = level.upper()
        if level in log_level_dict:
            return log_level_dict[level]
    return None


def run_main():
    args = parse_cl_args()
    verbose = args.verbose
    requests_limit = args.requests_limit
    articles_per_archive = args.articles_per_archive

    log_level = convert_log_level_to_int(args.log_level)
    if log_level is None:
        print("invalid log level '{}' specified; exiting".format(args.log_level))
        exit(1)
    logging.disable(log_level)
    logging.basicConfig(level=log_level)

    ignore_domains = load_ignore_domains()

    # modify behavior of requests.get
    requests.get = modify_get_request(
        requests.get,
        interactive=args.interactive,
        timeout_seconds=args.timeout_seconds,
        update_ignore_domains_on_403=args.update_ignore_domains_on_403,
        ignore_domains=ignore_domains,
    )

    Base.set_sess(engine)
    # drop_tables(SABase)
    create_tables(engine, SABase, schema_name)
    stop = False

    with Base.get_session() as sess:
        for newsletter_archive_url in read_newsletter_archive_urls():
            if stop:
                break
            # newsletter_archives get sess.add()ed here.
            newsletter_archives = ensure_base_sources_in_db(sess, [newsletter_archive_url])
            for newsletter_archive in newsletter_archives:
                if stop:
                    break

                num_articles_downloaded_this_archive = 0
                finished_this_archive = False
                newsletter_archive.ensure_full_html_and_bs(sess)
                newsletter_urls = newsletter_archive.extract_newsletter_urls()

                for newsletter_url in newsletter_urls:
                    if stop:
                        break
                    # enforce the articles-per-archive limit
                    if finished_this_archive:
                        break

                    # first filter by site-specific thingies..
                    newsletter = Newsletter.ensure_and_get_newsletter(sess, newsletter_url, newsletter_archive)
                    filtered_article_urls = newsletter.extract_article_urls(ignore_domains)

                    for i, discovered_article_url in enumerate(filtered_article_urls):

                        try:
                            article = Article.ensure_and_get_article(sess, discovered_article_url, newsletter)
                        except Exception as e:
                            if args.debug:
                                print('Caught Exception:', e)
                                try:
                                    import ipdb as pdb
                                except ImportError:
                                    import pdb
                                pdb.set_trace()
                                article = Article.ensure_and_get_article(sess, discovered_article_url, newsletter)
                            else:
                                raise

                        # enforce articles-per-archive limit
                        if articles_per_archive:
                            num_articles_downloaded_this_archive += 1
                            finished_this_archive = num_articles_downloaded_this_archive > articles_per_archive
                            if finished_this_archive:
                                break

                        if article and verbose:
                            print('\n', article.title, article.url)
                        if requests_limit and Counter.requests_total >= requests_limit:
                            stop = True
                            break

    print('{} requests attempted'.format(Counter.requests_total))
    print('{} requests successful'.format(Counter.requests_successful))

    success = True
    return success


def parse_cl_args():

    argParser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    argParser.add_argument(
        '-i', '--interactive', default=False, action='store_true',
        help="each time a download is about to occur, wait for user\n"
            "input first. by default, don't ask for confirmation.",
    )
    argParser.add_argument(
        '--requests-limit', default=False, type=int,
        help="limit to a given integer number of requests. This\n"
            "isn't perfect, because it only checks how many requests\n"
            "were made each time an article (as opposed to a newsletter\n"
            "archive page or a newsletter page) is downloaded.",
    )
    argParser.add_argument(
        '-v', '--verbose', default=False, action='store_true',
        help="after downloading each article, print its title and url.",
    )
    argParser.add_argument(
        '--log-level', default=default_logging_level,
        help="string or integer, one of: {}".format(
            log_levels,
        ),
    )
    argParser.add_argument(
        '--debug', default=False, action='store_true',
        help="when fetching an article fails, launch the debugger and retry the fetch",
    )
    argParser.add_argument(
        '--timeout-seconds', default=10, type=int,
        help="timeout seconds, default %(default)s",
    )
    argParser.add_argument(
        '--update-ignore-domains-on-403', default=False, action='store_true',
        help="when a url returns a 403 (forbidden), add the domain to the ignore_domains.txt file",
    )
    argParser.add_argument(
        '--articles-per-archive', default=25, type=int,
        help="download a different number of articles per archive; default %(default)s; use 0 for no limit",
    )

    args = argParser.parse_args()
    return args


if __name__ == '__main__':
    success = run_main()
    exit_code = 0 if success else 1
    exit(exit_code)
