#!/usr/bin/env python
import falcon
import newspaper

class HtmlToFulltextResource:
    def on_post(self, req, resp):
        chunk = req.stream.read()

        url = req.params.get('url', None)
        article = newspaper.Article(url, fetch_images=False)

        article.download(input_html=chunk)
        article.parse()
        full_text = article.text.replace('\x00', '')
        # full_html = article.html.replace('\x00', '')
        # title = article.title.replace('\x00', '')
        resp.body = full_text

api = falcon.API()
api.add_route('/html_to_fulltext', HtmlToFulltextResource())
