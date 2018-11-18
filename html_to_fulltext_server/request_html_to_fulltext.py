#!/usr/bin/env python
import requests

url = 'https://news.mit.edu/2016/ai-system-predicts-85-percent-cyber-attacks-using-input-human-experts-0418'

webpage = requests.get(url)

resp = requests.post(
    'http://localhost:7295/html_to_fulltext',
    data=webpage.content,
    params={
        'url': url,
    }
)
print(resp.content)
