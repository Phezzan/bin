#!python
from requests import Session
from base64 import b64decode
from time import sleep
from os import environ as ENV

headers = []
session = Session()
session.headers = {
"accept": "application/json, text/plain, */*",
"accept-language": "en-US,en;q=0.8",
"cookie": ENV.get('COOKIE'),
"referer": ENV.get('REFERER'),
"sec-fetch-dest": "empty",
"sec-fetch-mode": "cors",
"sec-fetch-site": "same-origin",
"sec-gpc": "1",
"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
}

_url = ENV.get('URL')


def fetch_page(page: int):
    rsp = session.get(url=f"{_url}{page}")
    if rsp.status_code < 300:
        return rsp.json()
    raise ValueError(f"{rsp}")


def save_pages(pages: list):
    for n, page in enumerate(pages):
        data = page['data'].split(',')
        mime, encoding = data[0].split(';')
        if encoding.lower() in ('base64', 'b64'):
            data = b64decode(data[1], validate=True)
        format = page['format'].lower()
        with open(f"page-{n}.{format}", 'wb') as f:
            f.write(data)


FORMATS = {
    None: 'get',
    'image/.*': 'get',
    'video/.*': 'yt-dlp',
    'ext/mkv': 'yt-dlp',
    'ext/mp4': 'yt-dlp',
    'ext/webm': 'yt-dlp',
}


def get_args():
    pass


if __name__ == '__main__':
    pages = []
    for n in range(1, 54):
        sleep(3)
        pages.append(fetch_page(n))
    save_pages(pages)
