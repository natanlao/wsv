'''
Load posts and comments saved by scrape.py into an SQLite database
'''
import argparse
import datetime
import json
import pathlib
import sqlite3
import sys


def b36decode(num: str) -> int:
    return int(num, 36)


class Database:

    def __init__(self, db_path: str, posts_path: pathlib.Path, comments_path: pathlib.Path):
        self.conn = sqlite3.connect(db_path)
        self.c = self.conn.cursor()
        self.posts = posts_path
        self.comms = comments_path

    def init_db(self):
        self.c.execute('''CREATE TABLE posts
                          (id integer, title text, selftext text, created text)''')
        self.c.execute('''CREATE TABLE comments
                          (id integer, body text, created text)''')
        self.conn.commit()

    def _posts(self):
        for post in self.posts.glob('*.json'):
            with post.open('r') as post_fh:
                post = json.load(post_fh)
            yield {
                'id': b36decode(post['id']),
                'title': post['title'],
                'selftext': post['selftext'],
                'created': self._cast_epoch(post['created'])
            }

    def _comments(self):
        for comm in self.comms.glob('*.json'):
            with comm.open('r') as comm_fh:
                comm = json.load(comm_fh)
            yield {
                'id': b36decode(comm['id']),
                'body': comm['body'],
                'created': self._cast_epoch(comm['created'])
            }

    def _cast_epoch(self, epochtime: int) -> str:
        timestamp = datetime.datetime.utcfromtimestamp(epochtime)
        # Truncate to the hour
        timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
        return timestamp.isoformat()

    def load(self):
        comments = ((c['id'], c['body'], c['created']) for c in self._comments())
        self.c.executemany('INSERT INTO comments VALUES (?, ?, ?)', comments)
        posts = ((p['id'], p['title'], p['selftext'], p['created']) for p in self._posts())
        self.c.executemany('INSERT INTO posts VALUES (?, ?, ?, ?)', posts)
        self.conn.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db_path')
    parser.add_argument('--cache-dir', default='.')
    arguments = parser.parse_args()

    cache_dir = pathlib.Path(arguments.cache_dir)
    posts_dir = cache_dir / 'posts'
    cache_dir = cache_dir / 'comments'

    db = Database(db_path=arguments.db_path,
                  comments_path=cache_dir,
                  posts_path=posts_dir)
    db.init_db()
    db.load()
