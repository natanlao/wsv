'''
Load posts and comments saved by scrape.py into an SQLite database
'''
import argparse
import datetime
import sqlite3

from scrape import RedditCache


class Database:

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.c = self.conn.cursor()

    def init_db(self):
        # TODO: Brittle coupling
        self.c.execute('''CREATE TABLE posts
                          (id integer, title text, selftext text, num_comments integer, score integer, created text)''')
        self.c.execute('''CREATE TABLE comments
                          (id integer, body text, score integer, created text)''')
        self.conn.commit()

    def load(self, cache: RedditCache):
        comments = ((
            b36decode(comment.id),
            comment.body,
            comment.score,
            timestamp_dbformat(comment.created)
        ) for comment in cache.comments)
        self.c.executemany('INSERT INTO comments VALUES (?, ?, ?, ?)', comments)

        posts = ((
            b36decode(post.id),
            post.title,
            post.selftext,
            post.num_comments,
            post.score,
            timestamp_dbformat(post.created)
        ) for post in cache.posts)
        self.c.executemany('INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?)', posts)

        self.conn.commit()


def b36decode(num: str) -> int:
    return int(num, 36)


def timestamp_dbformat(epochtime: int) -> str:
    timestamp = datetime.datetime.utcfromtimestamp(epochtime)
    timestamp -= datetime.timedelta(hours=5)  # UTC -> ET
    return timestamp.strftime('%Y-%m-%dT%H:00:00')  # truncate to the hour


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('subreddit_name')
    parser.add_argument('--db-path')
    parser.add_argument('--cache-dir', default='.')
    arguments = parser.parse_args()

    cache = RedditCache(arguments.cache_dir, arguments.subreddit_name)
    if not arguments.db_path:
        db_path = (cache.cache_dir / f'{arguments.subreddit_name}.db').as_posix()
    else:
        db_path = arguments.db_path

    db = Database(db_path=db_path)
    db.init_db()
    db.load(cache)
