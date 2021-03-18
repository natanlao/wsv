'''
Crawl as many recent posts for a subreddit as the Reddit API lets us.
'''
import argparse
import collections
import functools
import itertools
import json
import logging
import pathlib
from typing import Generator, NamedTuple

import praw
import prawcore.exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
reddit = praw.Reddit(user_agent='wsv v0.1 (https://github.com/natanlao/wsv)')


class RedditCache:

    def __init__(self, cache_dir: str, subreddit_name: str):
        subreddit = reddit.subreddit(subreddit_name)
        self.cache_dir = pathlib.Path(cache_dir)
        self.posts_dir = self.cache_dir / subreddit_name / 'posts'
        self.posts_dir.mkdir(parents=True, exist_ok=True)
        self.comments_dir = self.cache_dir / subreddit_name / 'comments'
        self.comments_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _yield_dir_json(path: pathlib.Path) -> Generator:
        for item in path.glob('*.json'):
            with item.open('r') as fh:
                yield json.load(fh)

    @property
    def posts(self) -> Generator:
        for item in self.posts_dir.glob('*.json'):
            with item.open('r') as fh:
                yield Post(**json.load(fh))

    @property
    def comments(self) -> Generator:
        for item in self.comments_dir.glob('*.json'):
            with item.open('r') as fh:
                yield Comment(**json.load(fh))

    @functools.cached_property
    def num_posts(self) -> int:
        return sum(1 for _ in self.posts_dir.glob('*.json'))

    @functools.cached_property
    def seen_posts(self) -> collections.Counter[str]:
        '''Posts with locally-cached comments'''
        seen = collections.Counter(comment['post_id'] for comment in self.comments)
        log.info('%d comments for %d posts saved so far',
                 sum(seen.values()), len(seen))
        return seen

    def post_path(self, post_id: str) -> pathlib.Path:
        return self.posts_dir / f'{post_id}.json'

    def comment_path(self, comment_id: str) -> pathlib.Path:
        return self.comments_dir / f'{comment_id}.json'

    def cache_comments(self):
        for post_num, post in enumerate(self.posts, start=1):
            if post['id'] in self.seen_posts:
                log.debug('Comments for post %d/%d (%s) already present, skipping',
                          post_num, self.num_posts, post['id'])
            else:
                log.info('Saving comments for post %d/%d (%s)',
                         post_num, self.num_posts, post['id'])
                for comment in fetch_comments_for_post(post['id']):
                    comment.save(self)

    def cache_posts(self):
        sub = self.subreddit
        listings = [sub.new(limit=None),
                    sub.top('day', limit=None),
                    sub.top('hour', limit=None),
                    sub.top('week', limit=None),
                    sub.hot(limit=None),
                    sub.controversial('day', limit=None),
                    sub.controversial('hour', limit=None),
                    sub.controversial('week', limit=None),
                    sub.rising(limit=None),
                    sub.search('silver', sort='new', syntax='plain'),  # TODO
                    sub.search('$SLV', sort='new', syntax='plain')]

        for post in itertools.chain.from_iterable(listings):
            log.info('Caching post ID %s', post.id)
            Post.from_praw(post).save(self)  # this feels wrong but too lazy to fix now

    def update_posts(self):
        for num, item in enumerate(self.posts_dir.glob('*.json'), 1):
            with item.open('r') as fh:
                submission = json.load(fh)
                log.info('Caching post ID %s (%d/%d)', submission['id'], num, self.num_posts)
                post = praw.models.Submission(reddit, id=submission['id'])
                Post.from_praw(post).save(self)


class Post(NamedTuple):
    author: str
    created: int
    edited: bool
    id: str
    num_comments: int
    permalink: str
    score: int
    selftext: str
    title: str
    url: str

    @classmethod
    def from_praw(cls, submission: praw.models.Submission):
        try:
            author = submission.author.name
        except AttributeError:
            author = '[deleted]'
        finally:
            return cls(author=author,
                       created=submission.created,
                       edited=submission.edited,
                       id=submission.id,
                       num_comments=submission.num_comments,
                       permalink=submission.permalink,
                       score=submission.score,
                       selftext=submission.selftext,
                       title=submission.title,
                       url=submission.url)

    def save(self, cache: RedditCache):
        with cache.post_path(self.id).open('w') as post_fh:
            json.dump(self._asdict(), post_fh)


class Comment(NamedTuple):
    author: str
    body: str
    created: int
    edited: bool
    id: str
    permalink: str
    post_id: str
    score: int

    @classmethod
    def from_praw(cls, comment: praw.models.Comment):
        try:
            author = comment.author.name
        except AttributeError:
            author = '[deleted]'
        finally:
            return cls(author=author,
                       body=comment.body,
                       created=comment.created,
                       edited=comment.edited,
                       id=comment.id,
                       permalink=comment.permalink,
                       post_id=comment.link_id,
                       score=comment.score)

    def save(self, cache: RedditCache):
        with cache.comment_path(self.id).open('w') as comment_fh:
            json.dump(self._asdict(), comment_fh)


def fetch_comments_for_post(post_id: str) -> Generator:
    post = praw.models.Submission(reddit, id=post_id)
    try:
        post.comments.replace_more(limit=None)
    except prawcore.exceptions.TooLarge:
        log.info('Post %s has too many comments', post_id)
        post = praw.models.Submission(reddit, id=post_id)
        post.comments.replace_more(limit=0)  # TODO
    finally:
        for comment in post.comments.list():
            yield Comment.from_praw(comment)


dispatch = {
    'fetch-comments': lambda c: c.cache_comments(),
    'fetch-posts': lambda c: c.cache_posts(),
    'update-posts': lambda c: c.update_posts()
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('subreddit_name')
    parser.add_argument('target',
                        choices=dispatch,
                        help='Comments are only collected from posts we have '
                             'already crawled; i.e., crawling comments before '
                             'posts will result in no comments being saved.')
    parser.add_argument('--cache-dir',
                        help='Directory to save crawled posts/comments',
                        default='.')
    arguments = parser.parse_args()

    cache = RedditCache(arguments.cache_dir, arguments.subreddit_name)
    dispatch[arguments.target](cache)
