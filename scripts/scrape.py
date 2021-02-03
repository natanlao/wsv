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
from typing import Generator

import praw
import prawcore.exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
reddit = praw.Reddit(user_agent='wsv v0.1 (https://github.com/natanlao/wsv)')


class RedditCache:

    def __init__(self, cache_dir: str, subreddit_name: str):
        self.cache_dir = pathlib.Path(cache_dir)
        self.posts_dir = self.cache_dir / subreddit_name / 'posts'
        self.comments_dir = self.cache_dir / subreddit_name / 'comments'

    @staticmethod
    def _yield_dir_json(path: pathlib.Path) -> Generator:
        for item in path.glob('*.json'):
            with item.open('r') as fh:
                yield json.load(fh)

    @property
    def posts(self) -> Generator:
        # This is leaky but I'm lazy
        return self._yield_dir_json(self.posts_dir)

    @property
    def comments(self) -> Generator:
        return self._yield_dir_json(self.comments_dir)

    @functools.cached_property
    def num_posts(self) -> int:
        return len(list(self.posts))

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
                    with self.comment_path(comment['id']).open('w') as json_fp:
                        json.dump(comment, json_fp, indent=2)

    def cache_posts(self):
        sub = reddit.subreddit(self.subreddit_name)
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
            with self.post_path(post.id).open('w') as post_fh:
                try:
                    author = post.author.name
                except AttributeError:
                    author = '[deleted]'
                finally:
                    post = {
                        'author': author,
                        'created': post.created,
                        'edited': post.edited,
                        'id': post.id,
                        'permalink': post.permalink,
                        'selftext': post.selftext,
                        'title': post.title,
                        'url': post.url
                    }
                    json.dump(post, post_fh, indent=2)


def fetch_comments_for_post(post_id: str) -> Generator:
    post = praw.models.Submission(reddit, id=post_id)

    try:
        post.comments.replace_more(limit=None)
    except prawcore.exceptions.TooLarge:
        log.info('Post %s has too many comments', post_id)
        post = praw.models.Submission(reddit, id=post_id)
        post.comments.replace_more(limit=0)  # TODO

    for comment in post.comments.list():
        try:
            author = comment.author.name
        except AttributeError:
            author = '[deleted]'
        finally:
            yield {
                'author': author,
                'body': comment.body,
                'created': comment.created,
                'edited': comment.edited,
                'id': comment.id,
                'permalink': comment.permalink,
                'post_id': post.id
            }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('subreddit_name')
    parser.add_argument('target',
                        choices=['comments', 'posts'],
                        help='Comments are only collected from posts we have '
                             'already crawled; i.e., crawling comments before '
                             'posts will result in no comments being saved.')
    parser.add_argument('--cache-dir',
                        help='Directory to save crawled posts/comments',
                        default='.')
    arguments = parser.parse_args()

    cache = RedditCache(arguments.cache_dir, arguments.subreddit_name)
    if arguments.target == 'posts':
        cache.cache_posts()
    elif arguments.target == 'comments':
        cache.cache_comments()
    else:
        exit(1)
