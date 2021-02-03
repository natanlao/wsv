'''
Crawl as many recent posts for a subreddit as the Reddit API lets us.
'''
import argparse
import collections
import itertools
import json
import logging
import pathlib
from typing import Iterator, Mapping, Sequence
import sys

import praw
import prawcore.exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
reddit = praw.Reddit(user_agent='wsv v0.1 (https://github.com/natanlao/wsv)')


def load_json(json_file: pathlib.Path):
    with json_file.open('r') as json_fp:
        return json.load(json_fp)


def index_seen_posts(comments_dir: pathlib.Path) -> collections.Counter[str]:
    seen = collections.Counter()
    for comment_json in comments_dir.glob('*.json'):
        seen[load_json(comment_json)['post_id']] += 1
    log.info('%d comments saved so far', len(seen))
    return seen


def fetch_comments_for_post(post_id: str) -> Iterator:
    post = praw.models.Submission(reddit, id=post_id)

    try:
        post.comments.replace_more(limit=None)
    except prawcore.exceptions.TooLarge:
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


def fetch_comments(comms_dir: pathlib.Path, posts_dir: pathlib.Path):
    seen_posts = index_seen_posts(comms_dir)
    saved_posts = list(posts_dir.glob('*.json'))
    for post_num, post_json in enumerate(saved_posts, start=1):
        if (post_id := load_json(post_json)['id']) in seen_posts:
            log.debug('Comments for post %d/%d (%s) already present, skipping',
                      post_num, len(saved_posts), post_id)
            continue
        else:
            log.info('Saving comments for post %d/%d (%s)',
                     post_num, len(saved_posts), post_id)
            for comment in fetch_comments_for_post(post_id):
                comm_json = comms_dir / f'{comment["id"]}.json'
                log.debug('Saved comment to %s', comm_json.as_posix())
                with comm_json.open('w') as json_fp:
                    json.dump(comment, json_fp, indent=2)


def fetch_posts(posts_dir: pathlib.Path, sub: praw.models.Subreddit):
    listings = [sub.new(limit=None),
                sub.top('day', limit=None),
                sub.top('hour', limit=None),
                sub.top('week', limit=None),
                sub.hot(limit=None),
                sub.controversial('day', limit=None),
                sub.controversial('hour', limit=None),
                sub.controversial('week', limit=None),
                sub.rising(limit=None),
                sub.search('silver', sort='new', syntax='plain'),
                sub.search('$SLV', sort='new', syntax='plain')]

    for post in itertools.chain.from_iterable(listings):
        log.info('Crawling post %r', post.id)
        with open(posts_dir / f'{post.id}.json', 'w') as post_json:
            try:
                author = post.author.name
            except AttributeError:
                author = '[deleted]'
            finally:
                post_obj = {
                    'author': author,
                    'created': post.created,
                    'edited': post.edited,
                    'id': post.id,
                    'permalink': post.permalink,
                    'selftext': post.selftext,
                    'title': post.title,
                    'url': post.url
                }
                json.dump(post_obj, post_json, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('subreddit')
    parser.add_argument('target',
                        choices=['comments', 'posts'],
                        help='Note that we only collect comments from posts '
                             'that we have already crawled; i.e., trying to '
                             'crawl comments before posts will result in no '
                             'comments being saved.')
    parser.add_argument('--cache-dir',
                        help='Directory to save crawled posts/comments',
                        default='.')
    arguments = parser.parse_args()

    sub = reddit.subreddit(arguments.subreddit)
    cache_dir = pathlib.Path(arguments.cache_dir)
    posts_dir = cache_dir / 'posts'
    comms_dir = cache_dir / 'comments'

    if arguments.target == 'posts':
        fetch_posts(posts_dir, sub)
    elif arguments.target == 'comments':
        fetch_comments(comms_dir, posts_dir)
    else:
        exit(1)

