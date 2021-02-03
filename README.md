# WSV: Wall Street Vets

Scraper to see if /r/wallstreetbets was *actually* talking about $SLV and
dumping $GME in early February, as reported by many media outlets.

I'm not formally affiliated with /r/wallstreetbets, nor am I informally part of
that community.

## How it works

1. scrape.py: Scrape a bunch of recent posts and comments from /r/wsb and save
   them as JSON locally.

   Reddit's API makes getting posts/comments in a date range non-trivial;
   ideally, we could use PushShift, but that's been down for some time (probably
   because a bunch of people are trying to do the same thing that I'm trying to
   do). So, the next best thing is to get as many posts as possible (by crawling
   new/rising/controversial/top/etc) and hoping that the sample is large and
   representative enough.

2. load.py: Load JSON into a SQLite database.

   This is a tradeoff; the time spent fetching posts and comments over the
   network is more expensive than disk space. It would be simpler to load the
   crawled data directly into an SQLite database, but I wasn't done with that at
   the time I wanted to start crawling.

3. plot.py: Query the SQLite database to generate plots using plotly.


## Usage

### Dependencies

This project was developed using Python 3.9.1.

```console
$ pip install -r requirements.txt
```

### Authenticating to Reddit

To authenticate to Reddit, you need to:

1. Create an app using your Reddit account.

2. Run `vendor/refresh-token.py`. See the [praw
   documentation](https://praw.readthedocs.io/en/latest/getting_started/authentication.html#code-flow)
   for more information.

3. Save your credentials in praw.ini:

   ```ini
   [DEFAULT]
   client_id=...
   client_secret=...
   refresh_token=...
   ```

### Collect and analyze data

```console
$ python scrape.py wallstreetbets posts
$ python scrape.py wallstreetbets comments
$ python load.py wsb.db
$ python plot.py ...
```

