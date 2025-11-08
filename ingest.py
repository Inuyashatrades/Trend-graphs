# ingest.py
import os, time, json, math, datetime as dt
from urllib.parse import urlencode
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError
import psycopg

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
DEFAULT_GEO = os.getenv("DEFAULT_GEO", "")

# ---- CONFIG ----
NICHES = [
    "SaaS","Patreon","Substack","Gumroad","Membership",
    "Creator Economy","No Code","AI Tools","Remote Work",
    "Online Learning","Freelancing","Online Communities"
]
TIMEFRAME = "now 7-d"   # also collect "today 3-m" if you want
SUBREDDITS = ["SaaS","Entrepreneur","startups","nocode","SideProject","marketing","YouTubeCreators"]
DAYS = 7

# ---- DB helpers ----
DDL = """
CREATE TABLE IF NOT EXISTS trend_points(
  source TEXT,
  niche  TEXT,
  date   DATE,
  metric TEXT,
  value  DOUBLE PRECISION,
  geo    TEXT,
  timeframe TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source,niche,date,metric,geo,timeframe)
);

CREATE TABLE IF NOT EXISTS collect_runs(
  id BIGSERIAL PRIMARY KEY,
  run_at TIMESTAMPTZ DEFAULT now(),
  source TEXT,
  niches JSONB,
  timeframe TEXT,
  geo TEXT,
  status TEXT,
  error TEXT
);
"""

def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)

def ensure_schema():
    with db() as conn:
        conn.execute(DDL)

def upsert_points(rows):
    if not rows: return
    with db() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO trend_points(source,niche,date,metric,value,geo,timeframe)
            VALUES (%(source)s,%(niche)s,%(date)s,%(metric)s,%(value)s,%(geo)s,%(timeframe)s)
            ON CONFLICT (source,niche,date,metric,geo,timeframe)
            DO UPDATE SET value=EXCLUDED.value, created_at=now()
            """, rows)

def log_run(source, status="ok", error=""):
    with db() as conn:
        conn.execute(
            "INSERT INTO collect_runs(source,niches,timeframe,geo,status,error) VALUES (%s,%s,%s,%s,%s,%s)",
            (source, json.dumps(NICHES), TIMEFRAME, DEFAULT_GEO, status, error)
        )

# ---- Google Trends ----
def fetch_google_trends(niches, timeframe=TIMEFRAME, geo=DEFAULT_GEO):
    py = TrendReq(hl="en-US", tz=360)
    rows=[]
    def chunks(lst,n): 
        for i in range(0,len(lst),n): yield lst[i:i+n]
    for group in chunks(niches,5):
        attempts=0
        while True:
            try:
                py.build_payload(group, timeframe=timeframe, geo=geo)
                df = py.interest_over_time().reset_index(names="date")
                if df.empty: break
                for _, r in df.iterrows():
                    d = pd.to_datetime(r["date"]).date().isoformat()
                    for n in group:
                        val = float(r.get(n, 0)) if not pd.isna(r.get(n, np.nan)) else 0.0
                        rows.append({"source":"google_trends","niche":n,"date":d,"metric":"interest",
                                     "value":val,"geo":geo,"timeframe":timeframe})
                break
            except (TooManyRequestsError, ResponseError):
                attempts += 1
                time.sleep(min(2**attempts, 60))
            except Exception as e:
                log_run("google_trends","error",str(e)); break
    upsert_points(rows); log_run("google_trends")

# ---- YouTube (official API) ----
def yt_views_for_query(q, days=DAYS):
    # 1) search videos in last N days
    published_after=(dt.datetime.utcnow()-dt.timedelta(days=days)).isoformat("T")+"Z"
    search_params = {
        "part":"id", "type":"video", "q":q,
        "maxResults": 25, "order":"date",
        "publishedAfter": published_after, "key": YOUTUBE_API_KEY
    }
    s = requests.get("https://www.googleapis.com/youtube/v3/search", params=search_params, timeout=30).json()
    ids = [it["id"]["videoId"] for it in s.get("items",[])]
    if not ids: return 0
    # 2) fetch stats
    stats_params = {"part":"statistics", "id":",".join(ids), "key":YOUTUBE_API_KEY}
    v = requests.get("https://www.googleapis.com/youtube/v3/videos", params=stats_params, timeout=30).json()
    views = sum(int(it["statistics"].get("viewCount","0")) for it in v.get("items",[]))
    return views

def fetch_youtube(niches):
    if not YOUTUBE_API_KEY: return
    today = dt.date.today().isoformat()
    rows=[]
    for n in niches:
        try:
            v = yt_views_for_query(n, DAYS)
            rows.append({"source":"youtube","niche":n,"date":today,"metric":"views_7d",
                         "value":float(v),"geo":DEFAULT_GEO,"timeframe":f"last_{DAYS}d"})
        except Exception as e:
            log_run("youtube","error",str(e))
    upsert_points(rows); log_run("youtube")

# ---- Reddit (official API via PRAW) ----
def fetch_reddit(niches):
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT): return
    import praw
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    cutoff = dt.datetime.utcnow() - dt.timedelta(days=DAYS)
    today = dt.date.today().isoformat()
    rows=[]
    for n in niches:
        count = 0
        for sub in SUBREDDITS:
            try:
                for post in reddit.subreddit(sub).search(n, sort="new", time_filter="week", limit=100):
                    created = dt.datetime.utcfromtimestamp(post.created_utc)
                    if created >= cutoff: count += 1
            except Exception:
                pass
        rows.append({"source":"reddit","niche":n,"date":today,"metric":"posts_7d",
                     "value":float(count),"geo":DEFAULT_GEO,"timeframe":f"last_{DAYS}d"})
    upsert_points(rows); log_run("reddit")

# ---- News (NewsAPI) ----
def fetch_news(niches):
    if not NEWSAPI_KEY: return
    today = dt.date.today().isoformat()
    from_date = (dt.date.today()-dt.timedelta(days=DAYS)).isoformat()
    rows=[]
    for n in niches:
        try:
            params={"q": n, "from": from_date, "language":"en", "pageSize": 100, "apiKey": NEWSAPI_KEY}
            r = requests.get("https://newsapi.org/v2/everything", params=params, timeout=30).json()
            total = int(r.get("totalResults", 0))
            rows.append({"source":"newsapi","niche":n,"date":today,"metric":"articles_7d",
                         "value":float(total),"geo":DEFAULT_GEO,"timeframe":f"last_{DAYS}d"})
        except Exception as e:
            log_run("newsapi","error",str(e))
    upsert_points(rows); log_run("newsapi")

# ---- HN Algolia (no key) ----
def fetch_hn(niches):
    today = dt.date.today().isoformat()
    since = int((dt.datetime.utcnow()-dt.timedelta(days=DAYS)).timestamp())
    rows=[]
    for n in niches:
        try:
            url = "https://hn.algolia.com/api/v1/search_by_date?" + urlencode({
                "query": n, "tags":"story", "numericFilters": f"created_at_i>{since}", "hitsPerPage": 1000
            })
            r = requests.get(url, timeout=30).json()
            rows.append({"source":"hn","niche":n,"date":today,"metric":"stories_7d",
                         "value":float(len(r.get("hits",[]))),"geo":DEFAULT_GEO,"timeframe":f"last_{DAYS}d"})
        except Exception as e:
            log_run("hn","error",str(e))
    upsert_points(rows); log_run("hn")

if __name__ == "__main__":
    ensure_schema()
    fetch_google_trends(NICHES, TIMEFRAME, DEFAULT_GEO)
    fetch_youtube(NICHES)
    fetch_reddit(NICHES)
    fetch_news(NICHES)
    fetch_hn(NICHES)
    print("Ingest complete.")
