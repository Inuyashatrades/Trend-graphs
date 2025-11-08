# app.py
import os, math, json, datetime as dt
import numpy as np
import pandas as pd
import streamlit as st
import psycopg
from dotenv import load_dotenv
from pytrends.request import TrendReq  # only for related queries (optional)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_GEO = os.getenv("DEFAULT_GEO", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL","")

st.set_page_config(page_title="Niche Trend Radar", layout="wide")

def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)

# --- boards table ---
DDL = """
CREATE TABLE IF NOT EXISTS boards(
  id BIGSERIAL PRIMARY KEY,
  name TEXT UNIQUE,
  niches JSONB,
  geo TEXT,
  timeframe TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
"""
with db() as conn: conn.execute(DDL)

DEFAULT_NICHES = [
    "SaaS","Patreon","Substack","Gumroad","Membership",
    "Creator Economy","No Code","AI Tools","Remote Work",
    "Online Learning","Freelancing","Online Communities"
]

def load_points(niches, start, end, geo, timeframes):
    q = """
    SELECT source,niche,date,metric,value,geo,timeframe
    FROM trend_points
    WHERE niche = ANY(%s) AND date BETWEEN %s AND %s AND geo = %s AND timeframe = ANY(%s)
    """
    with db() as conn:
        df = pd.read_sql(q, conn, params=(niches, start, end, geo, timeframes), parse_dates=["date"])
    return df

def compute_momentum(gt_df):
    if gt_df.empty: return pd.DataFrame(columns=["niche","wow_pct","slope7","zscore90","gt_score"])
    df = gt_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    stats=[]
    for niche, g in df.groupby("niche"):
        g = g.sort_values("date").set_index("date").asfreq("D").fillna(method="ffill")
        series = g["value"].values
        if len(series) < 14:
            stats.append({"niche":niche,"wow_pct":np.nan,"slope7":np.nan,"zscore90":np.nan,"gt_score":np.nan})
            continue
        cur7 = np.nanmean(series[-7:])
        prev7 = np.nanmean(series[-14:-7])
        wow = (cur7 - prev7)/prev7 if prev7 and not math.isclose(prev7,0) else np.nan
        # slope
        m = np.polyfit(np.arange(7), series[-7:], 1)[0]
        # z vs 90 (cap by length)
        base = series[-90:] if len(series)>=90 else series
        z = (cur7 - np.nanmean(base))/ (np.nanstd(base) if np.nanstd(base)>0 else np.nan)
        score = np.nansum([np.tanh(wow)*0.45 if not np.isnan(wow) else 0,
                           np.tanh(m/10)*0.35 if not np.isnan(m) else 0,
                           np.tanh(z/3)*0.20 if not np.isnan(z) else 0])
        stats.append({"niche":niche,"wow_pct":wow,"slope7":m,"zscore90":z,"gt_score":float(score)})
    return pd.DataFrame(stats).sort_values("gt_score", ascending=False)

def normalize(x):
    if len(x)==0: return x
    mn, mx = min(x), max(x)
    return [(v-mn)/(mx-mn) if mx>mn else 0 for v in x]

def fuse_scores(momentum, metrics_df):
    """Fuse GT score + YouTube/Reddit/News/HN counts (normalized)."""
    base = momentum.set_index("niche")[["gt_score"]]
    piv = metrics_df.pivot_table(index="niche", columns="metric", values="value", aggfunc="sum").fillna(0)
    for col in piv.columns:
        piv[col] = normalize(piv[col].tolist())
    fused = base.join(piv, how="outer").fillna(0)
    fused["fused_score"] = 0.6*fused["gt_score"] + 0.20*fused.get("views_7d",0) + 0.12*fused.get("posts_7d",0) + 0.08*fused.get("articles_7d",0) + 0.05*fused.get("stories_7d",0)
    return fused.sort_values("fused_score", ascending=False).reset_index()

def send_slack(text):
    if not SLACK_WEBHOOK_URL: return
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception:
        pass

# --- Sidebar (Boards) ---
with st.sidebar:
    st.header("Filters / Board")
    board_action = st.radio("Board mode", ["Use filters","Load board","Save board"], index=0)
    if board_action == "Load board":
        with db() as conn:
            rows = conn.execute("SELECT name,niches,geo,timeframe FROM boards ORDER BY name").fetchall()
        names = [r[0] for r in rows]
        chosen = st.selectbox("Saved boards", names) if names else None
        if chosen:
            row = [r for r in rows if r[0]==chosen][0]
            niches = list(row[1])
            geo = row[2]; timeframe = row[3]
        else:
            niches = DEFAULT_NICHES; geo = DEFAULT_GEO; timeframe = "now 7-d"
    else:
        niches = st.multiselect("Niches", DEFAULT_NICHES, default=DEFAULT_NICHES)
        geo = st.text_input("Geo", value=DEFAULT_GEO)
        timeframe = st.selectbox("Google Trends timeframe", ["now 1-d","now 7-d","today 3-m","today 12-m"], index=1)
        if board_action == "Save board":
            board_name = st.text_input("Board name")
            if st.button("Save"):
                with db() as conn:
                    conn.execute("INSERT INTO boards(name,niches,geo,timeframe) VALUES (%s,%s,%s,%s) ON CONFLICT (name) DO UPDATE SET niches=EXCLUDED.niches, geo=EXCLUDED.geo, timeframe=EXCLUDED.timeframe",
                                 (board_name, json.dumps(niches), geo, timeframe))
                st.success(f"Saved board: {board_name}")

    start = st.date_input("Start", dt.date.today()-dt.timedelta(days=7))
    end   = st.date_input("End", dt.date.today())
    run_alerts = st.button("Run alert check")

st.title("📊 Niche Trend Radar — PM View")

# --- Load data from Postgres (collected by ingest.py) ---
df = load_points(niches, start, end, geo, [timeframe, f"last_7d"])
gt = df[(df["source"]=="google_trends") & (df["metric"]=="interest")].copy()
metrics = df[(df["source"]!="google_trends")].copy()

# --- Charts ---
if not gt.empty:
    st.subheader("Google Trends — Interest Over Time")
    pivot = gt.pivot_table(index="date", columns="niche", values="value")
    st.line_chart(pivot)

st.subheader("Momentum & Breakouts")
momentum = compute_momentum(gt.rename(columns={"value":"value"})) if not gt.empty else pd.DataFrame()
st.dataframe(momentum, use_container_width=True)

st.subheader("Cross-source (YouTube / Reddit / News / HN) + Fused Score")
fused = fuse_scores(momentum, metrics) if not momentum.empty else pd.DataFrame()
st.dataframe(fused[["niche","fused_score","gt_score","views_7d","posts_7d","articles_7d","stories_7d"]].fillna(0), use_container_width=True)

# --- “Where to bet next?” ---
if not fused.empty:
    st.markdown("### Where to bet next (Top 3)")
    top3 = fused.head(3)["niche"].tolist()
    st.write(", ".join([f"**{x}**" for x in top3]))
    st.markdown("### Content calendar suggestions")
    for n in top3:
        st.markdown(f"- **{n}**: 3-post arc this week (Problem → Quick Win → Case Study). Short-form teaser; PH landing page if `geo='{geo or 'GLOBAL'}'`.")

# --- Alerts ---
if run_alerts and not fused.empty:
    # breakout = zscore90 >= 2 OR in top 3 fused
    breakers = momentum[momentum["zscore90"]>=2.0]["niche"].tolist()
    top3 = fused.head(3)["niche"].tolist()
    if breakers or top3:
        msg = f"Trend Alerts ({dt.datetime.now().isoformat()}):\nTop3: {', '.join(top3)}\nBreakouts: {', '.join(breakers) or '—'}"
        send_slack(msg)
        st.success("Alerts evaluated. Sent to Slack (if webhook configured).")
    else:
        st.info("No alerts today.")

# --- Optional: related queries for #1 niche ---
if not momentum.empty:
    try:
        top = momentum.iloc[0]["niche"]
        st.markdown(f"#### Related rising queries — **{top}**")
        py = TrendReq(hl="en-US", tz=360)
        py.build_payload([top], timeframe=timeframe, geo=geo)
        rq = py.related_queries()
        rising = rq[top]["rising"]
        if rising is not None:
            st.dataframe(rising.rename(columns={"query":"term","value":"score"}).head(10))
    except Exception:
        pass

