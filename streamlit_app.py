# streamlit_app.py

import streamlit as st
import pandas as pd
import datetime
from pytrends.request import TrendReq

# ------------- CONFIG & NICHE SETUP --------------
st.set_page_config(page_title="Global Trend Dashboard", layout='wide')

GOOD_NICHES = [
    "SaaS", "Stan", "Patreon", "Substack", "Gumroad",
    "Membership", "Creator Economy", "No Code", "AI Tools",
    "Remote Work", "NFT", "Online Learning", "Freelancing", "Online Communities"
]

# ------------- DATA FETCHING FUNCTIONS (MODULAR) ------------

def get_google_trends(niche_list, geo=''):
    pytrends = TrendReq(hl='en-US', tz=360)
    trend_data = []
    for topic in niche_list:
        pytrends.build_payload([topic], cat=0, timeframe='now 7-d', geo=geo)
        trends = pytrends.interest_over_time()
        if not trends.empty:
            for date, row in trends.iterrows():
                trend_data.append({
                    'date': date,
                    'niche': topic,
                    'trend_score': row[topic]
                })
    return pd.DataFrame(trend_data)

def fetch_sample_twitter_trends():
    # Placeholder for Twitter API/X API or third-party integration
    df = pd.DataFrame({
        'hashtag': ['#SaaS', '#NoCode', '#Patreon', '#StanStore'],
        'volume': [32000, 21000, 15000, 9900],
        'sample_tweet': [
            "SaaS never dies. New launches weekly.",
            "No code, more freedom! 🚀",
            "Patreon creators crushing it last month.",
            "Stan making lives easier for creators."
        ],
        'timestamp': pd.to_datetime(['2025-11-01', '2025-11-03', '2025-11-05', '2025-11-06'])
    })
    return df

def fetch_sample_instagram_trends():
    # Placeholder for Instagram Reel/post API or scrapers; fill in with real-time data as needed
    df = pd.DataFrame({
        'post': [
            'https://instagram.com/p/saas-post1',
            'https://instagram.com/p/stan-viral',
            'https://instagram.com/p/patreon-feature',
            'https://instagram.com/p/creator-tip'
        ],
        'caption': [
            "Create your SaaS empire with zero code.",
            "Why every creator is using Stan.",
            "How Patreon helped me go full-time.",
            "10 hooks that grew my followers!"
        ],
        'likes': [5400, 8300, 11900, 7700],
        'comments': [110, 89, 131, 94],
        'hashtag': ['#SaaS', '#Stan', '#Patreon', '#CreatorHooks'],
        'date': pd.to_datetime(['2025-11-01', '2025-11-05', '2025-11-02', '2025-11-04'])
    })
    return df

def get_weekly_comparison(trend_df):
    # Pivots data for week-over-week charting
    pivot = trend_df.pivot_table(index='date', columns='niche', values='trend_score')
    return pivot

def get_competitor_insights():
    # Sample: Real implementation could look up top Patreon/Stan/Gumroad accounts
    data = [
        {"brand": "Ali Abdaal (Patreon)", "niche": "Online Learning", "followers": 38000, "latest_hit": "How I Run a 7-Figure YouTube Business"},
        {"brand": "Ness Labs (Substack)", "niche": "Productivity", "followers": 21000, "latest_hit": "Mindful Productivity Newsletter"},
        {"brand": "Cue The Curves (Stan)", "niche": "Body Positivity Community", "followers": 4300, "latest_hit": "Create a Wardrobe with Confidence"},
    ]
    return pd.DataFrame(data)

# ------------- SIDEBAR CONTROLS -------------
with st.sidebar:
    st.header("Trend Filters")
    selected_niches = st.multiselect("Niches", GOOD_NICHES, default=GOOD_NICHES)
    start_date = st.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=7))
    end_date = st.date_input("End Date", datetime.date.today())
    download_btn = st.button("Download All Trends (CSV)")
    st.markdown("---")
    st.caption("Customize this dashboard or add new sources via API plugins.")

# ------------- MAIN DASHBOARD ----------------

st.title("🌍 Global Trends Dashboard (SaaS & Creator Economy Focus)")

# === Tabs ===
tabs = st.tabs([
    "Google Trends", "Twitter Trends", "Instagram Trends", "Competitor Insights", "Recommendations"
])

# --- Google Trends Tab ---
with tabs[0]:
    st.subheader("Google Trends (Global, last 7 days)")
    google_trends_df = get_google_trends(selected_niches)
    mask = (google_trends_df['date'] >= pd.to_datetime(start_date)) & (google_trends_df['date'] <= pd.to_datetime(end_date))
    st.line_chart(get_weekly_comparison(google_trends_df[mask]))
    st.dataframe(google_trends_df[mask])
    if download_btn:
        st.download_button("Download Google Trends CSV", google_trends_df.to_csv(index=False), file_name="google_trends.csv")

# --- Twitter Trends Tab ---
with tabs[1]:
    st.subheader("Top Twitter/X Trending Hashtags and Topics")
    twitter_df = fetch_sample_twitter_trends()
    st.bar_chart(twitter_df.set_index("hashtag")["volume"])
    st.dataframe(twitter_df)
    if download_btn:
        st.download_button("Download Twitter Trends CSV", twitter_df.to_csv(index=False), file_name="twitter_trends.csv")

# --- Instagram Trends Tab ---
with tabs[2]:
    st.subheader("Most Engaging Instagram Posts / Reels")
    insta_df = fetch_sample_instagram_trends()
    st.dataframe(insta_df)
    st.markdown("#### Trending Visuals")
    # Visual cards (simulate): List top 2 posts
    best_posts = insta_df.sort_values(by="likes", ascending=False).head(2)
    for _, row in best_posts.iterrows():
        st.markdown(f"""
        - **Caption:** {row['caption']}  
        - **Likes:** {row['likes']}  |  **Comments:** {row['comments']}  
        - [View Post]({row['post']})
        """)

# --- Competitor Insights Tab ---
with tabs[3]:
    st.subheader("Competitor Trends")
    comp_df = get_competitor_insights()
    st.dataframe(comp_df)
    for _, row in comp_df.iterrows():
        st.markdown(f"""
        - **Brand:** {row['brand']}  
        - **Niche:** {row['niche']}  
        - **Followers:** {row['followers']}  
        - **Recent Hit:** {row['latest_hit']}
        """)

# --- Recommendations Tab ---
with tabs[4]:
    st.subheader("Content Ideas & Best Practices")
    st.markdown("""
    - **Try these hooks:**  
        - "How I grew my [platform] income in 30 days"  
        - "Why everyone is switching to Stan this year"  
        - "My top 3 SaaS automations"  
    - **Post short-form teasers and behind-the-scenes clips.**
    - **Leverage hashtags:** #SaaS #CreatorEconomy #StanStore #PassiveIncome
    - **Collaborate with micro-influencers in your niche.**
    - **Monitor week-over-week trends and double down on rising topics.**
    """)

st.success("Customize this template to connect your APIs (Twitter, Instagram, etc) for live global trends!")
