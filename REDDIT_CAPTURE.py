import requests
import datetime
import time
import random
import sys
import psycopg2
from psycopg2 import extras
import supersecrets as shh

headers = {'User-Agent': 'NegativityProject:v1.0 (by /u/crazykingludwig)'}

class Post():
    def __init__(self, subreddit, title, selftext, score, upvote_ratio, post_url,
                 ID, num_comments, over_18, created_utc):
        self.subreddit = subreddit
        self.title = title
        self.selftext = selftext
        self.score = score
        self.upvote_ratio = upvote_ratio
        self.post_url = post_url
        self.ID = ID
        self.num_comments = num_comments
        self.over_18 = over_18
        self.created_utc = created_utc
        
    def make_comment_url(self):
        
        return f"https://www.reddit.com{self.permalink}.json?sort=top"
    
    def package_post(self):

        if isinstance(self.created_utc, (int, float)):
            ts = datetime.datetime.fromtimestamp(self.created_utc)
        else:
            ts = self.created_utc

        tuptup = (self.ID, self.subreddit, self.title, self.selftext, self.score,
                  self.upvote_ratio, self.post_url, self.num_comments, self.over_18,
                  ts)
        
        return tuptup
    
    
class Comment():
    def __init__(self, body, subreddit, ID, parent_ID, 
                 score, is_submitter, depth, post_ID,
                 created_utc, distinguished, author):
        self.body = body
        self.subreddit = subreddit
        self.ID = ID
        self.parent_ID = parent_ID
        self.post_ID = post_ID
        self.author = author
        self.score = score
        self.is_submitter = is_submitter
        self.depth = depth
        self.created_utc = created_utc
        self.distinguished = distinguished
        
    def package_comment(self):
        
        if isinstance(self.created_utc, (int, float)):
            ts = datetime.datetime.fromtimestamp(self.created_utc)
        else:
            ts = self.created_utc
        
        tuptup = (self.ID, self.post_ID, self.parent_ID, self.author, self.body,
                  self.score, self.subreddit, self.is_submitter, self.distinguished,
                  self.depth, ts)
        
        return tuptup

def logger_jobber(job_name, status, error_msg = None):
    try:
        con = get_db_connection()
        cur = con.cursor()
        sql = """
        INSERT INTO monitor.job_history (job_name, status, error_message)
        VALUES (%s, %s, %s)
        """

        cur.execute(sql, (job_name, status, error_msg))
        con.commit()
        cur.close()
        con.close()

    except Exception as e:
        print(f"log fail! {e}")
        pass
    

def get_db_connection():
    
    con = psycopg2.connect(
        host = shh.db_ip,
        database = shh.db_name,
        user = shh.db_user,
        password = shh.db_password,
        port = shh.db_port
        )
    
    return con

def init_db():
    con = get_db_connection()
    cur = con.cursor()

    create_comments = """
    CREATE TABLE IF NOT EXISTS reddit.comments (
        comment_id VARCHAR(20) PRIMARY KEY,
        post_id varchar(20),
        parent_id VARCHAR(20),
        author VARCHAR(50),
        body TEXT,
        score INTEGER,
        subreddit VARCHAR(50),
        is_submitter BOOLEAN,
        distinguished VARCHAR(20),
        comment_depth INTEGER,
        created_utc TIMESTAMP,
        ingest_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        ollama_scored_at TIMESTAMPTZ DEFAULT NULL,
        valence INTEGER,
        social_intent INTEGER,
        outlook INTEGER,
        ollama_reasoning TEXT
    );
    """

    create_posts = """
    CREATE TABLE IF NOT EXISTS reddit.posts (
        post_id VARCHAR(20) PRIMARY KEY,
        subreddit VARCHAR(50),
        title TEXT NOT NULL,
        selftext TEXT,
        score INTEGER DEFAULT 0,
        upvote_ratio NUMERIC(5, 2),
        permalink TEXT,
        num_comments INTEGER DEFAULT 0,
        over_18 BOOLEAN DEFAULT FALSE,
        created_utc TIMESTAMP,
        ollama_scored_at TIMESTAMPTZ DEFAULT NULL,
        valence INTEGER,
        social_intent INTEGER,
        outlook INTEGER,
        ollama_reasoning TEXT
        );
    """

    cur.execute(create_comments)
    cur.execute(create_posts)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_post ON reddit.comments (post_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_parent ON reddit.comments (parent_id);")

    con.commit()
    cur.close()
    con.close()

def track_n_tag():
    con = get_db_connection()
    cur = con.cursor()

    sql = """
    SELECT post_id, post_url
    FROM reddit.staging
    WHERE scrape_status = 'pending'
    AND created_utc < NOW() - INTERVAL '72 hours'
    ORDER BY created_utc ASC
    LIMIT 1;
    """

    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    con.close()

    return row

def parse_post(post_json):

    data = post_json['data']['children'][0]['data']

    POST = Post(
        subreddit = data.get('subreddit'),
        title = data.get('title'),
        selftext = data.get('selftext'),
        score = data.get('score'),
        upvote_ratio = data.get('upvote_ratio'),
        permalink = data.get('permalink'),
        ID = data.get('id'),
        num_comments = data.get('num_comments'),
        over_18 = data.get('over_18'),
        created_utc = data.get('created_utc')
    )

    return POST

def parse_comment(comment_json, post_id):

    COMMENT = Comment(
                ID = comment_json.get('id'),
                post_ID = post_id,
                parent_ID = comment_json.get('parent_id'),
                author = comment_json.get('author', '[deleted]'),
                body = comment_json.get('body', '[deleted]'),
                score = comment_json.get('score', 0),
                subreddit = comment_json.get('subreddit'),
                is_submitter = comment_json.get('is_submitter', False),
                distinguished = comment_json.get('distinguished'), 
                depth = comment_json.get('depth', 0),
                created_utc = comment_json.get('created_utc', 0)
            )
    
    return COMMENT

def extraction(comment_list, post_id, flattened = None):

    if flattened is None:
        flattened = []

    for item in comment_list:
        if item['kind'] == 'more':
            continue
        
        if item['kind'] == 't1':
            data = item['data']

            COMMENT = parse_comment(data, post_id)

            flattened.append(COMMENT)

            if 'replies' in data and data['replies'] != "":
                replies = data['replies'].get('data', {}).get('children', [])
                extraction(replies, post_id, flattened)

    return flattened

def save_data(POST, COMMENTS):
    con = get_db_connection()
    cur = con.cursor()

    post_sql = """
    INSERT INTO reddit.posts
    (post_id, subreddit, title, selftext, score, upvote_ratio,
    post_url, num_comments, over_18, created_utc)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (post_id) DO UPDATE
    SET score = EXCLUDED.score,
        upvote_ratio = EXCLUDED.upvote_ratio,
        num_comments = EXCLUDED.num_comments;
    """

    comment_sql = """
    INSERT INTO reddit.comments 
    (comment_id, post_id, parent_id, author, body, score, subreddit, 
    is_submitter, distinguished, comment_depth, created_utc)
    VALUES %s
    ON CONFLICT (comment_id) DO NOTHING;
    """

    try:
        cur.execute(post_sql, POST.package_post())

        if COMMENTS:
            values = [c.package_comment() for c in COMMENTS]
            extras.execute_values(cur, comment_sql, values)

        con.commit()
        return True
    except Exception as e:
        print(f"DB Save Error: {e}")
        con.rollback()
        return False
    finally:
        cur.close()
        con.close()

def update_staging_status(post_id, status):
    con = get_db_connection()
    cur = con.cursor()
    sql = "UPDATE reddit.staging SET scrape_status = %s WHERE post_id = %s"
    cur.execute(sql, (status, post_id))
    con.commit()
    cur.close()
    con.close()

def capture_data():
    task = track_n_tag()
    if not task:
        print("Nothing Pending")
        return True

    post_id, target_url = task
    print(f"Capturing {post_id}")

    try:
        time.sleep(random.randint(5, 30))
        resp = requests.get(target_url, headers = headers)

        if resp.status_code != 200:
            print(f"Reddit not happy! {resp.status_code}")
            update_staging_status(post_id, 'failed')
            return False
        
        json_data = resp.json()

        POST = parse_post(json_data[0])
        raw_comments = json_data[1]['data']['children']
        COMMENTS = extraction(raw_comments, post_id)

        print(f"Grabbed {len(COMMENTS)} comments!")

        success = save_data(POST, COMMENTS)

        if success:
            update_staging_status(post_id, 'scraped')
            return True
        else:
            update_staging_status(post_id, 'failed')
            return False
    except Exception as e:
        print(f'Error! {e}')
        update_staging_status(post_id, 'failed')
        return False
    
if __name__ == "__main__":

    try:
        init_db()
    except Exception:
        pass

    print(f"CAPTURE @ {datetime.datetime.now()}")
    success = capture_data()

    if success:
        logger_jobber('REDDIT_RUN', 1, 'Success')
    else:
        logger_jobber('REDDIT_RUN', 0, 'Failed')

    print('sleep')    
