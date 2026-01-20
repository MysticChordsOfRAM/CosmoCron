import json
import datetime
from psycopg2 import extras
import psycopg2
import supersecrets as shh

class Post():
    def __init__(self, subreddit, title, selftext, score, upvote_ratio, permalink,
                 ID, num_comments, over_18, created_utc,
                 ollama_scored_at = None, sentiment_score = None):
        self.subreddit = subreddit
        self.title = title
        self.selftext = selftext
        self.score = score
        self.upvote_ratio = upvote_ratio
        self.permalink = permalink
        self.ID = ID
        self.num_comments = num_comments
        self.over_18 = over_18
        self.created_utc = created_utc
        self.ollama_scored_at = ollama_scored_at
        self.sentiment_score = sentiment_score
        
    def make_comment_url(self):
        
        return f"https://www.reddit.com{self.permalink}.json?sort=top"
    
    def package_post(self):
        tuptup = (self.ID, self.subreddit, self.title, self.selftext, self.score,
                  self.upvote_ratio, self.permalink, self.num_comments, self.over_18,
                  self.created_utc)
        
        return tuptup
    
    
class Comment():
    def __init__(self, body, subreddit, ID, parent_ID, 
                 score, is_submitter, controversiality, depth, post_ID,
                 created_utc, distinguished, author,
                 ollama_scored_at = None, sentiment_score = None):
        self.body = body
        self.subreddit = subreddit
        self.ID = ID
        self.parent_ID = parent_ID
        self.post_ID = post_ID
        self.author = author
        self.score = score
        self.is_submitter = is_submitter
        self.controversiality = controversiality
        self.depth = depth
        self.created_utc = created_utc
        self.distinguished = distinguished
        self.ollama_scored_at = ollama_scored_at
        self.sentiment_score = sentiment_score
        
    def package_comment(self):
        
        ts = datetime.datetime.fromtimestamp(self.created_utc)
        
        tuptup = (self.ID, self.post_ID, self.parent_ID, self.author, self.body,
                  self.score, self.subreddit, self.is_submitter, self.distinguished,
                  self.controversiality, self.depth, ts)
        
        return tuptup
    
def land_at_echobase():
    
    con = psycopg2.connect(
        host=shh.db_ip,
        database=shh.db_name,
        user=shh.db_user,
        password=shh.db_password,
        port=shh.db_port
    )
    
    return con

def logger_jobber(job_name, status, error_msg=None):
    try:
        con = land_at_echobase()
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
        print(f"Log fail! {e}")
    
def parse_reddit_json(json_data):
    
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
        
    if not isinstance(json_data, list) or len(json_data) < 2:
        raise ValueError("JSON does not look like a standard Reddit Post+Comment list")
    
    p = json_data[0]['data']['children'][0]['data']
    
    POST = Post(
        subreddit = p.get('subreddit'),
        title = p.get('title'),
        selftext = p.get('selftext'),
        score = p.get('score', 0),
        upvote_ratio = p.get('upvote_ratio', 0.0),
        permalink = p.get('permalink'),
        ID = p.get('id'),
        num_comments = p.get('num_comments', 0),
        over_18 = p.get('over_18'),
        created_utc = p.get('created_utc', 0)
        )
    
    comments_listing = json_data[1]['data']['children']
    flattened_comments = []
    
    def process_comments_tree(comments_list, post_id):
        
        for child in comments_list:
            
            c = child['data']
            if child['kind'] == 'more':
                continue
            
            COMMENT = Comment(
                ID = c.get('id'),
                post_ID = post_id,
                parent_ID = c.get('parent_id'),
                author = c.get('author'),
                body = c.get('body'),
                score = c.get('score', 0),
                subreddit = c.get('subreddit'),
                is_submitter = c.get('is_submitter', False),
                distinguished = c.get('distinguished'),
                controversiality = c.get('controversiality', 0),
                depth = c.get('depth', 0),
                created_utc = c.get('created_utc', 0)
                )
        
            flattened_comments.append(COMMENT)
        
            replies = c.get('replies') # Safe get
            if replies and isinstance(replies, dict):
                process_comments_tree(replies['data']['children'], post_id)
            
    process_comments_tree(comments_listing, POST.ID)
    
    return POST, flattened_comments

def insert_post(cursor, post):
    sql = """
    INSERT INTO reddit.posts (
        post_id, subreddit, title, selftext, score, upvote_ratio, permalink,
        num_comments, over_18, created_utc
    ) VALUES (
        %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, to_timestamp(%s)
    )
    ON CONFLICT (post_id) DO NOTHING;
    """

    cursor.execute(sql, post.package_post())
    
def insert_comment(cursor, comments):
    if not comments:
        return None
    
    sql = """
    INSERT INTO reddit.comments (
        comment_id, post_id, parent_id, author, body, score, subreddit, is_submitter,
        distinguished, controversality, comment_depth, created_utc
    ) VALUES %s
    ON CONFLICT (comment_id) DO NOTHING;
    """
    
    clean_values = [c.package_comment() for c in comments]
    
    extras.execute_values(cursor, sql, clean_values)

def run_lola_run():
    con = None
    cur = None
    processed_count = 0
    error_count = 0
    
    max_loops = 20
    loop_count = 0
    
    try:
        con = land_at_echobase()
        cur = con.cursor()
        
        print(f"HERE WE GO!!!!!!  <<{datetime.datetime.now()}>>")
        
        while loop_count < max_loops:
            
            print(f"Batch {loop_count + 1} -+- {datetime.datetime.now()}")
            
            sql = "SELECT log_id, raw_json FROM reddit.jsons WHERE parsed_at IS NULL LIMIT 50"
            
            cur.execute(sql)
            rows = cur.fetchall()
            
            if not rows:
                print('Out of Data')
                break
            
            print(f"Process {loop_count + 1} Start -+- {datetime.datetime.now()}")
            
            batch_processed = 0
            
            for row in rows:
                log_id, raw_json = row
                
                try:
                    
                    post_obj, comments_list = parse_reddit_json(raw_json)
                    
                    insert_post(cur, post_obj)
                    
                    insert_comment(cur, comments_list)
                    
                    sql = "UPDATE reddit.jsons SET parsed_at = NOW() WHERE log_id = %s"
                    
                    cur.execute(sql, (log_id,))
                    con.commit()
                    batch_processed += 1
                
                except Exception as inner_e:
                    con.rollback()
                    error_count += 1
                    print(f"Failed to process {log_id} @ {datetime.datetime.now()} \n-- {inner_e}")
                    
            processed_count += batch_processed
            loop_count += 1
        
        if error_count > 0:
            status_msg = f"Processed {processed_count} with {error_count} errors"
            logger_jobber("REDDIT_RUN", 0, status_msg)
            
        else:
            logger_jobber("REDDIT_RUN", 1, "Success")
            
    except Exception as e:
        print(f"crit fail {e}")
        logger_jobber("REDDIT_RUN", 0, str(e))
    
    finally:
        if con:
            con.close()
            
if __name__ == '__main__':
    run_lola_run()
            
            