# -*- coding: utf-8 -*-
"""
Created on Sat Jan 10 03:58:06 2026

@author: devli
"""

import requests
import json
import datetime
import time
import random
import sys
import psycopg2
import supersecrets as shh

headers = {'User-Agent': 'NegativityProject:v1.0 (by /u/crazykingludwig)'}

subreddits = ['news', 'worldnews', 'gaming', 'todayilearned', 'music', 'mildlyinteresting', 'nottheonion', 
              'sports', 'politics', 'conservative', 'technology', 'science', 'interestingasfuck',
              'mildlyinfuriating', 'relationship_advice', 'videos', 'movies', 'facepalm',
              'upliftingnews', 'futurology', 'europe', 'fauxmoi', 'askmen', 'askwomen',
              'aww', 'travel', 'antiwork', 'latestagecapitalism', 'marvelstudios', 'starwars',
              'finance', 'wallstreetbets', 'AmItheAsshole', 'lego', 'memes', 'popculturechat', 
              'frugal', 'teenagers', 'offmychest', 'outoftheloop', 'meirl', 'mommit', 'teachers',
              'recipes', 'TaylorSwift', 'blackpeopletwitter', 'parenting', 'whitepeopletwitter']

run_schedule = {i: sub for i, sub in enumerate(subreddits)}

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
    
    cur.execute("CREATE SCHEMA IF NOT EXISTS reddit")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS reddit.jsons (
        log_id SERIAL PRIMARY KEY,
        ingest_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        subreddit VARCHAR(50),
        post_id VARCHAR(50),
        request_url TEXT,
        http_status INTEGER,
        raw_json JSONB
        );
    """
    
    cur.execute(create_table_sql)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_reddit_json_post ON reddit.jsons (post_id);")

    con.commit()
    cur.close()
    con.close()

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

def time_converter():
    NOW = datetime.datetime.now()
    HOUR = NOW.hour
    MINUTE = NOW.minute

    if MINUTE > 29:
        offset = 0.5
    else:
        offset = 0
        
    base = HOUR + offset
    
    return int(base * 2)

def go_get_it(subreddit):
    con = get_db_connection()
    cur = con.cursor()
    
    print(f"-- {datetime.datetime.now()} --  <<>> -- {subreddit} --")
    
    target_url = "https://www.reddit.com/r/" + subreddit + "/top.json?limit=10&t=day"
    
    try:
        time.sleep(random.randint(15, 30))
        resp = requests.get(target_url, headers = headers)
        
        if resp.status_code != 200:
            print(f'Fetch failed -- Status: {resp.status_code} -- {datetime.datetime.now()} --')
            return False
        
        posts = resp.json()['data']['children']
        
    except Exception as e:
        print(f"Python Error: {e} -- {datetime.datetime.now()}")
        return False
    
    for p in posts:
        post_data = p['data']
        post_id = post_data['id']
        permalink = post_data['permalink']
        
        post_url = "https://www.reddit.com" + permalink + ".json?sort=top"
        
        time.sleep(random.randint(4, 15))
        
        try:
            post_resp = requests.get(post_url, headers = headers)
            
            status = post_resp.status_code
            
            if status == 200:
                raw_blob = json.dumps(post_resp.json())
                
                sql_insert = """
                INSERT INTO reddit.jsons
                (subreddit, post_id, request_url, http_status, raw_json)
                VALUES (%s, %s, %s, %s, %s)
                """
                
                cur.execute(sql_insert, (subreddit, post_id, post_url, status, raw_blob))
                con.commit()
                
                print(f'Post saved: {post_id} -- {datetime.datetime.now()}')
                
            else:
                print(f"Failed on post {post_id} -- {datetime.datetime.now()}")
                return False
                
        except Exception as e:
            print(f"Error: {e} -- {datetime.datetime.now()}")
            con.rollback()
            continue
            
    cur.close()
    con.close()

    return True
    
if __name__ == '__main__':
    try:
        init_db()
    except Exception as e:
        print(f"Couldn't initialize database - {e}")
        sys.exit(1)

        
    slot = time_converter()
    capture_target = run_schedule.get(slot)
    
    print(f'Begining run to capture {capture_target} @ {datetime.datetime.now()}')
    
    if capture_target:
        
        time.sleep(random.randint(30, 90))
        
        win = go_get_it(capture_target)

        if win:
            logger_jobber("REDDIT_INGEST", 1, "Success")
        else:
            logger_jobber("REDDIT_INGEST", 0, "Job Failed")
    
    else:
        print('time_converter failed - no capture')
        logger_jobber("REDDIT_INGEST", 0, "Job Failed")
        
    print(f"Fin {datetime.datetime.now()}")
    
        
    
    
    
