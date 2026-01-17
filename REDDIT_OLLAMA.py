# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 07:09:07 2026

@author: devli
"""

import datetime
import json
import requests
import psycopg2
import supersecrets as shh

DB_CONFIG = {
    "dbname": shh.db_name,
    "user": shh.db_user,
    "password": shh.db_password,
    "host": shh.db_ip,
    "port": shh.db_port
}

OLLAMA_URL = f"http://{shh.db_ip}:11434/api/generate"
MODEL_NAME = "llama3.1:8b"
TIME_WINDOW_START = 3
TIME_WINDOW_END = 12

SYSTEM_PROMPT = """
You are an expert Sentiment Regression Engine. Your goal is to score Reddit comments with HIGH GRANULARITY.
Do not stick to "safe" numbers like 0, +/-5, +/-10. Use the full integer scale:
    -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

### 1. VALENCE (Emotional Charge)
How does the comment FEEL?
* **-10 (Vitriolic):** "I hope you rot in hell." (Pure hate, violent)
* **-7 (Hostile):** "You are a complete moron." (Strong insults)
* **-3 (Annoyed/Sarcastic):** "Oh great, another one." (Eye-rolling, mild irritation)
* **0 (Neutral):** "The bus arrives at 5pm." (Pure fact)
* **+3 (Pleasant):** "That looks nice." (Polite, mild approval)
* **+7 (Enthusiastic):** "This is amazing work!" (Strong praise)
* **+10 (Ecstatic):** "Best day of my entire life!!" (Overwhelming joy)

### 2. SOCIAL INTENT (Relation to OP/Parent)
Is the user bonding with or attacking the parent author?
* **-10 (Destructive):** "OP is a liar and a scammer." (Character assassination)
* **-5 (Dismissive):** "You clearly didn't read the article." (Invalidation)
* **0 (Unrelated):** Talking to someone else, or shouting into the void.
* **+3 (Supportive):** "I agree with your point." (Validation)
* **+10 (Bonding):** "I love you man, we are in this together." (Deep connection)
* **SPECIAL RULE:** "Shared Anger" (e.g., "Yeah, that politician sucks!") is **positive social intent** (Bonding) even if Valence is Negative.

### 3. OUTLOOK (Worldview)
Is the comment hopeful or doomed?
* **-10 (Nihilistic):** "Humanity is a virus, let it end." (Total doom)
* **-8 (Doomerism):** "This is the end of American Democracy." (Local Doom)
* **-3 (Pessimistic):** "This legislation will never pass." (Cynicism)
* **0 (Realist/Unclear):** "We will see what happens."
* **+3 (Moderately hopefull):** "Maybe this will be good."
* **+7 (Optimistic):** "It's a step in the right direction."
* **+10 (Utopian):** "We are entering a golden age of peace."

### INSTRUCTIONS
1. Assign integers based on the anchors above. 
2. **USE THE FULL RANGE OF POSSIBLE SCORES**
### RESPONSE FORMAT
You must return a JSON object with a "reasoning" field first.
{
  "reasoning": "Please explain briefly how you derived the scores for this comment",
  "valence": 0,
  "social_intent": 0,
  "outlook": 0
}

### EXAMPLES

**EXAMPLE 1:**
    **CONTEXT**
    Subreddit Post: "Man has his 4th Amendment right violated while skateboarding across America"
    Post Body: ""
    
    
    **TARGET COMMENT**
    "Rights? This is the US lol, you think we have rights? Turn on the news."
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "'Right?' Indicates a humorous agreement with a previous post, therefore slight positive social intent. However it also indicates a negative view of American Sociery, so negative outlook. Humor is laced with a sinciere cynicism so negative valence.",
  "valence": -1,
  "social_intent": 4,
  "outlook": -4
}

**EXAMPLE 2:**
    **CONTEXT**
    Subreddit Post: "Fox News Host Jessica Tarlov Points Out ICE Failures in Minneapolis"
    Post Body: ""
    Parent Comment: She’s worked for Fox for years so she’s obviously fine with the gig. 

Not saying that has any bearing on whether she’s good at her job or worth listening to but it’s not like Fox are holding her there under duress.
    
    **TARGET COMMENT**
    "As a pundit she's a lot better than former The Five token liberal Bob Beckel. He was freaking spineless and they obviously put him on the show cuz he was a big fat guy"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment implies support for Jessica Tarlov and agreement with parent comment, but is outrigt rude and dismissive of Bob Beckel. Comment Implies Fox news employs tokenism in its treatment of liberal voices. Negative valence and outlook, positive social intent.",
  "valence": -6,
  "social_intent": 3,
  "outlook": -2
}

**EXAMPLE 3:**
    **CONTEXT**
    Subreddit Post: "The amount of medication I have to take for the rest of my life as a mid-twenties woman"
    Post Body: ""
    Parent Comment: Currently on hold with my insurance company trying to figure out why I’ve been denied at four different pharmacies to refill a prescription for an anti-rejection medication for a life saving transplant. I feel ya.
    
    **TARGET COMMENT**
    "Have you tried using GoodRX? My child has been denied “name brand” medication  (that has proven to be very effective for their condition vs generics) coverage prescribed by their doctor when my insurance switched, I used GoodRX to get a substantial discount on my kid’s prescriptions. I don’t work for GoodRX or being paid to promote them before anyone jumps on me, just sharing my experience."
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment is offering good faith assistance to someone who might need it. They share an intimate story and offer a positive solution to a problem they both encountered. Positive valence, social intent, and outlook.",
  "valence": 4,
  "social_intent": 8,
  "outlook": 3
}

**EXAMPLE 4:**
    **CONTEXT**
    Subreddit Post: "And then there is always eating with bare hands"
    Post Body: ""
    
    
    **TARGET COMMENT**
    "imo it is the other way around, chopsticks is just 2 sticks and fork is something specifically made for eating"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment offers humble correction over banal matter. Small positive valence for humility, small negative social intent, neutral outlook.",
  "valence": 1,
  "social_intent": -1,
  "outlook": 0
}

**EXAMPLE 5:**
    **CONTEXT**
    Subreddit Post: "Ryan Coogler’s ‘Sinners’ sweeps Astra Film Awards; Warner Bros. dominates with 11 wins"
    Post Body: ""
    
    
    **TARGET COMMENT**
    "Can we please get a Ryan Coogler Star Wars trilogy"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment notes Ryan Coogler's success and wants more of his films. Comment is wistfully hopefull for future good movies. Positive valence, neutral social intent, slight positive outlook.",
  "valence": 5, 
  "social_intent": 0,
  "outlook": 2
}

**EXAMPLE 6:**
    **CONTEXT**
    Subreddit Post: "Kennedy Center insists it broke up with opera, not the other way around"
    Post Body: ""
    
    
    **TARGET COMMENT**
    "Fox News statement: “All liberals/demonrats do is lie, every day all day long, every single word is a lie that Satan taught them.”

MAGA conclusion: “The only way we can fight back is to lie even harder, even more dishonestly than they do. It’s OK for us to do it, because we are the righteous in the eyes of God.”"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment is intensely disrespectful of American Conservatives, negative social outlook, negative valence. As the comment is in support with the OP, positive social intent",
  "valence": -6,
  "social_intent": 4,
  "outlook": -4
}

**EXAMPLE 7:**
    **CONTEXT**
    Subreddit Post: "Good food always"
    Post Body: ""
    Parent Comment: Good food? German? 🤔
    
    **TARGET COMMENT**
    "Bread! There's no other country that has as many different types of bread and the quality is excellent. Also, many types of sausage that goes well with the bread. We also have our own type of noodles (Spätzle = egg noodles), that go well with Gulasch (typically with Knödel though) and Geschnetzeltes for example. Also, roast pork, Currywurst, etc. And fish dishes in northern part of Germany"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment is happy to share information about their favorite german foods. Positive valence, positive social intent. Comment offers no opinion on the future of the world, neutral outlook.",
  "valence": 7,
  "social_intent": 5,
  "outlook": 0
}

**EXAMPLE 8:**
    **CONTEXT**
    Subreddit Post: "G7 To Postpone Annual Meeting To Accommodate White House Cage Match"
    Post Body: ""
    
    
    **TARGET COMMENT**
    "Spinless EU leaders. Hope he will go there and shit on their tables or something."
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    

{
  "reasoning": "Comment is negative and outrageously gross. Negative valence, negative outlook, neutral social intent.",
  "valence": -8,
  "social_intent": 0,
  "outlook": -7
}

"""

class CommentContext():
    def __init__(self, comment_id, comment_body, parent_id, post_title, post_body):
        self.comment_id = comment_id
        self.comment_body = comment_body
        self.parent_id = parent_id
        self.post_title = post_title
        self.post_body = post_body
        self.parent_comment = None
        self.valence = None
        self.social_intent = None
        self.outlook = None
        
    def snip_text(self, mode = 1):
        
        comment_library = {1: self.post_body,
                           2: self.parent_comment}
        
        text = comment_library.get(mode)
        
        if not text:
            return ""
        
        if len(text) > 500:
            post_snippet = text[:250] + " ... " + text[-250:]
        else:
            post_snippet = text
            
        return post_snippet

def temp_check(limit = 93.0):

    prom_api_url = f"http://{shh.db_ip}:9090/api/v1/query"

    prom_qry = """
    node_hwmon_temp_celsius{instance="node_exporter:9100", chip="pci0000:00_0000:00:18_3", sensor="temp3"}
    """

    try:
        response = requests.get(prom_api_url, params={'query': prom_qry}, timeout=2)

        if response.status_code == 200:
            data = response.json()

            results = data.get('data', {}).get('result', [])

            if results:

                current_temp = float(results[0]['value'][1])

                if current_temp >= limit:
                    return False, current_temp
                else:
                    return True, current_temp
            
            else:
                print("[WARN] Prometheus query returned empty")
                return True, 0.0
            
        else:
            print(f"[WARN] Promtheus API Broke -- {response.status_code}")
            return True, 0.0

    except Exception as e:
        print(f"Comeplete Failure to check temps - {e}")
        return True, 0.0

def is_go_time(start_time, finish_time, testing_mode = False):
    
    current_hour = datetime.datetime.now().hour
    
    return (start_time <= current_hour < finish_time) or testing_mode

def start_connection():
    
    return psycopg2.connect(**DB_CONFIG)

def logger_jobber(job_name, status, error_msg=None):
    try:
        con = start_connection()
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

def fetch_comments(connection):
    
    sql = """
    SELECT c.comment_id, c.body, c.parent_id, p.title as post_title, p.selftext as post_body
    FROM reddit.comments c
    JOIN reddit.posts p ON c.post_id = p.post_id
    WHERE c.ollama_scored_at IS NULL 
    AND distinguished IS NULL AND comment_depth <= 5 AND LENGTH(body) > 10
    LIMIT 1;
    """
    
    with connection.cursor() as cur:
        cur.execute(sql)
        next_up = cur.fetchone()
        
        if next_up is None:
            return None
        
        else:
            
            COMMENT = CommentContext(*next_up)
            
            if COMMENT.parent_id.startswith('t1_'):
                look_for = COMMENT.parent_id[3:]
                
                sql = f"SELECT body FROM reddit.comments WHERE comment_id = '{look_for}'"
                
                cur.execute(sql)
                papa_comment = cur.fetchone()
                
                if papa_comment:
                    COMMENT.parent_comment = papa_comment[0]
                
            return COMMENT
        
def get_scored(comment):
    
    if comment.parent_comment is not None:
        parent_drop_in = f"Parent Comment: {comment.snip_text(mode = 2)}"
    else:
        parent_drop_in = ""
    
    user_prompt = f"""
    **CONTEXT**
    Subreddit Post: "{comment.post_title}"
    Post Body: "{comment.snip_text()}"
    {parent_drop_in}
    
    **TARGET COMMENT**
    "{comment.comment_body}"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    """
    
    payload = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": SYSTEM_PROMPT,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.3,
            "num_thread": 3
            }
        }
    
    try:
        response = requests.post(OLLAMA_URL, json = payload)
        response.raise_for_status()
        result = response.json()
        return json.loads(result['response'])
    except Exception as e:
        print(f"Ollama Error: {e}")
        return None
    
def lets_a_go():
    print(f"Starting Senitment Analysis With --{MODEL_NAME.upper()}--")
    
    comments_processed = 0
    
    home = start_connection()
    
    while True:

        is_safe, current_temp = temp_check(limit = 93)

        if not is_safe:
            msg = f"Thermal Shutdown {current_temp}c @ {datetime.datetime.now()}"
            print(f"[!!!] {msg}")
            logger_jobber('REDDIT OLLAMA', 0, msg)
        
        if not is_go_time(TIME_WINDOW_START, TIME_WINDOW_END, testing_mode = False):
            print(f"Ceasing Run @ {datetime.datetime.now()}")
            logger_jobber('REDDIT OLLAMA', 1, 'Success')
            break
        
        try:
            comment = fetch_comments(home)
            
            if comment is None:
                print(f'No unprocessed comments found - breaking @ {datetime.datetime.now()}')
                logger_jobber('REDDIT OLLAMA', 1, 'Success')
                break
            
            print('----------------------------------------------------------')
            print(f"pulled comment {comment.comment_id}")
            scores = get_scored(comment)
            print(f"Comment {comment.comment_id} scored!")
                        
            with home.cursor() as cur:
            
                
                if scores:
                    sql = """
                    UPDATE reddit.comments
                    SET ollama_scored_at = NOW(),
                    valence = %s,
                    social_intent = %s,
                    outlook = %s,
                    ollama_reasoning = %s
                    WHERE comment_id = %s
                    """
                    
                    inputs = (scores.get('valence'),
                              scores.get('social_intent'),
                              scores.get('outlook'),
                              scores.get('reasoning'),
                              comment.comment_id)
                    
                    cur.execute(sql, inputs)
                    print(f"Processed comment {comment.comment_id} --@-- {datetime.datetime.now()}")
                    comments_processed += 1
                    
                else:
                    sql = """
                    UPDATE reddit.comments
                    SET ollama_scored_at = NOW()
                    WHERE comment_id = %s
                    """
                    
                    cur.execute(sql, (comment.comment_id,))
                    
                    print(f"[!!] failed to score comment {comment.comment_id}")
                    comments_processed += 1
                    
                home.commit()
                
        except KeyboardInterrupt:
            print(f'Manual Stop. Comments processed: {comments_processed}')
            logger_jobber('REDDIT OLLAMA', 1, 'Success')
            break
                
        except Exception as e:
            print(f'[!!] MAJOR ERROR -- {e}')
            logger_jobber('REDDIT OLLAMA', 0, str(e))
            break
        
    home.close()
    print(f"JOB FINISHED: {comments_processed} comments processed")
      
if __name__ == "__main__":
    lets_a_go()