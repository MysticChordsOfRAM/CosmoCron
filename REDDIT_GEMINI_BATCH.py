# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 07:09:07 2026

@author: devli
"""

import datetime
import json
import psycopg2
import supersecrets as shh
from random import randint
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

DB_CONFIG = {
    "dbname": shh.db_name,
    "user": shh.db_user,
    "password": shh.db_password,
    "host": shh.db_ip,
    "port": shh.db_port
}

MODEL_NAME = "gemini-2.5-flash"

client = genai.Client(api_key = shh.gemini_reddit_key)

SYSTEM_PROMPT = """
You are a specialized Sentiment Analysis Engine. Your goal is to score Reddit comments with high granularity.

### CORE LOGIC RULES
1. VALENCE: Capture the emotional temperature. Sarcasm is almost always negative.
2. SOCIAL INTENT: Distinguish between attacking the author vs. bonding over a shared enemy. 
   - "Shared Anger" (e.g., "I hate this politician!") is bonding with the parent author (+Social Intent).
   - Direct attacks (e.g., "You are an idiot") are -Social Intent.
3. OUTLOOK: Realism or uncertainty is 0. Only score extremes for clear Doomerism or Utopianism.

### EXAMPLES
"""

# We format the examples as a list of dictionaries to make them easy to iterate or join
FEW_SHOT_EXAMPLES = [
    {
        "context": """Subreddit Post: "Progress Is Starting to Feel Less Linear"
Post Body: "Some technologies stall for years then suddenly accelerate... The future feels less like a straight line forward and more like a series of uneven jumps."
Parent Comment: Not everyone will resonate with every post, and that’s fine. I’ll continue sharing perspectives I think add value.""",
        "target": "'resonate'. Look at you, you can't even write a single line without resorting to chatGPT. And you say you're 'adding value'? You're removing value from this website by spamming it full of pointless, shallow engagement bait that you didn't even come up with.",
        "output": {
            "reasoning": "Comment is outwardly hostile to the parent comment, questioning their value in the discussion and mocking them for using AI. Negative Valence and Social Intent despite neutral context.",
            "valence": -7, "social_intent": -7, "outlook": 0
        }
    },
    {
        "context": """Subreddit Post: "The amount of medication I have to take for the rest of my life as a mid-twenties woman"
Post Body: ""
Parent Comment: Currently on hold with my insurance company trying to figure out why I’ve been denied at four different pharmacies... I feel ya.""",
        "target": "Have you tried using GoodRX? My child has been denied “name brand” medication coverage... I used GoodRX to get a substantial discount. I don’t work for GoodRX... just sharing my experience.",
        "output": {
            "reasoning": "Offering good faith assistance and sharing a personal story to solve a mutual problem. High empathy results in strongly positive social intent.",
            "valence": 4, "social_intent": 8, "outlook": 3
        }
    },
    {
        "context": """Subreddit Post: "Kennedy Center insists it broke up with opera, not the other way around"
Post Body: ""
Parent Comment: N/A""",
        "target": "Fox News statement: “All liberals/demonrats do is lie...” MAGA conclusion: “The only way we can fight back is to lie even harder... It’s OK for us to do it, because we are the righteous in the eyes of God.”",
        "output": {
            "reasoning": "Intensely disrespectful of the subject matter (-Valence), but identifies as 'Bonding' with a likely sympathetic audience (+Social Intent). Negative outlook on the state of political discourse.",
            "valence": -6, "social_intent": 4, "outlook": -4
        }
    }
]

# Helper to assemble the prompt in your script
def get_final_system_prompt():
    example_str = ""
    for i, ex in enumerate(FEW_SHOT_EXAMPLES):
        example_str += f"\nEXAMPLE {i+1}:\nContext: {ex['context']}\nTarget: {ex['target']}\nOutput: {json.dumps(ex['output'])}\n"
    return SYSTEM_PROMPT + example_str

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
            return "N/A"
        
        if len(text) > 500:
            post_snippet = text[:250] + " ... " + text[-250:]
        else:
            post_snippet = text
            
        return post_snippet

class SentimentResponse(BaseModel):
    reasoning: str = Field(
        description="A brief explanation of why the scores were chosen based on the comment text."
    )
    valence: int = Field(
        ge=-10, le=10, 
        description="Emotional charge from -10 (vitriolic) to 10 (ecstatic)."
    )
    social_intent: int = Field(
        ge=-10, le=10, 
        description="Social bonding vs attack from -10 (destructive) to 10 (bonding)."
    )
    outlook: int = Field(
        ge=-10, le=10, 
        description="Worldview from -10 (nihilistic) to 10 (utopian)."
    )

def start_connection():
    
    return psycopg2.connect(**DB_CONFIG)

def get_latest_job(connection):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT job_id FROM reddit.batch_jobs 
            WHERE status NOT IN ('SUCCEEDED', 'FAILED') 
            ORDER BY submitted_at ASC LIMIT 1
        """)
        res = cur.fetchone()
        return res[0] if res else None
    
def download_update():
    home = start_connection()

    job_id = get_latest_job(home)
    if not job_id:
        print('No job to download')
        return None
    
    job = client.batches.get(name = job_id)

    if job.state_name != 'JOB_STATE_SUCCEEDED':
        print('Job run failed?')
        return None
    
    print('Verified Job Completion, Downloading...')

    output_file = job.output_file_names[0]
    content = client.files.download(name = output_file)

    update_data = []
    failed_validation = 0

    for line in content.decode().splitlines():
        data = json.loads(line)
        comment_id = data['key']
        raw_text = data['response']['candidates'][0]['content']['parts'][0]['text']

        try:
            scores = SentimentResponse.model_validate_json(raw_text)
            update_data.append((scores.valence, scores.social_intent, scores.outlook,
                                scores.reasoning, comment_id))
        except Exception as e:
            print('Validation Failure')
            failed_validation += 1
        
    sql = """
        UPDATE reddit.comments AS c SET
            gemini_scored_at = NOW(),
            valence = v.val, social_intent = v.soc, outlook = v.out, gemini_reasoning = v.reas,
            submitted_to_gemini = FALSE
        FROM (VALUES %s) AS v(val, soc, out, reas, id)
        WHERE c.comment_id = v.id
    """

    with home.cursor() as cur:
        if update_data:
            psycopg2.extras.execute_values(cur, sql, update_data)
        cur.execute("UPDATE reddit.batch_jobs SET status = 'SUCCEEDED', completed_at = NOW() WHERE job_id = %s", (job_id,))
    
    home.commit()
    home.close()
    print(f"Pipeline complete: {len(update_data)} updated, {failed_validation} failed validation.")
    return True

def fetch_comments(connection, limit):
    
    sql = f"""
    SELECT c.comment_id, c.body, c.parent_id, p.title as post_title, p.selftext as post_body
    FROM reddit.comments c
    JOIN reddit.posts p ON c.post_id = p.post_id
    WHERE c.gemini_scored_at IS NULL 
    AND c.submitted_to_gemini = FALSE
    AND c.distinguished IS NULL 
    AND c.body NOT IN ('[deleted]', '[removed]')
    AND LENGTH(c.body) > 10
    LIMIT {limit};
    """
    
    with connection.cursor() as cur:
        cur.execute(sql)
        batch = cur.fetchall()
        
        sorted_comments = []

        for next_up in batch:
            
            COMMENT = CommentContext(*next_up)
            
            if COMMENT.parent_id.startswith('t1_'):
                look_for = COMMENT.parent_id[3:]
                
                sql = f"SELECT body FROM reddit.comments WHERE comment_id = '{look_for}'"
                
                cur.execute(sql)
                papa_comment = cur.fetchone()
                
                if papa_comment:
                    COMMENT.parent_comment = papa_comment[0]
                
            sorted_comments.append(COMMENT)

        return sorted_comments

def make_ro(comment):
    
    user_prompt = f"""
    **CONTEXT**
    Subreddit Post: "{comment.post_title}"
    Post Body: "{comment.snip_text()}"
    Parent Comment: "{comment.snip_text(mode = 2)}"
    
    **TARGET COMMENT**
    "{comment.comment_body}"
    
    **INSTRUCTION**
    Analyze the TARGET COMMENT. Return JSON.
    """

    ro = {
        "key": comment.comment_id,
        "request": {
            "system_instruction": {"parts": [{"text": get_final_system_prompt()}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generation_config": {
                "response_mime_type": "application/json",
                "response_schema": SentimentResponse.model_json_schema()
            }
          }
        }
    
    return ro

def assemble_batch(comment_array):

    ro_array = [make_ro(i) for i in comment_array]

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"sentiment_batch_{ts}.jsonl"

    with open(filename, 'w') as f:
        for r in ro_array:
            f.write(json.dumps(r) + '\n')

    return filename

def hit_send_you_coward(limit = 8000):
    home = start_connection()

    comment_series = fetch_comments(home, limit)

    load_file = assemble_batch(home, comment_series)
    up = client.files.upload(file = load_file)
    job = client.batches.create(model = MODEL_NAME, src = up.name)

    sql_meta = """
    INSERT INTO reddit.batch_meta (job_id, input_file_id, comment_count)
    VALUES (%s, %s, %s);
    """

    sql_submit = """
    UPDATE reddit.comments 
    SET submitted_to_gemini = TRUE 
    WHERE comment_id = ANY(%s);
    """
    with home.cursor() as cur:
        cur.execute(sql_meta, (job.name, up.name, len(comment_series)))
        comment_ids = [c.comment_id for c in comment_series]
        cur.execute(sql_submit, (comment_ids, ))
    home.commit()
    home.close()

if __name__ == "__main__":
    hit_send_you_coward(limit = 1000)
    download_update()