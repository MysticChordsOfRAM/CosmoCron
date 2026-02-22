import psycopg2
from psycopg2 import extras
import re
import supersecrets as shh

DB_CONFIG = {
    "dbname": shh.db_name,
    "user": shh.db_user,
    "password": shh.db_password,
    "host": shh.db_ip,
    "port": shh.db_port
}

CLASSA = {'this', 'based', 'agreed', 'exactly', 'true', '100', 'this is the way', 'yes', 'yeah', 'yep'}
CLASSB = {'lol', 'lmao', 'lmfao', 'haha', 'dead', 'rofl'}
CLASSC = {'cap', 'bullshit', 'fake', 'cope', 'delusional', 'no', 'nope'}
CLASSD = {'source', 'link', 'why', 'what', 'sauce'}
CLASSE = {'thanks', 'thank you', 'ty', 'tysm', 'thanks op'}

# (V, SI, O)
CLASSF = {
    'nta': (2, 6, 0),
    'yta': (-2, -7, 0),
    'esh': (-3, 0, 0),
    'nah': (2, 4, 0),
    'good bot': (2, 3, 0),
    'bad bot': (-2, -3, 0)
}

class micro_comment():
    def __init__(self, comment_id, comment_body, parent_id, parent_outlook, parent_valence, parent_intent):
        self.comment_id = comment_id
        self.comment_body = comment_body
        self.text = self.clean_text()
        self.parent_id = parent_id
        self.parent_outlook = parent_outlook
        self.parent_valence = parent_valence
        self.parent_intent = parent_intent
        self.valence = None
        self.social_intent = None
        self.outlook = None
        self.note = None
        
    def clean_text(self):
        """Strips punctuation, lowercases, and squeezes repeating characters."""

        if not self.comment_body:
            return ""
    
        cleaned = re.sub(r'[^\w\s]', '', str(self.comment_body).lower()).strip()
        cleaned = re.sub(r'(.)\1{2,}', r'\1', cleaned)

        return cleaned
    
    def score(self):
        if self.parent_id and self.parent_id.startswith('t3_'):
            safe_outlook = 0
            safe_valence = 0
            safe_intent = 0
        else:
            safe_outlook = self.parent_outlook
            safe_valence = self.parent_valence
            safe_intent = self.parent_intent

        if safe_outlook is None or safe_valence is None or safe_intent is None:
            self.valence = None
            self.social_intent = None
            self.outlook = None
            return
        
        if self.text in CLASSA:
            self.valence = 0
            self.social_intent = 5
            self.outlook = safe_outlook
            self.note = "AUTO-SCORED: Class A Amplified Agreement"

        elif self.text in CLASSB:
            self.valence = 3
            self.social_intent = int(safe_intent * 0.5) 
            self.outlook = 0
            self.note = "AUTO-SCORED: Class B Laughter"

        elif self.text in CLASSC:
            self.valence = -3
            self.social_intent = -3
            self.outlook = safe_outlook * -1
            self.note = "AUTO-SCORED: Class C Hostile"

        elif self.text in CLASSD:
            self.valence = 0
            self.social_intent = -1
            self.outlook = 0
            self.note = "AUTO-SCORED: Class D Questioning"

        elif self.text in CLASSE:
            self.valence = 2
            self.social_intent = 4
            self.outlook = 0
            self.note = "AUTO-SCORED: Class E Thanks"

        elif self.text in CLASSF:
            v, s, o = CLASSF.get(self.text)
            self.valence = v
            self.social_intent = s
            self.outlook = o
            self.note = "AUTO-SCORED: Class F Reddit Code"

def run_micro_scorer():
    home = psycopg2.connect(**DB_CONFIG)
    
    sql_out = r"""
        SELECT c.comment_id, c.body, c.parent_id, p.outlook, p.valence, p.social_intent
        FROM reddit.comments c
        LEFT JOIN reddit.comments p ON c.parent_id = 't1_' || p.comment_id
        WHERE c.gemini_scored_at IS NULL
        AND c.submitted_to_gemini = FALSE
        -- Performance safeguard: Prevents running heavy regex on 5,000-word essays
        AND LENGTH(c.body) < 25
        
        -- The Airtight Mirror: Only pulls comments that exactly match our micro-sentiments
        AND TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(c.body), '[^\w\s]', '', 'g'), '(.)\1{2,}', '\1', 'g')) IN (
            -- Class A & H: Amplifiers and Yes
            'this', 'based', 'agreed', 'exactly', 'true', '100', 'this is the way', 'yes', 'yeah', 'yep',
            -- Class B: Amusements
            'lol', 'lmao', 'lmfao', 'haha', 'dead', 'rofl',
            -- Class C & H: Hostiles and No
            'cap', 'bullshit', 'fake', 'cope', 'delusional', 'no', 'nope',
            -- Class D: Inquiries
            'source', 'link', 'why', 'what', 'sauce',
            -- Class E: Gratitude
            'thanks', 'thank you', 'ty', 'tysm', 'thanks op',
            -- Classes F & G: Static Scores
            'nta', 'yta', 'esh', 'nah', 'good bot', 'bad bot'
        )
    """
    sql_in = """
        UPDATE reddit.comments AS c SET
            gemini_scored_at = NOW(),
            valence = v.val, social_intent = v.soc, outlook = v.out, gemini_reasoning = v.reas
        FROM (VALUES %s) AS v(val, soc, out, reas, id)
        WHERE c.comment_id = v.id
    """
    
    update_data = []

    with home.cursor() as cur:
        cur.execute(sql_out)
        candidates = cur.fetchall()
        COMMENTS = [micro_comment(*i) for i in candidates]

        for cmnt in COMMENTS:
            cmnt.score()
            if cmnt.valence is not None:
                update_data.append((cmnt.valence, cmnt.social_intent, cmnt.outlook, cmnt.note, cmnt.comment_id))

        if update_data:
            extras.execute_values(cur, sql_in, update_data)
            home.commit()
            print(f"Micro-Scorer complete: Auto-scored {len(update_data)} comments.")
        else:
            print("Micro-Scorer complete: 0 comments to update.")

    home.close()

if __name__ == "__main__":
    run_micro_scorer()