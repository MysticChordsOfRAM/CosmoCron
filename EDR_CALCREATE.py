import json
import time
import httpx
import pytz
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Optional
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from random import randint
from pydantic import BaseModel, Field

import supersecrets as shh

PDF_URL = "https://edr.state.fl.us/Content/calendar.pdf"
OUTPUT_FILE = f"/output/{shh.cal_id}.ics"
API_KEY = shh.gemini_key
#MODEL_NAME = 'gemini-3-flash-preview'
MODEL_NAME = 'gemini-2.5-flash'

DB_PARAMS = {'host': shh.db_ip,
			 'user': shh.db_user,
			 'dbname': shh.db_name,
			 'password': shh.db_password,
			 'port': shh.db_port}

class CalendarEvent(BaseModel):
    title: str = Field(description="The topic of the meeting, taken from the TOPIC Column (e.g., 'Impact Conference').")
    date: str = Field(description="The date of the event in DD-MM format")
    start_time: str = Field(description="Start time in 24-hour HH:MM format")
    location: Optional[str] = Field(None, description="The room number or venue name")
    status: str = Field("active", description="Set to 'cancelled' if there is a strikethrough or 'CANCELED' text; otherwise 'active'")
    description: Optional[str] = Field(None, description="Any additional notes, such as 'Moved to Room 202' or 'Revised'")

class CalendarResponse(BaseModel):
    events: List[CalendarEvent]
	
class Event():
	def __init__(self, title, start, end, location, description = None, status = 'active'):
		self.title = title
		self.start = start
		self.end = end
		self.location = location
		self.description = description
		self.status = status

	def package_event(self):
		tuptup = (self.title, self.start, self.end, self.location, self.description, self.status)
		return tuptup

def log(msg):
	print(f"{[datetime.now().strftime('%H:%M:%S')]} {msg}", flush = True)

def sql_sync(events, db_init):
	if not events:
		return
	
	earliest_date = min([e.start for e in events])

	SQL_DELETE = """
	DELETE FROM prd.edr_calevents
	WHERE start_time >= %s;
	"""

	SQL_INSERT = """
	INSERT INTO prd.edr_calevents (event_title, start_time, end_time, event_loc, event_desc, status)
	VALUES (%s, %s, %s, %s, %s, %s);
	"""

	with psycopg2.connect(**db_init) as home:
		with home.cursor() as cur:

			cur.execute(SQL_DELETE, (earliest_date, ))

			data_tuples = [e.package_event() for e in events]
			execute_batch(cur, SQL_INSERT, data_tuples)
		
		home.commit()
	log(f"Database Sync Complete for Dates >= {earliest_date.date()}")

def make_gemini_do_the_hard_part(client, model, document, retries = 4):
	prompt = """
	System Prompt: You are a specialized data extraction assistant for the Florida Revenue Estimating Conference.

	Task: Extract all scheduled meetings from the provided PDF into the specified JSON format.

	Rules:
	1. Date Handling: Convert all dates to DD-MM.
	2. Title Handling: Do NOT merge the contents of the Conference and the Topic Columns. The title is to be taken from the Topic Column only.
	3. Cancellations: If an event has a strikethrough or the word "CANCELED" (bold or otherwise) is next to it, set the status field to "cancelled". 
	   Move any cancellation notes (e.g., "Moved to Friday") into the description.
	4. Time: Convert all times to 24-hour format (HH:MM).
	5. Cleanup: Ignore all page headers, footers, and decorative text.
	6. Output: Return only a valid JSON list of events.
	"""
		
	log("Gemini Working...")

	for i in range(retries):
		try:
			response = client.models.generate_content(
			model = model,
			contents = [
				types.Part.from_bytes(
					data = document,
					mime_type = 'application/pdf'
				), 
				prompt
			],
			config = {"response_mime_type": "application/json",
					  "response_json_schema": CalendarResponse.model_json_schema()}
			)

			return response
		
		except Exception as e:
			if "503" in str(e) or "overloaded" in str(e).lower():
				wait_time = (2 ** i) + (randint(1, 1000) / 1000)
				log(f"Server Overloaded, Retry #{i+1}")
				time.sleep(wait_time)
			else:
				raise e

	return None 

def pull_calendar():
	log('starting job')
	
	try:
		doc_data = httpx.get(PDF_URL).content
		log(f"Calendar is {len(doc_data)} bytes today")
	except Exception as e:
		log(f"Download Failed: {e}")
		return None

	client = genai.Client(api_key = API_KEY)

	try:
		response = make_gemini_do_the_hard_part(client, MODEL_NAME, doc_data)
		log("Received Response")
		clean_text = response.text.replace("```json", "").replace("```", "").strip()
		data = json.loads(clean_text)		 
		event_list = data.get('events', [])
		log(f"Gemini Found {len(event_list)} events!")
	except Exception as e:
		log(f"Gemini Failure: {e}")
		log(f"Raw Response: {response.text}")
		return None
		
	parsed_events = []
	current_year = datetime.now().year
	timezone = pytz.timezone('US/Eastern')
	
	for item in event_list:
		DATE = item['date']
		TIME = item['start_time']

		try:
			dt_str = f"{DATE} {current_year} {TIME}"
			dt_naive = datetime.strptime(dt_str, "%d-%m %Y %H:%M")
			dt = timezone.localize(dt_naive)

			EVENT = Event(item.get('title'), dt, None, item.get('location'), item.get('description'), item.get('status'))
			
			parsed_events.append(EVENT)
		except Exception as e:
			log(f"Bad Data Found {e}")

	parsed_events.sort(key = lambda x: x.start)
	
	for i, citem in enumerate(parsed_events):
		
		start_time = citem.start
		
		limit_5h = start_time + timedelta(hours = 5)
		candidates = [limit_5h]
		
		limit_7pm = start_time.replace(hour = 19, minute = 0, second = 0)
		if limit_7pm > start_time:
			candidates.append(limit_7pm)
			
		if i + 1 < len(parsed_events):
			next_event = parsed_events[i + 1]
			if next_event.start.date() == start_time.date():
				candidates.append(next_event.start)

		end_time = min(candidates)

		duration = end_time - start_time

		if duration < timedelta(minutes = 15):
			duration = timedelta(minutes = 15)

		citem.end = start_time + duration
	
	sql_sync(parsed_events, DB_PARAMS)

if __name__ == '__main__':
	time.sleep(randint(3, 14))
	pull_calendar()

