import os, json
from dotenv import load_dotenv
load_dotenv('.env')
from steel import Steel

client = Steel(steel_api_key=os.getenv('STEEL_API_KEY'))

# Try the /api/cards endpoint with no params
r = client.scrape(url='https://onepiece.limitlesstcg.com/api/cards', delay=3.0)
print('Response:')
print(r.content.html[:1000])

# Check if it might be returning JavaScript that renders the content
# Try with extra headers to pretend to be a browser
r2 = client.scrape(
    url='https://onepiece.limitlesstcg.com/api/cards',
    delay=3.0,
    extra_headers={'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}
)
print('\nWith JSON headers:')
print(r2.content.html[:500])