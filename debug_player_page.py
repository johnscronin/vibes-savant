#!/usr/bin/env python3
"""
Debug script to investigate HRL player page structure and API calls.
"""
import json
from playwright.sync_api import sync_playwright

def debug_page(slug):
    url = f"https://hrltwincities.com/player/{slug}"
    api_calls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Intercept API calls
        def on_request(request):
            if 'api' in request.url.lower() or 'stats' in request.url.lower():
                api_calls.append(('REQUEST', request.url, request.method))

        def on_response(response):
            if 'api' in response.url.lower() or 'stats' in response.url.lower():
                try:
                    body = response.body()
                    api_calls.append(('RESPONSE', response.url, response.status, len(body), body[:500] if body else b''))
                except:
                    pass

        page.on('request', on_request)
        page.on('response', on_response)

        page.set_default_timeout(30000)
        page.goto(url, wait_until='networkidle')
        page.wait_for_timeout(5000)

        # Print all API calls
        print(f"\n=== API calls for {slug} ===")
        for call in api_calls:
            if call[0] == 'REQUEST':
                print(f"REQ: {call[2]} {call[1]}")
            else:
                print(f"RES: {call[2]} {call[1]} ({call[3]} bytes)")
                if call[4]:
                    try:
                        data = json.loads(call[4])
                        print(f"     Preview: {str(data)[:300]}")
                    except:
                        print(f"     Raw: {call[4][:200]}")

        # Look for table elements
        tables = page.query_selector_all('table')
        print(f"\nFound {len(tables)} <table> elements")

        # Look for any stats-related elements
        stats_els = page.query_selector_all('[class*="stat"], [class*="season"], [class*="table"]')
        print(f"Found {len(stats_els)} stat/season/table elements")

        # Try to get all text to see what data is there
        content = page.content()
        print(f"\nTotal HTML length: {len(content)}")

        # Find any JSON data embedded in page
        import re
        # Look for JSON-like structures
        json_matches = re.findall(r'window\.__.*?=.*?;', content[:5000])
        for m in json_matches[:5]:
            print(f"window data: {m[:200]}")

        # Look for season data
        if '2025' in content or '2024' in content:
            # Find context around year
            idx = content.find('2025')
            if idx > 0:
                print(f"\nContext around '2025': ...{content[idx-100:idx+200]}...")

        browser.close()
        return api_calls


if __name__ == '__main__':
    calls = debug_page('Psych')
