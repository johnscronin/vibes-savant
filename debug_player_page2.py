#!/usr/bin/env python3
"""
Debug script - intercept ALL network requests, find stats API endpoint.
"""
import json, re
from playwright.sync_api import sync_playwright

def debug_page(slug):
    url = f"https://hrltwincities.com/player/{slug}"
    all_requests = []
    all_responses = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def on_request(request):
            all_requests.append(request.url)

        def on_response(response):
            url = response.url
            # Capture JSON responses that look like stats
            if response.status == 200:
                content_type = response.headers.get('content-type', '')
                if 'json' in content_type:
                    try:
                        body = response.body()
                        all_responses.append((url, body[:2000]))
                    except:
                        pass

        page.on('request', on_request)
        page.on('response', on_response)

        page.set_default_timeout(45000)
        print(f"Loading {url}...")
        page.goto(url, wait_until='networkidle')
        page.wait_for_timeout(8000)

        # All requests made
        print(f"\n=== ALL {len(all_requests)} requests ===")
        for r in all_requests:
            print(f"  {r}")

        print(f"\n=== JSON responses ({len(all_responses)}) ===")
        for url, body in all_responses:
            print(f"\nURL: {url}")
            try:
                data = json.loads(body)
                print(f"Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                print(f"Preview: {str(data)[:500]}")
            except:
                print(f"Raw: {body[:300]}")

        browser.close()


if __name__ == '__main__':
    debug_page('Psych')
