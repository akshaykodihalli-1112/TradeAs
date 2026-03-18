name: Fetch Dhan Security IDs

on:
  schedule:
    - cron: '30 0 * * 0'
  workflow_dispatch:

jobs:
  fetch-ids:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install requests

      - name: Test CSV reachability
        run: |
          echo "Testing CSV URL..."
          curl -s -o /tmp/test.csv -w "HTTP %{http_code} | Size: %{size_download} bytes\n" \
            "https://images.dhan.co/api-data/api-scrip-master.csv"
          echo "First line: $(head -1 /tmp/test.csv)"

      - name: Fetch correct IDs
        run: python fetch_ids.py

      - name: Verify output
        run: |
          echo "Symbols fetched: $(python -c 'import json; print(len(json.load(open("correct_ids.json"))))')"

      - name: Commit correct_ids.json
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add correct_ids.json
          git diff --staged --quiet || git commit -m "chore: update Dhan NSE_EQ security IDs [skip ci]"
          git push
