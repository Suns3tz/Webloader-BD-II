import json
from urllib.parse import unquote

input_file = "webCrawler/wiki_data/wiki_data.jsonl"
output_file = "webCrawler/wiki_data/wiki_dataREVISED.jsonl"

seen_urls = set()
duplicates = 0
total = 0

def normalize_url(url):
    if not url:
        return ""
    url = url.strip().rstrip('/')
    url = unquote(url)
    url = url.lower()
    return url

with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
    for line in infile:
        total += 1
        try:
            obj = json.loads(line)
            url = obj.get("url")
            norm_url = normalize_url(url)
            if norm_url and norm_url not in seen_urls:
                seen_urls.add(norm_url)
                outfile.write(json.dumps(obj, ensure_ascii=False) + "\n")
            else:
                duplicates += 1
        except json.JSONDecodeError:
            continue  # Skip malformed lines if any

print(f"Processed {total} lines. Removed {duplicates} duplicates. Output: {output_file}")