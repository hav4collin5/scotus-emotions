import json
import requests
from bs4 import BeautifulSoup
import time
import random
import urllib.parse
import pandas as pd


def extract_case_details(url):
    """Extracts relevant case details from a given JSON URL."""
    response = requests.get(url)
    data = response.json()

    details = {
        "decision_date": data.get("decision_date"),
        "official_citation": None,
        "case_name": data.get("name_abbreviation"),
        "court_name": data.get("court", {}).get("name_abbreviation"),
        "docket_number": data.get("docket_number"),
        "panel_of_judges": data.get("casebody", {}).get("judges"),
        "majority_author": None,
        "concurrence_author": None,
        "dissent_author": None
    }

    # Extract the official citation
    for citation in data.get("citations", []):
        if citation.get("type") == "official":
            details["official_citation"] = citation.get("cite")
            break

    # Extract opinion authors
    opinions = data.get("casebody", {}).get("opinions", [])
    for opinion in opinions:
        if opinion.get("type") == "majority":
            details["majority_author"] = opinion.get("author")
        elif opinion.get("type") == "concurrence":
            details["concurrence_author"] = opinion.get("author")
        elif opinion.get("type") == "dissent":
            details["dissent_author"] = opinion.get("author")

    return details


def get_all_json_links(base_url):
    """
    Fetches all JSON case links by first navigating into volumes, then cases.
    """
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract links that look like volumes (end with "/")
    volume_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('/')]

    print(f"📁 Found {len(volume_links)} volumes in {base_url}: {volume_links}")

    all_json_links = []

    for volume in volume_links:
        volume_url = urllib.parse.urljoin(base_url, volume)
        cases_url = urllib.parse.urljoin(volume_url, "cases/")

        print(f"🔎 Checking: {cases_url}")

        try:
            response = requests.get(cases_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            json_links = [
                urllib.parse.urljoin(cases_url, a['href'])
                for a in soup.find_all('a', href=True)
                if a['href'].endswith('.json')
            ]

            print(f"📄 Found {len(json_links)} cases in {cases_url}")
            all_json_links.extend(json_links)

        except Exception as e:
            print(f"⚠️ Skipping {cases_url} due to error: {e}")

    return all_json_links


def process_all_cases(base_url, output_file):
    """
    Fetches all case details from a given base URL and writes them incrementally to the output file.
    """
    json_links = get_all_json_links(base_url)
    print(f"🔍 Found {len(json_links)} case JSON files in {base_url}")

    all_case_details = []

    for i, link in enumerate(json_links):
        try:
            print(f"📂 Fetching: {link}")
            case_details = extract_case_details(link)
            print(f"✅ Processed {i+1}/{len(json_links)}: {case_details}")

            all_case_details.append(case_details)


            with open(output_file, "a", encoding="utf-8") as f:
                json.dump(case_details, f, indent=4)
                f.write(",\n")  # Add a newline for readability

            time.sleep(random.uniform(1, 3))  # Prevent rate limiting

        except Exception as e:
            print(f"⚠️ Error processing {link}: {e}")

    return all_case_details


# Base URLs containing JSON case files
BASE_URLS = [
    "https://static.case.law/f/",
    "https://static.case.law/f2d/",
    "https://static.case.law/f3d/",
    "https://static.case.law/f-appx/"
]

output_file = "case_details.json"


with open(output_file, "w", encoding="utf-8") as f:
    f.write("[\n")  # Open a JSON array

all_cases = []
for base_url in BASE_URLS:
    print(f"Processing cases from {base_url}...")
    cases = process_all_cases(base_url, output_file)
    all_cases.extend(cases)


with open(output_file, "a", encoding="utf-8") as f:
    f.write("\n]")  # Close the JSON array properly

print("✅ Case details have been saved to case_details.json")

# Display extracted cases for verification
print(json.dumps(all_cases[:5], indent=4))  # Print first 5 cases for confirmation


with open("case_details.json", "r", encoding="utf-8") as f:
    data = json.load(f)

df = pd.DataFrame(data)
df.to_csv("case_details.csv", index=False)

print("✅ Saved as case_details.csv!")

