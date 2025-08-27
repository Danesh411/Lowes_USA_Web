import json
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from parsel import Selector
from pymongo import MongoClient
from evpn import ExpressVpnApi

def change_vpn():
    # while True:
    with ExpressVpnApi() as api:
        # locations = [loc for loc in api.locations if loc["country_code"] in ("US", "UK", "SG", "DE", "IN", "TR", "ID", "TH", "MY")]
        locations = [loc for loc in api.locations if loc["country_code"] in ("UK", "SG", "DE", "IN", "TR",  "MY")]
        loc = random.choice(locations)  # Choose a random location from all available
        api.connect(loc["id"])
        print(f"Connected to: {loc['name']}")
        # time.sleep(35)
        # api.disconnect()

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"  # Change to your URI
DB_NAME = "Lowes_US_Demo"
COLLECTION_NAME = "sample_input"


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36 Edg/101.0.1210.47",
    "Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0"
]
IMPERSONATIONS = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107",
    "chrome110", "chrome116", "chrome119", "chrome120",
    "chrome123", "chrome124", "chrome131", "chrome133a", "chrome136",
    "chrome99_android", "chrome131_android",
    "safari153", "safari155", "safari170", "safari180", "safari184",
    # "safari260",
    "safari172_ios", "safari180_ios", "safari184_ios",
    # "safari260_ios",
    "firefox133", "firefox135",
    "tor145"
]


def fetch(task):
    # product_id = task.get("product_id")
    url = task.get("product_url")

    attempt_flag = True
    while attempt_flag:
    # for attempt in range(20):  # retry up to 5 times
        ua = random.choice(USER_AGENTS)
        impersonate = random.choice(IMPERSONATIONS)
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "referer": url,
            "user-agent": ua,
        }
        try:
            resp = requests.get(url, headers=headers, impersonate=impersonate, timeout=30)
            if resp.status_code == 200:
                try:
                    my_selector = Selector(text=resp.text)
                    price_checked = my_selector.xpath('//script[contains(text(),"priceCurrency")]/text()').get()
                    price_json_load = json.loads(price_checked)
                    price_check = price_json_load[2].get("offers","").get("price","")
                except:
                    price_check = ''
                attempt_flag = False
                print("Good response ....." if price_check else "Bad response .......",url)
                # print("Good response .....")
                # Optionally: update MongoDB Status to 'fetched'
                return {
                    "success": True,
                    "product_url": url,
                    "Status_code": resp.status_code,
                    "length": len(resp.text),
                    "impersonate": impersonate,
                    "user_agent": ua[:40],
                    "error": None
                }
            else:
                # time.sleep(0.5)
                continue
        except Exception as e:
            err = str(e)
            print(f"[Exception]:{err}")
            # time.sleep(1)
            continue
    # After retries
    return {
        "success": False,
        "product_url": url,
        "Status_code": None,
        "length": 0,
        "impersonate": impersonate,
        "user_agent": ua[:40],
        "error": str(e)
    }


def run_scraping_from_mongo(batch_size):
    overall_start = time.time()

    while True:
        # Fetch next batch of unprocessed products

        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = client[DB_NAME][COLLECTION_NAME]

        tasks = list(collection.find(
            {"Status": {"$in": [None, "pending"]}},  # Only fetch unprocessed
            {"product_url": 1,}
        ).limit(batch_size))

        if not tasks:
            print("No more tasks to process. Exiting.")
            break

        print(f"Processing batch of {len(tasks)} tasks...")

        # Update Status to 'in_progress' to avoid reprocessing
        task_ids = [task["_id"] for task in tasks]
        collection.update_many(
            {"_id": {"$in": task_ids}},
            {"$set": {"Status": "in_progress"}}
        )

        # Run concurrent fetch
        start_time = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(fetch, task) for task in tasks]
            for future in as_completed(futures):
                results.append(future.result())

        end_time = time.time()

        print(f"Batch completed in {end_time - start_time:.2f} seconds")
        for r in results:
            # print(r)
            # Update MongoDB with result
            Status = "success" if r["success"] else "failed"
            collection.update_one(
                {"product_url": r["product_url"]},
                {"$set": {
                    "Status": Status,
                }}
            )

        # Rotate IP (if using change_vpn)
        try:
            print("Changing VPN...")
            change_vpn()
            time.sleep(5)
        except Exception as e:
            print(f"VPN change failed: {e}")
            time.sleep(10)  # fallback delay

    print(f"All tasks completed in {time.time() - overall_start:.2f} seconds")

# ---------------------------
# Main Entry
# ---------------------------
if __name__ == "__main__":
    run_scraping_from_mongo(400)