import json
from traceback import print_tb

from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from parsel import Selector
from pymongo import MongoClient
from evpn import ExpressVpnApi
import pydash as _
# counters = 0

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

def fetch(task):
    # global counters
    url = task.get("product_url")
    split_url_id = url.split("/")[-1]
    updated_url = f"https://www.lowes.com/wpd/{split_url_id}/productdetail/1046/Guest"

    attempt_flag = True
    for attempt in range(20):
        # if attempt_flag == False:
    # while attempt_flag:
        #   # retry up to 5 times

        ua = random.choice(USER_AGENTS)
        impersonate = random.choice(IMPERSONATIONS)
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': ua,
        }
        try:
            resp = requests.get(updated_url, headers=headers, impersonate=impersonate, timeout=5)
            if resp.status_code == 200:
                try:
                    my_json = resp.json()

                    details = my_json.get("productDetails", {})
                    item_data = details.get(split_url_id, {})
                    location = item_data.get("location", {})

                    price_data = location.get("price", {}).get("pricingDataList", [{}])[0]
                    base_price = price_data.get("basePrice", "")
                    final_price = price_data.get("finalPrice", "")
                    retail_price = price_data.get("retailPrice", "")

                    offer_list = []
                    offer_loop = location.get("promotion", {}).get("productLevelPromotions", {})
                    for offer_ls in offer_loop:
                        offer_msg = offer_ls.get("detailPageMessage", {}).get("shortDescription", {})
                        offer_list.append(offer_msg)
                    offer = offer_list if offer_list else ""

                    # Inventory: Pickup and Delivery
                    pickup = ""
                    delivery = ""

                    item_avail_list = location.get("itemInventory", {}).get("itemAvailList", [])
                    for point in item_avail_list:
                        msg = point.get("fullMtdMsg", "")
                        date = point.get("itmLdTm", "")
                        qty = point.get("totalQty", 0)

                        if msg == "Pickup":
                            pickup = f"date: {date}, quantity: {qty}"
                        elif msg == "Delivery":
                            delivery = f"date: {date}, quantity: {qty}"

                except:
                    print(resp.json())
                    base_price = ''
                    final_price = ''
                    retail_price = ''
                    offer = ''
                    pickup = ''
                    delivery = ''
                attempt_flag = False
                response_checker = _.get(my_json, f"productDetails.{split_url_id}.product.omniItemId", "N/A")
                # counters += 1
                print(f"Good response data available.....,{resp.text[:50]}" if response_checker else "Bad response ......")
                # print(counters)
                # Update MongoDB
                client = MongoClient(MONGO_URI)
                db = client[DB_NAME]
                collection = db[COLLECTION_NAME]

                update_data = {
                    "Status": "Done",
                    "base_price": base_price,
                    "final_price": final_price,
                    "retail_price": retail_price,
                    "offer": offer,
                    "pickup": pickup,
                    "delivery": delivery,
                    "last_fetched": time.strftime("%Y-%m-%d %H:%M:%S")
                }

                collection.update_one(
                    {"product_url": url},
                    {"$set": update_data}
                )
                client.close()
                return None
            else:
                # time.sleep(0.5)
                continue
        except Exception as e:
            err = str(e)
            print(f"[Exception]:{err}")
            print(url)
            print(updated_url)
            # time.sleep(1)
            continue


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
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(fetch, task) for task in tasks]
            for future in as_completed(futures):
                results.append(future.result())

        end_time = time.time()
        print(f"Batch completed in {end_time - start_time:.2f} seconds")
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
    run_scraping_from_mongo(100)
