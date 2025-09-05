import psycopg2
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

def parse_count(text):
    text = text.strip().lower()
    # Extract the number
    num_match = re.search(r"([\d\.]+)", text)
    if not num_match:
        return 0
    num = float(num_match.group(1))
    # Check if there is a 'k' or 'm' in the text
    if 'k' in text:
        return int(num * 1_000)
    elif 'm' in text:
        return int(num * 1_000_000)
    else:
        return int(num)

# === Connect to PostgreSQL ===
conn = psycopg2.connect("postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs")
cursor = conn.cursor()

# Fetch rows from tbl_tk_sources where source_check is true
cursor.execute("""
    SELECT id, source_name, source_id FROM public.tbl_tk_sources
    WHERE source_check = true
    ORDER BY id ASC
""")
rows = cursor.fetchall()

# === Setup ChromeDriver ===
chrome_options = Options()
#chrome_options.add_argument("--start-maximized")
#chrome_options.add_argument("--headless")  # Optional, if you don't need the browser window
chromedriver_path = "C:\\Users\\dell\\chromedriver.exe"
driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)

# === Process Each Row ===
for row in rows:
    id, source_name, source_id = row
    if not source_name:
        continue

    # Construct TikTok URL
    url = f"https://www.tiktok.com/@{source_name}"
    print(f"🌐 Opening: {url}")
    driver.get(url)

    try:
        # Wait for and close the CAPTCHA button if present
        captcha_close_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "captcha_close_button"))
        )
        captcha_close_button.click()
        print("✅ CAPTCHA closed.")
    except Exception as e:
        print(f"⚠️ No CAPTCHA found or failed to close: {e}")
    
    try:
        # Wait until the following, followers, and likes counts are loaded
        following_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "strong[data-e2e='following-count']"))
        )
        followers_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "strong[data-e2e='followers-count']"))
        )
        likes_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "strong[data-e2e='likes-count']"))
        )
    
        # Extract text values
        following_count = following_elem.text
        followers_count = followers_elem.text
        likes_count = likes_elem.text
    
        # Parse the counts
        following_count = parse_count(following_count)
        followers_count = parse_count(followers_count)
        likes_count = parse_count(likes_count)

        # Print the extracted values
        print(f"Following: {following_count}")
        print(f"Followers: {followers_count}")
        print(f"Likes: {likes_count}")

        # === Update the database with the scraped counts using `id` ===
        try:
            cursor.execute("""
                UPDATE public.tbl_tk_sources
                SET following = %s, followers = %s, likes = %s
                WHERE id = %s
            """, (following_count, followers_count, likes_count, id))
            conn.commit()
            print(f"✅ Updated counts in DB for id {id}")
        except Exception as update_err:
            print(f"💥 DB Update Error for id {id}: {update_err}")
            conn.rollback()

        # === Insert data into tbl_tk_followers ===
        try:
            current_time = datetime.now()
            cursor.execute("""
                INSERT INTO public.tbl_tk_followers (tk_id, source_name, following, followers, likes, time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (id, source_name, following_count, followers_count, likes_count, current_time))
            conn.commit()
            print(f"✅ Inserted into tbl_tk_followers for id {id}")
        except Exception as insert_err:
            print(f"💥 Insert Error into tbl_tk_followers for id {id}: {insert_err}")
            conn.rollback()

    except Exception as e:
        print(f"Error extracting data: {e}")

    # Wait for 2 seconds before opening the next URL
    time.sleep(2)

# === Cleanup ===
driver.quit()
cursor.close()
conn.close()