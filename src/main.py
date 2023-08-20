from urllib.parse import urljoin

import requests
import time
import re
import json
import os
import configparser
import subprocess
import uuid
import socket

from apify import Actor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

# To run this Actor locally, you need to have the Selenium Chromedriver installed.
# https://www.selenium.dev/documentation/webdriver/getting_started/install_drivers/
# When running on the Apify platform, it is already included in the Actor's Docker image.

# Parameters

LOOP_MAX = 50
SCROLL_INCREMENT = 600  # This value might need adjusting depending on the website

STORAGE_PATH = "storage"
PATHS = {
    'storage': STORAGE_PATH,
    'captures': os.path.join(STORAGE_PATH, "captures"),
    'mitmdump': os.path.join(STORAGE_PATH, "mitmdump"),
    'stdout_log_file' : '',
    'stderr_log_file' : '',
    'captured_file': '',
    'error_file': ''
}

async def main():
    async with Actor:
        
        unique_id = str(uuid.uuid4())
        Actor.log.info("Using unique id: "+str(unique_id))

        paths = update_paths(unique_id)

        # Read the Actor input
        actor_input = await Actor.get_input() or {}
        url_template = actor_input.get('url_template', 'https://www.foodpanda.com.kh/en/restaurants/new?lat={lat}&lng={lng}&expedition=delivery')
        location = actor_input.get('location')
        Actor.log.info("Using location: "+str(location))

        lat, lng = get_location(location)      

        # Start the MITM proxy
        proxy_port = find_open_port()
        Actor.log.info("Using proxy port: "+str(proxy_port))
        mitm_process = start_mitmproxy(unique_id, proxy_port)

        # Load website
        driver = get_driver(proxy_port)
        await process_website(driver, lat, lng, url_template) 
                
        driver.quit()
        stop_mitmproxy(mitm_process)
        await process_capture(unique_id)
        clean_files()

def ensure_directory_exists(directory: str):
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

def update_paths(unique_id: str):

    PATHS['stdout_log_file']    = os.path.join(PATHS['mitmdump'], f'mitmdump_stdout_{unique_id}.log')
    PATHS['stderr_log_file']    = os.path.join(PATHS['mitmdump'], f'mitmdump_stderr_{unique_id}.log')
    PATHS['captured_file']      = os.path.join(PATHS['captures'], f"captured_requests_{unique_id}.txt")
    PATHS['error_file']         = os.path.join(PATHS['captures'], f"errors_{unique_id}.txt")

    # List of directory paths to ensure exist
    directories_to_ensure = [
        PATHS['storage'],
        PATHS['captures'],
        PATHS['mitmdump'],
    ]

    # Ensure directories exist
    for directory in directories_to_ensure:
        ensure_directory_exists(directory)

    return PATHS

async def process_website(driver, lat, lng, url_template):     
    url = url_template.format(lat=lat, lng=lng)
    Actor.log.info("Using url: "+str(url))

    driver.get(url)
    time.sleep(3)

    check_captcha(driver)

    title = driver.title
    try:
        # wait to load the page
        element_present = EC.presence_of_element_located((By.ID, 'restaurant-listing-root'))
        WebDriverWait(driver, 10).until(element_present)
        time.sleep(2)
    except TimeoutException:
        Actor.log.error("The expected element did not appear in the specified time! Closing the driver...")
        return       

    scroll_to_bottom(driver)

    vendor_tiles = driver.find_elements(By.CSS_SELECTOR, '.vendor-tile-wrapper')

    # Get the count of items and log them
    item_count = len(vendor_tiles)
    Actor.log.info(f'Webscrper located {item_count} returants.')

    # Loop through and process each div
    for tile in vendor_tiles:
        vendor_data = extract_vendor_data(tile)
        if vendor_data is not None:
            await Actor.push_data(vendor_data)

def get_location(location):
    # Get location
    GOOGLE_MAPS_API_KEY = get_maps_api_key()
    if GOOGLE_MAPS_API_KEY:
        try:
            lat, lng = get_lat_lng(GOOGLE_MAPS_API_KEY, location)            
            if lat and lng:
                Actor.log.info(f"Latitude: {lat}, Longitude: {lng}")
        except Exception as e:
            Actor.log.error(str(e))
            raise Exception(str(e))
    # Exit if error
    if not lat or not lng or not GOOGLE_MAPS_API_KEY:
        msg = "Error fetching the location data! Exiting..."
        Actor.log.error(msg)
        raise Exception(msg)    
    return lat, lng
        
def scroll_to_bottom(driver):
    loop_count = 0
    loop_max = LOOP_MAX
    while True:
        current_position = driver.execute_script("return window.pageYOffset;")
        driver.execute_script(f"window.scrollTo(0, {current_position + SCROLL_INCREMENT});")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if current_position + SCROLL_INCREMENT >= new_height:
            break
        if loop_count >= loop_max:
            break
        loop_count = loop_count + 1

def check_captcha(driver):
    # Check fo catpcha
    captcha = driver.find_elements(By.CSS_SELECTOR, '.px-captcha-container')
    if captcha:
        msg = "Captcha detected! Exiting..."
        Actor.log.error(msg)
        # TODO: Handle Captcha
        raise Exception(msg)        

def get_driver(proxy_port = 8080):
    # Launch a new Selenium Chrome WebDriver
    Actor.log.info('Launching Chrome WebDriver...')
    chrome_options = ChromeOptions()
    #    if Actor.config.headless:
    #        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    PROXY = "localhost:" + str(proxy_port)
    chrome_options.add_argument(f"--proxy-server={PROXY}")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    driver = webdriver.Chrome(options=chrome_options)

    return driver

async def process_capture(unique_id):
    captured_file_path = PATHS['captured_file']
    
    # Ensure that the file is read using 'utf-8' encoding
    with open(captured_file_path, "r", encoding='utf-8') as file:
        lines = file.readlines()

    # Loop through the lines
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "Response Body:" in line:
            i += 1  # Move to the next line where the actual response body is
            if i < len(lines):  # Ensure we don't go out of bounds
                response_body = lines[i].strip()

                # Check if the response body is empty
                if not response_body:
                    Actor.log.warning(f"Empty Response Body found on line {i + 1}.")
                # Check if the response body is a valid JSON
                elif is_valid_json(response_body):
                    # Actor.log.info("Processing file JSON catpure...")                    
                    data = json.loads(response_body)
                    await process_vendors(data)                    
                else:
                    Actor.log.error(f"Invalid JSON found on line {i + 1}.")
        i += 1

async def process_vendors(data):
    if not isinstance(data, dict):
        Actor.log.error("Expected data to be a dictionary but received a %s", type(data))
        return

    dataset = await Actor.open_dataset(name='captured-vendors')

    try:
        # Get vendors from organic_listing
        organic_views = data.get('data', {}).get('rlp', {}).get('organic_listing', {}).get('views', [])
        if organic_views:
            if not isinstance(organic_views, list):
                Actor.log.warning("Expected organic views to be a list but received a %s", type(organic_views))
                return

            for view in organic_views:
                vendors = view.get('items', [])            
                if not isinstance(vendors, list):
                    Actor.log.warning("Expected items to be a list but received a %s", type(vendors))
                    continue

                for vendor in vendors:
                    await add_vendor_to_dataset(dataset, vendor)

    except Exception as e:
        Actor.log.error("Error while processing organic listing: %s", str(e))

    try:
        # Get vendors from swimlanes
        swimlanes = data.get('data', {}).get('rlp', {}).get('swimlanes', {}).get('data', {}).get('items', [])

        if swimlanes:
            if not isinstance(swimlanes, list):
                Actor.log.warning("Expected swimlanes to be a list but received a %s", type(swimlanes))
                print (swimlanes)
                return

            for swimlane in swimlanes:
                vendors = swimlane.get('vendors', [])
                if not isinstance(vendors, list):
                    print(vendors)
                    Actor.log.warning("Expected vendor items to be a list but received a %s", type(vendors))                
                    continue

                for vendor in vendors:
                    await add_vendor_to_dataset(dataset, vendor)
    except Exception as e:
        Actor.log.error("Error while processing swimlanes: %s", str(e))


async def add_vendor_to_dataset(dataset, vendor_data):
    # Check if vendor data is directly inside vendor key
    if "vendor" in vendor_data:
        vendor_data = vendor_data.get('vendor', {})

    # Now add the vendor data to the dataset
    if vendor_data:
        await dataset.push_data(vendor_data)
    else:
        Actor.log.warning("Received empty vendor data.")

def extract_vendor_data(tile):
    # Create an empty dictionary to store the vendor's data
    vendor = {}

    # Extract the vendor's name
    try:
        name_element = tile.find_element(By.CSS_SELECTOR, '.name.fn')
        vendor['name'] = name_element.text if name_element else None
    except NoSuchElementException:
        return None

    # Extract the vendor's URL
    try:
        link_element = tile.find_element(By.CSS_SELECTOR, 'a[data-testid*="vendor-tile"]')
        vendor['url'] = link_element.get_attribute('href') if link_element else None
    except NoSuchElementException:
        return None

    # Extract the vendor's image URL
    try:
        image_element = tile.find_element(By.CSS_SELECTOR, '.vendor-picture[data-testid*="vendor-picture-lazy-image-actual"]')
        if image_element:
            style = image_element.get_attribute('style')
            match = re.search(r'background-image:\s*url\("?(.*?)"?\)', style)
            vendor['image_url'] = match.group(1) if match else ''
        else:
            vendor['image_url'] = ''
    except NoSuchElementException:
        vendor['image_url'] = ''

    # Extract the vendor's star rating
    try:
        rating_element = tile.find_element(By.CSS_SELECTOR, '.rating--label-primary')
        vendor['star_rating'] = rating_element.text.split('/')[0] if rating_element else None
    except NoSuchElementException:
        vendor['star_rating'] = ''

    # Extract the vendor's rating count
    try:
        rating_count_element = tile.find_element(By.CSS_SELECTOR, '.rating--label-secondary')
        vendor['rating_count'] = rating_count_element.text.strip('()') if rating_count_element else None
    except NoSuchElementException:
        vendor['rating_count'] = ''

    # Extract the category of the restaurant
    try:
        category_element = tile.find_element(By.CSS_SELECTOR, '.vendor-characteristic')
        vendor['category'] = category_element.text if category_element else None
    except NoSuchElementException:
        vendor['category'] = None

    # Extract the vendor's delivery option
    try:
        delivery_element = tile.find_element(By.CSS_SELECTOR, '.extra-info.mov-df-extra-info')
        vendor['delivery_option'] = delivery_element.text if delivery_element else None
    except NoSuchElementException:
        vendor['delivery_option'] = ''

    # Extract the vendor's price rating (Budget Symbol)
    try:
        price_rating_elements = tile.find_elements(By.CSS_SELECTOR, '[data-testid*="budget-symbol"]')
        vendor['price_rating'] = ''.join([price.text for price in price_rating_elements])
    except NoSuchElementException:
        vendor['price_rating'] = ''

    return vendor

def find_open_port(start_port=8080):
    port = start_port
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            socket_result = s.connect_ex(('localhost', port))
            if socket_result == 0:  # port is already in use
                port += 1
            else:
                return port

def start_mitmproxy(unique_id, port = 8080):
    # Ensure data folder exists or create it
    data_folder = PATHS['storage']
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)   

    # Set path to the mitmdump script inside the data folder
    dump_script_path = os.path.join("src", "save_requests.py")

    # Define paths for stdout and stderr logs
    stdout_log_path = PATHS['stdout_log_file']
    stderr_log_path = PATHS['stderr_log_file']

    # Start mitmdump with the specified port
    with open(stdout_log_path, 'w') as stdout_file, open(stderr_log_path, 'w') as stderr_file:
        cmd = f'mitmdump --quiet -p {port} -s {dump_script_path} {unique_id} > {stdout_log_path} 2> {stderr_log_path}'
        process = subprocess.Popen(cmd, shell=True)

    time.sleep(3)

    # Check the stderr log for errors
    with open(stderr_log_path, 'r') as stderr_file:
        error_output = stderr_file.read()
        if "Address already in use" in error_output:
            raise Exception("Another process is already using the required port. Make sure mitmproxy isn't already running.")
        elif "Error" in error_output:
            raise Exception(f"Error starting mitmproxy: {error_output}")

    return process

def stop_mitmproxy(process):
    try:
        # Send a SIGTERM signal to the process
        process.terminate()
        # Wait for up to 5 seconds for the process to terminate
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        # If the process doesn't terminate within the timeout, forcibly kill it
        process.kill()
    Actor.log.info("Mitmproxy stopped.")

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except ValueError:
        return False
    
def clean_files():
    Actor.log.info("Cleaning up files...")
    delete_files(PATHS['stdout_log_file'], PATHS['stderr_log_file'], PATHS['captured_file'], PATHS['error_file'])

def delete_files(*file_paths):
    """Delete files specified by their paths."""
    for path in file_paths:
        try:
            os.remove(path)
            #Actor.log.info(f"Successfully deleted {path}")
        except FileNotFoundError:
            Actor.log.warning(f"{path} not found.")
        except Exception as e:
            Actor.log.error(f"Error deleting {path}: {e}")

def get_lat_lng(api_key, location):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    endpoint = f"{base_url}?address={location}&key={api_key}"

    # Send the request to the API
    response = requests.get(endpoint)
    if response.status_code not in range(200, 299):
        error_msg = "Unknown error"
        try:
            # Attempt to get an error message from the API response
            error_data = response.json()
            error_msg = error_data.get("error_message", "Unknown error")
        except:
            pass
        raise Exception(f"Error fetching data from Google Maps API: {error_msg}")

    try:
        results = response.json()['results'][0]
        lat = results['geometry']['location']['lat']
        lng = results['geometry']['location']['lng']
        return lat, lng
    except:
        Actor.log.error("Unexpected response format from Google Maps API" + response) 
        raise Exception("Unexpected response format from Google Maps API")
    
def get_maps_api_key():
    # Try to get the API key from the environment variable first
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')

    if api_key:
        Actor.log.info("API key fetched from environment variable.")
        return api_key

    # If not found, then fetch it from the config.ini file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Check if the config file is found and read
    if not config.sections():
        Actor.log.error("Error: config.ini not found or empty!")
        return None

    # Check if the "GoogleMaps" section exists in the config
    if 'GoogleMaps' not in config.sections():
        Actor.log.error("Error: 'GoogleMaps' section not found in config.ini!")
        return None

    try:
        api_key = config['GoogleMaps']['GOOGLE_MAPS_API_KEY']
        Actor.log.info("API key fetched from config.ini.")
        return api_key
    except KeyError:
        Actor.log.error("Error: Couldn't find the 'GOOGLE_MAPS_API_KEY' in 'GoogleMaps' section of config.ini!")
        return None