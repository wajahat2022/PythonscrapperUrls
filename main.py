import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from csv import reader
import re
import logging
import os
from io import BytesIO
from PIL import Image
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

urls = []
all_data = []

with open('bookslinks.csv', 'r') as f:
    csv_reader = reader(f)
    for row in csv_reader:
        urls.append(row[0])

def download_image(url, filename):
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content))
        #Check if image is actually an image
        if image.format is None:
            logging.error(f"Invalid image format from {url}")
            return
        image.save(filename)
        logging.info(f"Image saved: {filename}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading image from {url}: {e}")
    except PIL.UnidentifiedImageError as e:
        logging.error(f"Error processing image from {url}: {e}")
    except Exception as e:
        logging.exception(f"Error processing image from {url}: {e}")

def transform(url):
    try:
        r = requests.get(str(url), timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        data = {'url': url}

        logo_img = None
        for selector in ['img.logo', 'img[alt="logo"]', 'header img:first-child', 'img[src*="logo"]']:
            logo_img = soup.select_one(selector)
            if logo_img:
                break

        if logo_img and logo_img['src']:
            img_url = logo_img['src']
            if not img_url.startswith('http'):
                img_url = requests.compat.urljoin(url, img_url)
            filename = os.path.join('img', os.path.basename(img_url))
            os.makedirs('img', exist_ok=True)
            download_image(img_url, filename)
            data['logo_path'] = filename
            data['logo_url'] = img_url

        # ... (other data extraction with improved selectors) ...

        email_matches = []
        for text in soup.find_all(string=True):
            email_match = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
            email_matches.extend(email_match)
        data['Company Email'] = email_matches if email_matches else None

        all_data.append(data)
        logging.info(f"Successfully processed {url}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error processing {url}: {e}")
    except Exception as e:
        logging.exception(f"Error processing {url}: {e}")
    return

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(transform, urls)

if all_data:
    df = pd.DataFrame(all_data)
    df.to_csv('collectedMails.csv', index=False)
    print(f"Data saved to collectedMails.csv")
else:
    print("No data found.")

print('Complete.')
