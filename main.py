import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse
from collections import OrderedDict

# OrderedDict of Bible books and their number of chapters, in order
bible_books = OrderedDict([
    ('GEN', 50), ('EXO', 40), ('LEV', 27), ('NUM', 36), ('DEU', 34), 
    ('JOS', 24), ('JDG', 21), ('RUT', 4), ('1SA', 31), ('2SA', 24), 
    ('1KI', 22), ('2KI', 25), ('1CH', 29), ('2CH', 36), ('EZR', 10), 
    ('NEH', 13), ('EST', 10), ('JOB', 42), ('PSA', 150), ('PRO', 31), 
    ('ECC', 12), ('SNG', 8), ('ISA', 66), ('JER', 52), ('LAM', 5), 
    ('EZK', 48), ('DAN', 12), ('HOS', 14), ('JOL', 3), ('AMO', 9), 
    ('OBA', 1), ('JON', 4), ('MIC', 7), ('NAM', 3), ('HAB', 3), 
    ('ZEP', 3), ('HAG', 2), ('ZEC', 14), ('MAL', 4), ('MAT', 28), 
    ('MRK', 16), ('LUK', 24), ('JHN', 21), ('ACT', 28), ('ROM', 16), 
    ('1CO', 16), ('2CO', 13), ('GAL', 6), ('EPH', 6), ('PHP', 4), 
    ('COL', 4), ('1TH', 5), ('2TH', 3), ('1TI', 6), ('2TI', 4), 
    ('TIT', 3), ('PHM', 1), ('HEB', 13), ('JAS', 5), ('1PE', 5), 
    ('2PE', 3), ('1JN', 5), ('2JN', 1), ('3JN', 1), ('JUD', 1), ('REV', 22)
])

# Dictionary mapping Bible versions to their corresponding IDs on bible.com
bible_versions = {
'AMP': '1588',
'AMPC': '8',
'ASV': '12',
'BSB': '3034',
'CEB': '37',
'CEV': '392',
'CEVDCI': '303',
'CEVUK': '294',
'CJB': '1275',
'CPDV': '42',
'CSB': '1713',
'DARBY': '478',
'DRC1752': '55',
'EASY': '2079',
'ERV': '406',
'ESV': '59',
'FBV': '1932',
'FNVNT': '3633',
'GNBDC': '416',
'GNBDK': '431',
'GNBUK': '296',
'GNT': '68',
'GNTD': '69',
'GNV': '2163',
'GW': '70',
'GWC': '1047',
'HCSB': '72',
'ICB': '1359',
'JUB': '1077',
'KJV': '1',
'KJVAAE': '546',
'KJVAE': '547',
'LEB': '90',
'LSB': '3345',
'MEV': '1171',
'MP1650': '1365',
'MP1781': '3051',
'MSG': '97',
'NABRE': '463',
'NASB1995': '100',
'NASB2020': '2692',
'NCV': '105',
'NET': '107',
'NIRV': '110',
'NIV': '111',
'NIVUK': '113',
'NKJV': '114',
'NLT': '116',
'NMV': '2135',
'NRSV': '2016',
'NRSV-CI': '2015',
'NRSVUE': '3523',
'OYBCENGL': '3915',
'PEV': '2530',
'RAD': '2753',
'RSV': '2020',
'RSV-C': '2017',
'RSVCI': '3548',
'RV1885': '477',
'RV1895': '1922',
'TCENT': '3427',
'TEG': '3010',
'TLV': '314',
'TOJB2011': '130',
'TPT': '1849',
'TS2009': '316',
'WBMS': '2407',
'WEBBE': '1204',
'WEBUS': '206',
'WMB': '1209',
'WMBBE': '1207',
'YLT98': '821'
}

# Setup logging
logging.basicConfig(filename='bible_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_progress():
    try:
        if os.path.exists('progress.json'):
            with open('progress.json', 'r') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
                else:
                    logging.warning("progress.json is empty. Starting with empty progress.")
        else:
            logging.info("progress.json not found. Starting with empty progress.")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding progress.json: {e}. Starting with empty progress.")
    except Exception as e:
        logging.error(f"Unexpected error loading progress: {e}. Starting with empty progress.")
    return {}

def save_progress(progress):
    try:
        with open('progress.json', 'w') as f:
            json.dump(progress, f, indent=2)
        logging.info("Progress saved successfully.")
    except Exception as e:
        logging.error(f"Error saving progress: {e}")

def get_numbered_book_name(book):
    book_number = list(bible_books.keys()).index(book) + 1
    return f"{book_number:02d}_{book}"

def scrape_bible_chapter(book, chapter_number, version, progress):
    version_id = bible_versions.get(version)
    if not version_id:
        logging.error(f"Unknown Bible version: {version}")
        return False

    url = f"https://www.bible.com/bible/{version_id}/{book}.{chapter_number}.{version}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        verses = soup.select('span[data-usfm]')
        bible_data = [
            {
                "verse_number": verse['data-usfm'].split('.')[-1],
                "text": verse.text.strip()
            }
            for verse in verses
        ]
        # Create directory for the book and version if it doesn't exist
        numbered_book = get_numbered_book_name(book)
        book_dir = os.path.join('data', version, numbered_book)
        os.makedirs(book_dir, exist_ok=True)
        # Save file with new naming convention
        filename = f"{book}_{chapter_number:03d}_{version}.json"
        with open(os.path.join(book_dir, filename), 'w', encoding='utf-8') as f:
            json.dump(bible_data, f, ensure_ascii=False, indent=4)
        logging.info(f"{filename} has been scraped and saved in {book_dir}.")

        # Update progress
        if version not in progress:
            progress[version] = {}
        if book not in progress[version]:
            progress[version][book] = 0
        progress[version][book] += 1
        save_progress(progress)

        return True
    except requests.RequestException as e:
        logging.error(f"An error occurred while fetching {book} chapter {chapter_number} ({version}): {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred with {book} chapter {chapter_number} ({version}): {e}")
    return False

def scrape_book(book, chapters, version, progress):
    logging.info(f"Scraping {book} ({version})...")
    completed_chapters = progress.get(version, {}).get(book, 0)
    newly_downloaded = 0
    for chapter in range(completed_chapters + 1, chapters + 1):
        success = scrape_bible_chapter(book, chapter, version, progress)
        if success:
            newly_downloaded += 1
        if not success:
            logging.warning(f"Skipping remaining chapters of {book} ({version}) due to error.")
            break
        time.sleep(2)
    return newly_downloaded

def scrape_all_bible(versions, max_workers=5):
    progress = load_progress()
    total_downloaded = {version: 0 for version in versions}
    total_books = len(bible_books) * len(versions)

    with tqdm(total=total_books, desc="Overall Progress") as pbar:
        for version in versions:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_book = {executor.submit(scrape_book, book, chapters, version, progress): book 
                                  for book, chapters in bible_books.items()}

                for future in as_completed(future_to_book):
                    book = future_to_book[future]
                    try:
                        newly_downloaded = future.result()
                        total_downloaded[version] += newly_downloaded
                        pbar.update(1)
                        pbar.set_postfix({"Downloaded": sum(total_downloaded.values())}, refresh=True)
                    except Exception as exc:
                        logging.error(f'{book} generated an exception: {exc}')

    return total_downloaded

def display_book_order(version):
    """Display the current order of books in the data directory for a given version."""
    version_dir = os.path.join('data', version)
    if not os.path.exists(version_dir):
        print(f"No data found for version {version}")
        return

    books = sorted(os.listdir(version_dir))
    print(f"Current order of books for {version}:")
    for book_dir in books:
        print(book_dir)

def main():
    parser = argparse.ArgumentParser(description='Scrape Bible chapters from bible.com')
    parser.add_argument('--versions', nargs='+', help='Specific Bible versions to scrape (default: all versions)')
    parser.add_argument('--workers', type=int, default=5, help='Number of worker threads (default: 5)')
    parser.add_argument('--books', nargs='+', help='Specific books to scrape (e.g., GEN EXO LEV)')
    parser.add_argument('--display-order', action='store_true', help='Display the current order of books in data directory')
    args = parser.parse_args()

    # If no specific versions are provided, use all versions from bible_versions
    versions_to_scrape = args.versions if args.versions else list(bible_versions.keys())

    if args.display_order:
        for version in versions_to_scrape:
            display_book_order(version)
        return

    progress = load_progress()
    total_downloaded = {version: 0 for version in versions_to_scrape}

    if args.books:
        total_books = len(args.books) * len(versions_to_scrape)
        with tqdm(total=total_books, desc="Overall Progress") as pbar:
            for version in versions_to_scrape:
                for book in args.books:
                    if book in bible_books:
                        newly_downloaded = scrape_book(book, bible_books[book], version, progress)
                        total_downloaded[version] += newly_downloaded
                        pbar.update(1)
                        pbar.set_postfix({"Downloaded": sum(total_downloaded.values())}, refresh=True)
                    else:
                        logging.warning(f"Unknown book: {book}")
    else:
        total_downloaded = scrape_all_bible(versions_to_scrape, args.workers)

    # Display the order of books after scraping
    for version in versions_to_scrape:
        display_book_order(version)

    print("\nFinal Download Summary:")
    for version, count in total_downloaded.items():
        print(f"{version}: {count} chapters downloaded")

if __name__ == "__main__":
    main()