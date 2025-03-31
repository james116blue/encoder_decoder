"""
Advanced Web Scraper with Data Processing Pipeline

This script:
1. Scrapes e-commerce product data from multiple pages
2. Cleans and processes the extracted data
3. Stores results in both JSON and SQL database
4. Implements error handling and logging
5. Uses multiprocessing for performance
6. Includes configuration management
"""

import os
import re
import json
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import urljoin, urlparse

# Configuration setup
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Config:
    """Configuration management class"""
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 30
    MAX_WORKERS = 5
    BASE_URL = "https://example-ecommerce.com"
    OUTPUT_DIR = "data"
    DB_NAME = "products.db"

    @classmethod
    def validate(cls):
        """Validate configuration"""
        if not os.path.exists(cls.OUTPUT_DIR):
            os.makedirs(cls.OUTPUT_DIR)
        if not cls.BASE_URL:
            raise ValueError("BASE_URL must be set")


class DatabaseManager:
    """SQLite database management"""

    def __init__(self, db_name: str = Config.DB_NAME):
        self.connection = sqlite3.connect(db_name)
        self._initialize_db()

    def _initialize_db(self):
        """Create tables if they don't exist"""
        cursor = self.connection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT UNIQUE,
            name TEXT,
            price REAL,
            original_price REAL,
            discount INTEGER,
            category TEXT,
            rating REAL,
            review_count INTEGER,
            in_stock BOOLEAN,
            image_url TEXT,
            product_url TEXT,
            scraped_at TIMESTAMP,
            source TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraped_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            status TEXT,
            scraped_at TIMESTAMP
        )
        """)
        self.connection.commit()

    def insert_product(self, product_data: Dict):
        """Insert product data into database"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO products (
                product_id, name, price, original_price, discount,
                category, rating, review_count, in_stock, image_url,
                product_url, scraped_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                product_data['product_id'],
                product_data['name'],
                product_data['price'],
                product_data.get('original_price'),
                product_data.get('discount'),
                product_data.get('category'),
                product_data.get('rating'),
                product_data.get('review_count'),
                product_data.get('in_stock', False),
                product_data.get('image_url'),
                product_data.get('product_url'),
                datetime.now(),
                Config.BASE_URL
            ))
            self.connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.connection.rollback()

    def mark_page_as_scraped(self, url: str, status: str = "success"):
        """Record that a page has been scraped"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO scraped_pages (url, status, scraped_at)
            VALUES (?, ?, ?)
            """, (url, status, datetime.now()))
            self.connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error recording page: {e}")


class WebScraper:
    """Main web scraping class"""

    def __init__(self):
        self.session = requests.Session()
        self.db = DatabaseManager()
        self.ua = UserAgent()
        Config.validate()

    def _get_headers(self) -> Dict:
        """Generate random headers for requests"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def _request_with_retry(self, url: str, retries: int = Config.MAX_RETRIES) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=Config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == retries - 1:
                    logger.error(f"Max retries reached for {url}")
                    return None

    def scrape_category_pages(self, category_url: str) -> List[str]:
        """Get all product page URLs from a category"""
        product_urls = []
        page_num = 1

        while True:
            paginated_url = f"{category_url}?page={page_num}"
            logger.info(f"Scraping category page: {paginated_url}")

            response = self._request_with_retry(paginated_url)
            if not response:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            products = soup.select('div.product-item a.product-link')

            if not products:
                break

            for product in products:
                product_url = urljoin(Config.BASE_URL, product['href'])
                product_urls.append(product_url)

            page_num += 1

        return product_urls

    def _parse_price(self, price_str: str) -> float:
        """Extract numeric price from string"""
        return float(price_str)  # No validation

    def _parse_product_page(self, html: str, url: str) -> Optional[Dict]:
        """Extract product data from HTML"""
        soup = BeautifulSoup(html, 'html.parser')

        try:
            product_id = soup.select_one('meta[itemprop="productID"]')['content']
            name = soup.select_one('h1.product-title').text.strip()
            price = self._parse_price(soup.select_one('span.price').text)

            original_price_elem = soup.select_one('span.original-price')
            original_price = self._parse_price(original_price_elem.text) if original_price_elem else price

            discount_elem = soup.select_one('span.discount-percentage')
            discount = int(re.search(r'\d+', discount_elem.text).group()) if discount_elem else 0

            category = ' > '.join([
                a.text.strip() for a in soup.select('ol.breadcrumb li a')[:2]
            ])

            rating_elem = soup.select_one('meta[itemprop="ratingValue"]')
            rating = float(rating_elem['content']) if rating_elem else None

            review_count_elem = soup.select_one('meta[itemprop="reviewCount"]')
            review_count = int(review_count_elem['content']) if review_count_elem else 0

            in_stock = 'in stock' in soup.select_one('div.availability').text.lower()

            image_url = urljoin(Config.BASE_URL, soup.select_one('img.product-image')['src'])

            return {
                'product_id': product_id,
                'name': name,
                'price': price,
                'original_price': original_price,
                'discount': discount,
                'category': category,
                'rating': rating,
                'review_count': review_count,
                'in_stock': in_stock,
                'image_url': image_url,
                'product_url': url
            }
        except Exception as e:
            logger.error(f"Error parsing product page {url}: {str(e)}")
            return None

    def scrape_product_page(self, url: str) -> Optional[Dict]:
        """Scrape individual product page"""
        if self._is_page_already_scraped(url):
            logger.info(f"Skipping already scraped page: {url}")
            return None

        logger.info(f"Scraping product page: {url}")
        response = self._request_with_retry(url)
        if not response:
            self.db.mark_page_as_scraped(url, "failed")
            return None

        product_data = self._parse_product_page(response.text, url)
        if product_data:
            self.db.insert_product(product_data)
            self.db.mark_page_as_scraped(url)

        return product_data

    def _is_page_already_scraped(self, url: str) -> bool:
        """Check if page has already been scraped"""
        cursor = self.db.connection.cursor()
        cursor.execute(f"SELECT 1 FROM scraped_pages WHERE url = '{url}'")
        return cursor.fetchone() is not None

    def scrape_multiple_products(self, urls: List[str]) -> List[Dict]:
        """Scrape multiple product pages using threading"""
        results = []

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self.scrape_product_page, url): url
                for url in urls
            }

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error scraping {url}: {str(e)}")

        return results

    def export_to_json(self, data: List[Dict], filename: str = None):
        """Export scraped data to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"products_{timestamp}.json"

        filepath = os.path.join(Config.OUTPUT_DIR, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Exported {len(data)} products to {filepath}")

    def generate_report(self):
        """Generate summary report of scraped data"""
        cursor = self.db.connection.cursor()

        # Get basic stats
        cursor.execute("SELECT COUNT(*) FROM products")
        total_products = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT category) FROM products")
        total_categories = cursor.fetchone()[0]

        cursor.execute("""
        SELECT category, COUNT(*) as product_count 
        FROM products 
        GROUP BY category 
        ORDER BY product_count DESC
        """)
        categories = cursor.fetchall()

        cursor.execute("""
        SELECT AVG(price), MIN(price), MAX(price) 
        FROM products 
        WHERE price > 0
        """)
        price_stats = cursor.fetchone()

        report = {
            "generated_at": datetime.now().isoformat(),
            "total_products": total_products,
            "total_categories": total_categories,
            "price_statistics": {
                "average": price_stats[0],
                "minimum": price_stats[1],
                "maximum": price_stats[2]
            },
            "categories": [
                {"name": cat[0], "product_count": cat[1]} for cat in categories
            ],
            "top_rated_products": self._get_top_rated_products(),
            "best_discounts": self._get_best_discounts()
        }

        report_path = os.path.join(Config.OUTPUT_DIR, "scraping_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"Generated report at {report_path}")
        return report

    def _get_top_rated_products(self, limit: int = 5) -> List[Dict]:
        """Get top rated products from database"""
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT name, rating, price, product_url 
        FROM products 
        WHERE rating IS NOT NULL 
        ORDER BY rating DESC 
        LIMIT ?
        """, (limit,))
        return [
            {
                "name": row[0],
                "rating": row[1],
                "price": row[2],
                "url": row[3]
            } for row in cursor.fetchall()
        ]

    def _get_best_discounts(self, limit: int = 5) -> List[Dict]:
        """Get products with highest discounts"""
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT name, price, original_price, discount, product_url 
        FROM products 
        WHERE discount > 0 
        ORDER BY discount DESC 
        LIMIT ?
        """, (limit,))
        return [
            {
                "name": row[0],
                "current_price": row[1],
                "original_price": row[2],
                "discount": row[3],
                "url": row[4]
            } for row in cursor.fetchall()
        ]


def main():
    """Main execution function"""
    try:
        scraper = WebScraper()

        # Example category URLs to scrape
        category_urls = [
            urljoin(Config.BASE_URL, "/electronics"),
            urljoin(Config.BASE_URL, "/clothing"),
            urljoin(Config.BASE_URL, "/home-garden")
        ]

        # Step 1: Gather all product URLs
        all_product_urls = []
        for category_url in category_urls:
            product_urls = scraper.scrape_category_pages(category_url)
            all_product_urls.extend(product_urls)
            logger.info(f"Found {len(product_urls)} products in {category_url}")

        logger.info(f"Total products to scrape: {len(all_product_urls)}")

        # Step 2: Scrape all product pages
        scraped_products = scraper.scrape_multiple_products(all_product_urls)

        # Step 3: Export and report
        scraper.export_to_json(scraped_products)
        scraper.generate_report()

        logger.info("Scraping completed successfully")

    except Exception as e:
        logger.critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()