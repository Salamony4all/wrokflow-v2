"""
Architonic-specific scraper for product collection links
Handles Architonic.com product pages and collections
"""

import logging
import time
import json
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Enable DEBUG logging to see product extraction details

try:
    from utils.selenium_scraper import SeleniumScraper, SELENIUM_AVAILABLE
    from selenium.webdriver.common.by import By
except ImportError:
    SELENIUM_AVAILABLE = False
    SeleniumScraper = None
    By = None
    logger.warning("Selenium scraper not available")


class ArchitonicScraper:
    """Scraper specifically for Architonic.com product collections"""
    
    def __init__(self, use_selenium: bool = True):
        """
        Initialize Architonic scraper
        
        Args:
            use_selenium: Use Selenium for JavaScript-heavy pages
        """
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.base_url = "https://www.architonic.com"
        self.rate_limit_delay = 1  # Reduced delay for faster scraping
    
    def is_architonic_url(self, url: str) -> bool:
        """Check if URL is an Architonic link"""
        parsed = urlparse(url)
        return 'architonic.com' in parsed.netloc.lower()
    
    def is_collections_page(self, url: str) -> bool:
        """Check if URL is a brand collections page"""
        parsed = urlparse(url)
        path = parsed.path.lower()
        # Check for patterns like /en/b/brandname/collections/ or /en/b/brandname/products/
        return '/collections/' in path or ('/b/' in path and '/products' in path)
    
    def scrape_collection(self, url: str, brand_name: str) -> Dict:
        """
        Scrape an Architonic product collection or brand collections page
        
        Args:
            url: Architonic collection, brand products page, or collections page URL
            brand_name: Brand name for data organization
            
        Returns:
            Dictionary with products organized by collections (matching the example structure)
        """
        if not self.is_architonic_url(url):
            return {'error': 'Not an Architonic URL'}
        
        try:
            logger.info(f"Scraping Architonic: {url}")
            
            # Check if this is a collections page (brand page with multiple collections)
            if self.is_collections_page(url) or '/products' in url.lower():
                logger.info(f"Detected collections/products page, scraping all collections")
                return self._scrape_collections_page(url, brand_name)
            
            # Otherwise, scrape as a single collection
            # Use Selenium for Architonic (heavily JavaScript-based)
            if self.use_selenium:
                return self._scrape_with_selenium(url, brand_name)
            else:
                return self._scrape_with_requests(url, brand_name)
                
        except Exception as e:
            logger.error(f"Error scraping Architonic collection {url}: {e}")
            return {'error': str(e)}
    
    def _scrape_collections_page(self, url: str, brand_name: str) -> Dict:
        """
        Scrape a brand's collections page (like /en/b/narbutas/products/)
        Returns data in the same structure as the example JSON file
        """
        scraper = None  # Initialize at function scope
        try:
            from datetime import datetime
            import requests
            
            logger.info(f"Loading collections page: {url}")
            
            # Use Selenium only if available and enabled
            if self.use_selenium and SeleniumScraper:
                try:
                    scraper = SeleniumScraper(headless=True, timeout=120)
                    soup = scraper.get_page(url, wait_for_selector='body', wait_time=20)
                    if not soup:
                        return {'error': 'Failed to load page'}
                    time.sleep(5)
                    # Find all collection links on the page
                    collections = self._find_collection_links(scraper, url, brand_name)
                except Exception as e:
                    logger.warning(f"Selenium failed, falling back to requests: {e}")
                    # Fall back to requests
                    if scraper:
                        try:
                            scraper.close()
                        except:
                            pass
                    scraper = None
                    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    collections = self._find_collection_links_requests(soup, url, brand_name)
            else:
                # Use requests-based scraping
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                collections = self._find_collection_links_requests(soup, url, brand_name)
            
            if not collections:
                logger.warning("No collections found, trying to scrape as single product page")
                if self.use_selenium and SeleniumScraper:
                    return self._scrape_with_selenium(url, brand_name)
                else:
                    return self._scrape_with_requests(url, brand_name)
            
            logger.info(f"Found {len(collections)} collections, scraping each...")
            
            # Scrape each collection
            all_collections_data = {}
            all_products_list = []
            
            for collection_name, collection_url in collections.items():
                logger.info(f"Scraping collection: {collection_name}")
                
                # Parse category/subcategory from collection name
                # Remove product count if present (e.g. "Name\n10 Products")
                # Skip if collection_name is None
                if not collection_name:
                    continue
                    
                clean_name = collection_name.split('\n')[0].strip()
                category = clean_name
                subcategory = None
                
                # Check for hierarchy in name
                if ' > ' in clean_name:
                    parts = clean_name.split(' > ')
                    category = parts[0]
                    subcategory = parts[-1]
                
                # Scrape collection - use Selenium if available, otherwise use requests
                if scraper:
                    collection_products = self._scrape_single_collection(scraper, collection_url, collection_name, brand_name)
                else:
                    # Use requests-based scraping for single collection
                    collection_products = self._scrape_single_collection_requests(collection_url, collection_name, brand_name)
                
                if collection_products:
                    # Enrich products with category info
                    for prod in collection_products:
                        prod['category'] = category
                        prod['subcategory'] = subcategory
                    
                    all_collections_data[collection_name] = {
                        'url': collection_url,
                        'category': category,
                        'subcategory': subcategory,
                        'product_count': len(collection_products),
                        'products': collection_products
                    }
                    all_products_list.extend(collection_products)
                    time.sleep(1)  # Reduced rate limiting delay
            
            # Close scraper if it was used
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
            
            # Build the final structure matching the example
            result = {
                'brand': brand_name,
                'source': 'Architonic Collections',
                'scraped_at': datetime.now().isoformat(),
                'total_products': len(all_products_list),
                'total_collections': len(all_collections_data),
                'collections': all_collections_data,
                'all_products': all_products_list
            }
            
            # Also create category_tree structure for compatibility with app.py
            result['category_tree'] = self._convert_collections_to_category_tree(all_collections_data)
            
            logger.info(f"Scraped {len(all_products_list)} products from {len(all_collections_data)} collections")
            return result
            
        except Exception as e:
            logger.error(f"Error scraping collections page: {e}")
            # Close scraper if it was used
            if 'scraper' in locals() and scraper:
                try:
                    scraper.close()
                except:
                    pass
            return {'error': str(e)}
        finally:
            # Close scraper if it was used
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
    
    def _find_collection_links(self, scraper: SeleniumScraper, url: str, brand_name: str) -> Dict[str, str]:
        """Find all collection links on a brand products/collections page"""
        collections = {}
        
        try:
            # Scroll to load all content
            logger.info("Scrolling to load all collections...")
            scraper.scroll_to_bottom(pause_time=2.0)
            time.sleep(3)
            
            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
            
            # Look for collection links - Architonic typically uses patterns like:
            # /en/b/brandname/brandid/collection/collection-name/collectionid
            # or /collection/collection-name/id
            # Note: Pattern needs to be flexible for different Architonic URL structures
            collection_patterns = [
                re.compile(r'/collection/[^/]+/\d+', re.I),  # /collection/name/id
                re.compile(r'/b/[^/]+/\d+/collection/[^/]+/\d+', re.I),  # /b/brand/id/collection/name/id
            ]
            
            logger.info(f"Looking for collection links...")
            
            # Find all links that match any collection pattern
            links = []
            for pattern in collection_patterns:
                found = soup.find_all('a', href=pattern)
                if found:
                    logger.info(f"  Pattern '{pattern.pattern}' matched {len(found)} links")
                    links.extend(found)
            
            logger.info(f"Found {len(links)} total links matching collection patterns")
            
            for idx, link in enumerate(links):
                href = link.get('href', '')
                if href:
                    collection_url = urljoin(self.base_url, href)
                    # Get collection name from link text or parent
                    collection_name = link.get_text(strip=True)
                    
                    # If no text, try to get from parent or data attributes
                    if not collection_name or len(collection_name) < 2:
                        parent = link.find_parent(['div', 'article', 'li'])
                        if parent:
                            collection_name = parent.get_text(strip=True)
                    
                    # Try to get product count if available
                    product_count_text = ''
                    parent_elem = link.find_parent(['div', 'article', 'li', 'a'])
                    if parent_elem:
                        count_match = re.search(r'(\d+)\s*products?', parent_elem.get_text(), re.I)
                        if count_match:
                            product_count_text = f"\n{count_match.group(1)} Products"
                    
                    if collection_name and len(collection_name) > 2:
                        full_name = collection_name + product_count_text
                        if full_name not in collections:
                            collections[full_name] = collection_url
                            logger.info(f"  Collection {idx+1}: {full_name[:50]}... → {collection_url}")
            
            # Also try to find collection links in filters/sidebar (Categories filter)
            filter_sections = soup.find_all(['div', 'nav', 'section', 'aside'], class_=re.compile(r'filter|sidebar|collection|category|facet', re.I))
            for section in filter_sections:
                # Look for "Categories" header
                section_text = section.get_text().lower()
                if 'categor' in section_text or 'collection' in section_text:
                    section_links = section.find_all('a', href=collection_link_pattern)
                    for link in section_links:
                        href = link.get('href', '')
                        if href:
                            collection_url = urljoin(self.base_url, href)
                            collection_name = link.get_text(strip=True)
                            
                            # Try to get product count from nearby text
                            parent = link.find_parent(['div', 'li', 'article'])
                            if parent:
                                count_text = parent.get_text()
                                count_match = re.search(r'(\d+)\s*products?', count_text, re.I)
                                if count_match:
                                    collection_name = collection_name + f"\n{count_match.group(1)} Products"
                            
                            if collection_name and len(collection_name.split('\n')[0].strip()) > 2:
                                if collection_name not in collections:
                                    collections[collection_name] = collection_url
            
            # If still no collections found, try looking for product grid items that link to collections
            if not collections:
                logger.info("No collections found with standard patterns, trying alternative methods...")
                product_items = soup.find_all(['article', 'div'], class_=re.compile(r'product|item|card|tile|collection', re.I))
                logger.info(f"Found {len(product_items)} potential collection/product items")
                
                for item in product_items[:100]:  # Increased limit
                    # Try to find any link within the item
                    link = item.find('a', href=True)
                    if link:
                        href = link.get('href', '')
                        if href and ('/collection/' in href.lower() or '/product' in href.lower()):
                            collection_url = urljoin(self.base_url, href)
                            
                            # Try to extract collection name and product count
                            collection_name = link.get_text(strip=True)
                            if not collection_name or len(collection_name) < 2:
                                collection_name = item.get_text(strip=True)[:100]
                            
                            # Extract product count if mentioned
                            count_match = re.search(r'(\d+)\s*products?', collection_name, re.I)
                            if count_match:
                                # Clean the name and add formatted count
                                clean_name = re.sub(r'\d+\s*products?', '', collection_name, flags=re.I).strip()
                                product_count_text = f"\n{count_match.group(1)} Products"
                                full_name = clean_name + product_count_text
                            else:
                                full_name = collection_name
                            
                            if full_name and len(full_name.strip()) > 2:
                                if full_name not in collections and collection_url not in collections.values():
                                    collections[full_name] = collection_url
                                    logger.info(f"  Alt method found: {full_name[:50]}... → {collection_url}")
            
            logger.info(f"Total collections found: {len(collections)}")
            return collections
            
        except Exception as e:
            logger.error(f"Error finding collection links: {e}")
            return {}
    
    def _find_collection_links_requests(self, soup: BeautifulSoup, url: str, brand_name: str) -> Dict[str, str]:
        """Find all collection links on a brand products/collections page using requests/BeautifulSoup"""
        collections = {}
        
        try:
            # Look for collection links - Architonic typically uses patterns like:
            # /en/b/brandname/brandid/collection/collection-name/collectionid
            # or /collection/collection-name/id
            collection_patterns = [
                re.compile(r'/collection/[^/]+/\d+', re.I),  # /collection/name/id
                re.compile(r'/b/[^/]+/\d+/collection/[^/]+/\d+', re.I),  # /b/brand/id/collection/name/id
            ]
            
            logger.info(f"Looking for collection links using requests...")
            
            # Find all links that match any collection pattern
            links = []
            for pattern in collection_patterns:
                found = soup.find_all('a', href=pattern)
                if found:
                    logger.info(f"  Pattern '{pattern.pattern}' matched {len(found)} links")
                    links.extend(found)
            
            logger.info(f"Found {len(links)} total links matching collection patterns")
            
            for idx, link in enumerate(links):
                href = link.get('href', '')
                if href:
                    collection_url = urljoin(self.base_url, href)
                    # Get collection name from link text or parent
                    collection_name = link.get_text(strip=True)
                    
                    # If no text, try to get from parent or data attributes
                    if not collection_name or len(collection_name) < 2:
                        parent = link.find_parent(['div', 'article', 'li'])
                        if parent:
                            collection_name = parent.get_text(strip=True)
                    
                    # Try to get product count if available
                    product_count_text = ''
                    parent_elem = link.find_parent(['div', 'article', 'li', 'a'])
                    if parent_elem:
                        count_match = re.search(r'(\d+)\s*products?', parent_elem.get_text(), re.I)
                        if count_match:
                            product_count_text = f"\n{count_match.group(1)} Products"
                    
                    if collection_name and len(collection_name) > 2:
                        full_name = collection_name + product_count_text
                        if full_name not in collections:
                            collections[full_name] = collection_url
                            logger.info(f"  Collection {idx+1}: {full_name[:50]}... → {collection_url}")
            
            logger.info(f"Total collections found: {len(collections)}")
            return collections
            
        except Exception as e:
            logger.error(f"Error finding collection links with requests: {e}")
            return {}
    
    def _scrape_single_collection_requests(self, collection_url: str, collection_name: str, brand_name: str) -> List[Dict]:
        """Scrape a single collection page using requests and return list of products"""
        formatted_products = []
        seen_product_ids = set()
        
        try:
            import requests
            logger.info(f"Loading collection with requests: {collection_url}")
            response = requests.get(collection_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            if not soup:
                return formatted_products
            
            # Extract products from page
            page_products = self._extract_all_products_from_page(soup, collection_url, brand_name)
            
            logger.info(f"Extracted {len(page_products)} products from collection")
            
            # Add products
            for product in page_products:
                product_id = product.get('product_id')
                product_url = product.get('source_url', '')
                product_name = product.get('model', 'Unknown')
                
                if not product_id and product_url:
                    id_match = re.search(r'/(\d+)/?$', product_url)
                    if id_match:
                        product_id = id_match.group(1)
                
                unique_id = product_id or product_url
                
                if unique_id and unique_id not in seen_product_ids:
                    seen_product_ids.add(unique_id)
                    
                    formatted_product = {
                        'name': product_name,
                        'url': product_url,
                        'product_id': product_id or '',
                        'collection': collection_name,
                        'model': product.get('model', product_name),
                        'description': product.get('description', ''),
                        'image_url': product.get('image_url'),
                        'source_url': product_url,
                        'brand': brand_name,
                        'price': product.get('price'),
                        'price_range': product.get('price_range', 'Contact for price'),
                        'features': product.get('features', []),
                        'specifications': product.get('specifications', {})
                    }
                    formatted_products.append(formatted_product)
            
            return formatted_products
            
        except Exception as e:
            logger.error(f"Error scraping collection {collection_url} with requests: {e}")
            return formatted_products
    
    def _scrape_single_collection(self, scraper: SeleniumScraper, collection_url: str, 
                                  collection_name: str, brand_name: str) -> List[Dict]:
        """Scrape a single collection page and return list of products"""
        formatted_products = []
        seen_product_ids = set()
        
        try:
            logger.info(f"Loading collection: {collection_url}")
            soup = scraper.get_page(collection_url, wait_for_selector='body', wait_time=15)
            
            if not soup:
                return formatted_products
            
            # Handle Pagination / Infinite Scroll
            page_count = 0
            max_pages = 10  # Limit pages to avoid infinite loops
            
            while page_count < max_pages:
                page_count += 1
                scraper.scroll_to_bottom(pause_time=2.0)
                time.sleep(2)
                
                # Extract products from current view
                current_soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                page_products = self._extract_all_products_from_page(current_soup, collection_url, brand_name)
                
                logger.info(f"Page {page_count}: Extracted {len(page_products)} products")
                
                # Add new products
                for product in page_products:
                    # Use product ID or URL as unique identifier
                    product_id = product.get('product_id')
                    product_url = product.get('source_url', '')
                    product_name = product.get('model', 'Unknown')
                    
                    if not product_id and product_url:
                        id_match = re.search(r'/(\d+)/?$', product_url)
                        if id_match:
                            product_id = id_match.group(1)
                    
                    unique_id = product_id or product_url
                    
                    if unique_id and unique_id not in seen_product_ids:
                        seen_product_ids.add(unique_id)
                        
                        # Format product to match desired output structure
                        formatted_product = {
                            'name': product_name,
                            'url': product_url,
                            'product_id': product_id or '',
                            'collection': collection_name,
                            'image_url': product.get('image_url'),
                            'category': product.get('category'),
                            'subcategory': product.get('subcategory'),
                            'description': product.get('description', '')  # Include description from listing
                        }
                        
                        formatted_products.append(formatted_product)
                
                # Try to find and click "Load More" or "Next" button
                try:
                    # Architonic specific load more selectors + generic ones
                    next_selectors = [
                        "a.load-more", 
                        "button.load-more",
                        "div.load-more",
                        "a[rel='next']", 
                        "button:contains('Load more')",
                        "span:contains('Load more')"
                    ]
                    
                    clicked_next = False
                    for selector in next_selectors:
                        try:
                            found = scraper.driver.execute_script(f"""
                                var el = document.querySelector("{selector}");
                                if (el && el.offsetParent !== null) {{
                                    el.click();
                                    return true;
                                }}
                                return false;
                            """)
                            if found:
                                logger.info(f"Clicked load more: {selector}")
                                time.sleep(3) # Wait for load
                                clicked_next = True
                                break
                        except:
                            continue
                    
                    if not clicked_next:
                        if page_count > 1 and len(page_products) == 0:
                             break
                        if page_count > 3 and not clicked_next: 
                             break
                        
                except Exception as e:
                    logger.warning(f"Pagination error: {e}")
                    break
            
            logger.info(f"Scraped {len(formatted_products)} products from {collection_name}")
            
            # Enrich products with detailed "About" descriptions from product pages
            # Sample 5 products per collection to keep it fast
            if formatted_products and len(formatted_products) > 0:
                sample_size = min(5, len(formatted_products))
                logger.info(f"Enriching {sample_size} products with 'About' descriptions...")
                self._enrich_products_with_descriptions(scraper, formatted_products[:sample_size])
            
            return formatted_products
            
        except Exception as e:
            logger.error(f"Error scraping collection {collection_url}: {e}")
            return formatted_products
    
    def _enrich_products_with_descriptions(self, scraper: SeleniumScraper, products: List[Dict]) -> None:
        """
        Visit product detail pages and extract 'About this product' descriptions
        
        Args:
            scraper: Active Selenium scraper instance
            products: List of product dictionaries to enrich (modified in-place)
        """
        for idx, product in enumerate(products):
            product_url = product.get('url')
            if not product_url:
                continue
            
            try:
                logger.debug(f"  Fetching description for: {product.get('name', 'Unknown')} ({idx+1}/{len(products)})")
                
                # Load product page
                soup = scraper.get_page(product_url, wait_for_selector='body', wait_time=8)
                
                if not soup:
                    continue
                
                # Extract "About this product" description
                description = self._extract_product_about_section(soup)
                
                if description:
                    product['description'] = description
                    logger.debug(f"    ✓ Added description ({len(description)} chars)")
                else:
                    logger.debug(f"    ✗ No description found")
                
                # Small delay to be respectful
                time.sleep(0.5)
                
            except Exception as e:
                logger.debug(f"    ✗ Error fetching description: {e}")
                continue
    
    def _extract_product_about_section(self, soup: BeautifulSoup) -> str:
        """
        Extract the 'About this product' description from an Architonic product page
        
        Args:
            soup: BeautifulSoup object of the product page
        
        Returns:
            Description text or empty string
        """
        description_parts = []
        
        # Strategy 1: Look for "About this product" section
        about_section = soup.find(['div', 'section'], string=re.compile(r'about\s+this\s+product', re.I))
        if not about_section:
            # Look for heading
            about_heading = soup.find(['h1', 'h2', 'h3', 'h4'], string=re.compile(r'about\s+this\s+product', re.I))
            if about_heading:
                about_section = about_heading.find_parent(['div', 'section'])
        
        if about_section:
            # Find all paragraphs in the about section
            paragraphs = about_section.find_all(['p', 'div'], class_=re.compile(r'text|description|content', re.I))
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and len(text) > 20:  # Filter out short/empty content
                    description_parts.append(text)
        
        # Strategy 2: Look for description in common Architonic patterns
        if not description_parts:
            # Look for product description divs
            desc_candidates = soup.find_all(['div', 'section'], class_=re.compile(r'product.*description|description.*product|product.*detail|detail.*product', re.I))
            for candidate in desc_candidates[:3]:  # Check first 3 candidates
                paragraphs = candidate.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text and len(text) > 50:  # Longer text more likely to be description
                        description_parts.append(text)
        
        # Strategy 3: Look for any div with class containing "cmpboxtxt" (from your HTML structure)
        if not description_parts:
            desc_div = soup.find('div', class_=re.compile(r'cmpboxtxt|cmptxt', re.I))
            if desc_div:
                paragraphs = desc_div.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text:
                        description_parts.append(text)
        
        # Combine all description parts
        if description_parts:
            full_description = ' '.join(description_parts)
            # Clean up whitespace
            full_description = re.sub(r'\s+', ' ', full_description).strip()
            return full_description[:1000]  # Limit to 1000 chars
        
        return ""
    
    def _scrape_with_selenium(self, url: str, brand_name: str) -> Dict:
        """Scrape using Selenium (recommended for Architonic)"""
        scraper = SeleniumScraper(headless=True, timeout=120)
        
        try:
            logger.info(f"Loading Architonic page: {url}")
            # Load page with various possible product selectors
            soup = scraper.get_page(url, wait_for_selector='body', wait_time=20)
            
            if not soup:
                return {'error': 'Failed to load page'}
            
            # Wait a bit more for JavaScript to fully render
            time.sleep(5)
            
            # First, extract categories from the sidebar/filters
            categories = self._extract_categories_from_page(scraper.driver.page_source)
            
            # Scroll multiple times to load all products (Architonic uses infinite scroll)
            logger.info("Starting to scroll and load all products...")
            products_seen = set()
            scroll_attempts = 0
            max_scrolls = 30  # Reduced limit for faster scraping
            no_new_products_count = 0
            
            while scroll_attempts < max_scrolls:
                scroll_attempts += 1
                logger.info(f"Scroll attempt {scroll_attempts}")
                
                # Get current products
                soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                current_products = self._extract_all_products_from_page(soup, url, brand_name)
                
                # Check if we got new products
                current_product_count = len(current_products)
                products_seen.add(current_product_count)
                
                logger.info(f"Found {current_product_count} products so far")
                
                # If we got the same count multiple times, we're done (faster exit)
                if len(products_seen) == 1 and scroll_attempts > 5:
                    no_new_products_count += 1
                    if no_new_products_count >= 3:  # Faster exit - reduced from 5
                        logger.info("No new products after multiple scrolls, assuming complete")
                        break
                else:
                    no_new_products_count = 0
                
                # Scroll down incrementally (faster scrolling)
                last_height = scraper.driver.execute_script("return document.body.scrollHeight")
                scraper.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)  # Reduced wait time from 3s
                
                # Scroll back up a bit and down again to trigger lazy loading
                scraper.driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 1000);")
                time.sleep(0.5)  # Reduced from 1s
                scraper.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)  # Reduced from 3s
                
                # Check if page height changed
                new_height = scraper.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # Try clicking "Load More" if it exists
                    load_more_clicked = self._click_load_more(scraper)
                    if not load_more_clicked:
                        time.sleep(1)  # Reduced wait time
                        # If still no change, we might be done
                        if no_new_products_count >= 2:  # Faster exit
                            break
            
            # Final extraction
            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
            products_data = self._extract_products_from_soup(soup, url, brand_name)
            
            # Use extracted categories
            if categories:
                products_data['categories'] = categories
                # Re-categorize products
                self._assign_products_to_categories(products_data, categories)
            
            logger.info(f"Final product count: {len(products_data['products'])}")
            
            # Return results immediately - no recursive retries (prevents infinite loops)
            # The calling code in app.py handles retries if needed
            return products_data
            
        except Exception as e:
            logger.error(f"Error in Selenium scraping: {e}")
            return {'error': str(e)}
        finally:
            # Close scraper if it was used
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
    
    def _scrape_with_requests(self, url: str, brand_name: str) -> Dict:
        """Scrape using requests (fallback, may not work well for JavaScript content)"""
        import requests
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            products_data = self._extract_products_from_soup(soup, url, brand_name)
            return products_data
            
        except Exception as e:
            logger.error(f"Error in requests scraping: {e}")
            return {'error': str(e)}
    
    def _extract_all_products_from_page(self, soup: BeautifulSoup, base_url: str, brand_name: str) -> List[Dict]:
        """Extract all products from page - returns list directly"""
        products = []
        products_seen = set()  # Track unique products by model+url
        
        # PRIORITY STRATEGY: Architonic uses /p/ URLs for products
        # Find all links with /p/ pattern FIRST (most reliable for Architonic)
        all_links = soup.find_all('a', href=True)
        product_links = []
        
        for link in all_links:
            href = link.get('href', '')
            # Architonic product pattern: /p/brand-product-name-id/
            if re.search(r'/p/[^/]+-\d+/?$', href, re.I):
                product_links.append(link)
        
        logger.info(f"Found {len(product_links)} product links with /p/ pattern (from {len(all_links)} total links)")
        
        products_found = product_links
        
        # Fallback Strategy 1: Try other product selectors if no /p/ links found
        if not products_found:
            product_selectors = [
                'article[data-product-id]',
                'article.product',
                '.product-item',
                '.product-card',
                '.product-tile',
                '[data-product-id]',
                'a[href*="/products/"]',
                '.product-grid-item'
            ]
            
            for selector in product_selectors:
                found = soup.select(selector)
                if found:
                    logger.info(f"Found {len(found)} elements using fallback selector: {selector}")
                    products_found.extend(found)
                    break  # Use first successful selector
        
        # Fallback Strategy 2: Find all product-type links
        if not products_found:
            for link in all_links:
                href = link.get('href', '')
                # More flexible patterns
                if re.search(r'/products?/(\d+)/?$', href, re.I):
                    product_links.append(link)
                elif re.search(r'/products?/[^/]+-(\d+)/?$', href, re.I):
                    if not re.search(r'/collections?/', href, re.I) and not re.search(r'/b/[^/]+/?$', href, re.I):
                        product_links.append(link)
            logger.info(f"Found {len(product_links)} product links using fallback patterns")
            products_found = product_links
        
        # ARCHITONIC SPECIFIC: Group links by URL first (multiple links per product)
        # Architonic has separate links for image and text pointing to the same product
        links_by_url = {}
        for link in products_found:
            href = link.get('href', '')
            if href:
                full_url = urljoin(base_url, href)
                if full_url not in links_by_url:
                    links_by_url[full_url] = []
                links_by_url[full_url].append(link)
        
        logger.info(f"Grouped {len(products_found)} links into {len(links_by_url)} unique product URLs")
        
        # Extract product information, combining data from multiple links
        extracted_count = 0
        skipped_count = 0
        error_count = 0
        
        for product_url, link_elements in links_by_url.items():
            try:
                # Combine data from all links pointing to the same product
                combined_product = None
                
                for link_elem in link_elements:
                    product = self._extract_product_info(link_elem, base_url)
                    if product:
                        if combined_product is None:
                            combined_product = product
                        else:
                            # Merge data: prefer non-empty values
                            if not combined_product.get('model') and product.get('model'):
                                combined_product['model'] = product['model']
                            if not combined_product.get('image_url') and product.get('image_url'):
                                combined_product['image_url'] = product['image_url']
                            if not combined_product.get('description') and product.get('description'):
                                combined_product['description'] = product['description']
                
                if combined_product and combined_product.get('model'):
                    # Check for duplicates by URL (already unique by grouping)
                    product_key = (combined_product.get('model', ''), combined_product.get('source_url', ''))
                    if product_key not in products_seen:
                        products_seen.add(product_key)
                        products.append(combined_product)
                        extracted_count += 1
                        logger.debug(f"✓ Extracted product {extracted_count}: {combined_product.get('model')} - {product_url[:80]}")
                    else:
                        skipped_count += 1
                else:
                    skipped_count += 1
                    logger.debug(f"✗ Skipped URL: No valid product data - {product_url[:80]}")
            except Exception as e:
                logger.debug(f"✗ Error extracting product from URL {product_url[:80]}: {e}")
                error_count += 1
                continue
        
        logger.info(f"Product extraction: {len(products_found)} links processed, {extracted_count} products extracted, {skipped_count} skipped, {error_count} errors")
        return products
    
    def _extract_products_from_soup(self, soup: BeautifulSoup, base_url: str, brand_name: str) -> Dict:
        """Extract products from BeautifulSoup object"""
        products_data = {
            'brand': brand_name,
            'website': base_url,
            'source': 'architonic',
            'categories': {},
            'products': []
        }
        
        products = self._extract_all_products_from_page(soup, base_url, brand_name)
        products_data['products'] = products
        
        # Remove duplicates based on model name
        seen_models = set()
        unique_products = []
        for product in products:
            model = product.get('model', '').strip().lower()
            if model and model not in seen_models:
                seen_models.add(model)
                unique_products.append(product)
        
        products_data['products'] = unique_products
        
        logger.info(f"Extracted {len(products_data['products'])} unique products from Architonic")
        return products_data
    
    def _extract_product_info(self, elem, base_url: str) -> Optional[Dict]:
        """Extract product information from an element (can be a link or container)"""
        try:
            # Determine if elem is the link itself or a container
            if elem.name == 'a' and elem.get('href'):
                link_elem = elem
                # For Architonic, when we pass links directly, we need to find the parent container
                # to get additional info like images
                container_elem = elem.find_parent(['div', 'article', 'li'])
            else:
                link_elem = elem.find('a', href=True)
                container_elem = elem
            
            if not link_elem:
                logger.debug("No link element found")
                return None
            
            href = link_elem.get('href', '')
            if not href:
                logger.debug("Link element has no href attribute")
                return None
            
            product_url = urljoin(base_url, href)
            
            # Exclude brand pages, collection pages, and other non-product URLs
            # Don't process if it's a collection or brand page
            if re.search(r'/collections?/', product_url, re.I):
                logger.debug(f"Skipping collection URL: {product_url}")
                return None  # Collection page, not a product
            if re.search(r'/b/[^/]+/?$', product_url, re.I):
                logger.debug(f"Skipping brand page URL: {product_url}")
                return None  # Brand page, not a product
            
            # Extract product ID from URL 
            # Architonic patterns: /p/brandname-product-name-20732680/ or /products/20732680/
            # Try /p/ pattern first (most common on Architonic)
            p_pattern_match = re.search(r'/p/[^/]+-(\d+)/?$', product_url)
            if p_pattern_match:
                product_id = p_pattern_match.group(1)
                logger.debug(f"Extracted product ID {product_id} from /p/ pattern: {product_url}")
            else:
                # Fallback to /products/id/ pattern (must be numeric ID only, not collection)
                id_match = re.search(r'/products?/(\d+)/?$', product_url)
                if id_match:
                    product_id = id_match.group(1)
                    logger.debug(f"Extracted product ID {product_id} from /products/id/ pattern: {product_url}")
                else:
                    # More flexible: Also try /products/name-id/ pattern
                    name_id_match = re.search(r'/products?/[^/]+-(\d+)/?$', product_url)
                    if name_id_match:
                        product_id = name_id_match.group(1)
                        logger.debug(f"Extracted product ID {product_id} from /products/name-id/ pattern: {product_url}")
                    else:
                        # Last resort: Check if it's a product URL without strict pattern
                        if '/p/' in product_url or '/product' in product_url:
                            # Extract any numeric ID from the URL (6+ digits)
                            any_id = re.search(r'(\d{6,})', product_url)
                            if any_id:
                                product_id = any_id.group(1)
                                logger.debug(f"Extracted product ID {product_id} from flexible pattern: {product_url}")
                            else:
                                # Accept product URLs even without numeric ID
                                logger.debug(f"Product URL without numeric ID, accepting anyway: {product_url}")
                                product_id = None  # Will be handled later
                        else:
                            logger.debug(f"Skipping non-product URL (no /p/ or /product): {product_url}")
                            return None
            
            # Find product name/title - try multiple strategies
            title = None
            search_elem = container_elem if container_elem else link_elem
            
            # Strategy 1: Get text from the link itself (most direct for Architonic)
            link_text = link_elem.get_text(strip=True)
            if link_text and len(link_text) > 1 and link_text.lower() not in ['products', 'product', 'view', 'see more']:
                title = link_text
            
            # Strategy 2: Look for headings with product title classes
            if not title:
                title_elem = search_elem.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], class_=re.compile(r'title|name|product', re.I))
                if not title_elem:
                    # Strategy 3: Look for any heading
                    title_elem = search_elem.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                if not title_elem:
                    # Strategy 4: Look for link with title text in container
                    title_elem = search_elem.find('a', class_=re.compile(r'title|name|product', re.I))
                if not title_elem:
                    # Strategy 5: Look for data attributes
                    title = search_elem.get('data-title') or search_elem.get('data-name') or search_elem.get('data-product-name')
                
                if title_elem and not title:
                    title = title_elem.get_text(strip=True)
            
            # Strategy 7: If title is generic like "Products" or missing, extract from URL slug
            # Architonic URLs are like: /en/p/brandname-product-name-12345678/ or /en/p/brandname-product-name-productid/
            if (not title or len(title) < 2 or title.lower() in ['products', 'product', 'view', 'see more', 'more']) and product_url:
                # Match pattern: /p/[brandname]-[product-name]-[id]/
                # Example: /en/p/narbutas-parthos-acoustic-columns-20732680/
                url_path_match = re.search(r'/p/([^/]+?)(?:-\d+)?/?$', product_url)
                if url_path_match:
                    full_slug = url_path_match.group(1)
                    slug_parts = full_slug.split('-')
                    
                    # Remove the last part if it's a numeric ID (product ID)
                    if slug_parts and slug_parts[-1].isdigit():
                        slug_parts = slug_parts[:-1]
                    
                    # The first part is usually the brand name, the rest is product name
                    # Example: ['narbutas', 'parthos', 'acoustic', 'columns'] -> 'Parthos Acoustic Columns'
                    if len(slug_parts) > 1:
                        # Skip first part (brand), join the rest
                        product_name_parts = slug_parts[1:]
                        title = ' '.join(product_name_parts).title()
                    elif len(slug_parts) == 1:
                        # If only one part, use it as product name
                        title = slug_parts[0].replace('-', ' ').title()
                    else:
                        # Fallback: use full slug
                        title = full_slug.replace('-', ' ').title()
                else:
                    # Try alternative pattern: /products/[id]/ or /product/[name]-[id]/
                    alt_match = re.search(r'/product(?:s)?/([^/]+?)(?:-\d+)?/?$', product_url)
                    if alt_match:
                        slug = alt_match.group(1)
                        # Remove numeric suffix if present
                        if slug.endswith('-') or re.search(r'-\d+$', slug):
                            slug = re.sub(r'-\d+$', '', slug)
                        title = slug.replace('-', ' ').title()
            
            # If still no good title, look for product name in img alt or other attributes
            if (not title or len(title) < 2 or title.lower() in ['products', 'product']) and link_elem:
                # Check img alt text within the link
                img = link_elem.find('img')
                if img:
                    alt_text = img.get('alt', '')
                    if alt_text and alt_text.lower() not in ['products', 'product', 'image', 'photo']:
                        title = alt_text.strip()
                
                # Check aria-label
                if (not title or title.lower() in ['products', 'product']) and link_elem.get('aria-label'):
                    aria_label = link_elem.get('aria-label')
                    if aria_label.lower() not in ['products', 'product', 'view', 'see more']:
                        title = aria_label.strip()
            
            # Final fallback: use "Unknown Product" if we have a URL but no name
            if not title or len(title) < 2:
                if product_url:
                    title = "Unknown Product"
                else:
                    return None
            
            # Find image - try multiple sources
            # Look in the link first, then in the container
            image_url = None
            img = link_elem.find('img')
            if not img and container_elem:
                img = container_elem.find('img')
            
            if img:
                # Architonic uses 'src' attribute directly (not lazy loading)
                image_url = img.get('src')
                
                # Fallback to other attributes
                if not image_url:
                    image_url = (img.get('data-src') or 
                               img.get('data-lazy-src') or 
                               img.get('data-original'))
                
                # Try srcset as last resort
                if not image_url and img.get('srcset'):
                    srcset = img.get('srcset', '')
                    # srcset format: "url 1x, url 2x" - get first URL
                    first_src = srcset.split(',')[0].strip().split(' ')[0]
                    if first_src:
                        image_url = first_src
            
            # Try background image from style attribute
            if not image_url and container_elem:
                style = container_elem.get('style', '')
                if style:
                    bg_match = re.search(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
                    if bg_match:
                        image_url = bg_match.group(1)
            
            if image_url:
                image_url = urljoin(base_url, image_url)
            
            # Find description
            description = ""
            if container_elem:
                desc_elem = container_elem.find(['p', 'div', 'span'], class_=re.compile(r'description|detail|summary|text|subtitle', re.I))
                if desc_elem:
                    description = desc_elem.get_text(strip=True)[:500]
            
            # Find designer/manufacturer info
            designer = ""
            if container_elem:
                designer_elem = container_elem.find(['span', 'div', 'p'], class_=re.compile(r'designer|manufacturer|brand|author', re.I))
                if designer_elem:
                    designer = designer_elem.get_text(strip=True)
            
            # Extract category if available
            category = ""
            if container_elem:
                category_elem = container_elem.find(['span', 'div'], class_=re.compile(r'category|type|group', re.I))
                if category_elem:
                    category = category_elem.get_text(strip=True)
            
            return {
                'model': title.strip(),
                'description': description,
                'designer': designer,
                'category': category,
                'image_url': image_url,
                'source_url': product_url or base_url,
                'product_id': product_id,  # Include product ID extracted from URL
                'price': None,  # Architonic usually doesn't show prices
                'features': []
            }
            
        except Exception as e:
            logger.debug(f"Error extracting product info: {e}")
            return None
    
    def _extract_categories_from_page(self, page_source: str) -> Dict[str, Dict]:
        """Extract categories from the filter sidebar"""
        soup = BeautifulSoup(page_source, 'html.parser')
        categories = {}
        
        # Look for filter sidebar - Architonic typically has filters on the left
        filter_sections = soup.find_all(['div', 'section', 'nav'], class_=re.compile(r'filter|sidebar|categories|facet', re.I))
        
        for section in filter_sections:
            # Look for "Categories" filter specifically
            category_header = section.find(string=re.compile(r'categor(y|ies)', re.I))
            if category_header:
                # Find parent container
                filter_container = category_header.find_parent(['div', 'section'])
                if filter_container:
                    # Find all category links/items
                    category_items = filter_container.find_all(['a', 'button', 'div'], href=re.compile(r'/products|/category|/filter', re.I))
                    for item in category_items:
                        cat_name = item.get_text(strip=True)
                        cat_href = item.get('href', '')
                        if cat_name and len(cat_name) > 0:
                            cat_url = urljoin(self.base_url, cat_href) if cat_href else None
                            categories[cat_name] = {
                                'name': cat_name,
                                'url': cat_url,
                                'products': []
                            }
        
        # Also look for breadcrumbs or navigation that might indicate categories
        breadcrumbs = soup.find_all(['nav', 'div'], class_=re.compile(r'breadcrumb|navigation', re.I))
        for breadcrumb in breadcrumbs:
            links = breadcrumb.find_all('a', href=True)
            for link in links:
                text = link.get_text(strip=True).lower()
                if text and text not in ['home', 'brands', 'products'] and len(text) > 2:
                    cat_name = link.get_text(strip=True)
                    cat_url = urljoin(self.base_url, link.get('href', ''))
                    if cat_name not in categories:
                        categories[cat_name] = {
                            'name': cat_name,
                            'url': cat_url,
                            'products': []
                        }
        
        logger.info(f"Extracted {len(categories)} categories from page")
        return categories
    
    def _detect_categories(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """Detect product categories from page (legacy method)"""
        categories = {}
        
        # Look for category navigation
        nav_elements = soup.find_all(['nav', 'ul', 'div'], class_=re.compile(r'category|filter|navigation', re.I))
        
        for nav in nav_elements:
            links = nav.find_all('a', href=True)
            for link in links:
                text = link.get_text(strip=True).lower()
                href = link.get('href', '')
                
                # Common furniture categories
                category_keywords = {
                    'seating': ['chair', 'seating', 'sofa', 'bench', 'stool'],
                    'desking': ['desk', 'table', 'workstation'],
                    'storage': ['storage', 'cabinet', 'shelf', 'drawer'],
                    'lighting': ['lamp', 'light', 'lighting'],
                    'accessories': ['accessory', 'accessories']
                }
                
                for category, keywords in category_keywords.items():
                    if any(keyword in text for keyword in keywords):
                        category_name = link.get_text(strip=True)
                        category_url = urljoin(base_url, href)
                        categories[category_name] = category_url
                        break
        
        # If no categories found, use generic
        if not categories:
            categories['General'] = base_url
        
        return categories
    
    def _categorize_product(self, product: Dict) -> str:
        """Categorize a product based on its name/description"""
        text = (product.get('model', '') + ' ' + product.get('description', '')).lower()
        
        if any(word in text for word in ['chair', 'seating', 'sofa', 'bench', 'stool', 'seat']):
            return 'Seating'
        elif any(word in text for word in ['desk', 'table', 'workstation']):
            return 'Desking'
        elif any(word in text for word in ['cabinet', 'storage', 'shelf', 'drawer']):
            return 'Storage'
        elif any(word in text for word in ['lamp', 'light']):
            return 'Lighting'
        else:
            return 'General'
    
    def _click_load_more(self, scraper: SeleniumScraper) -> bool:
        """Try to click 'Load More' button if it exists"""
        if not By:
            return False
        
        load_more_selectors = [
            'button[data-load-more]',
            '.load-more',
            'button:contains("Load More")',
            'button:contains("More")',
            'a:contains("More")',
            '.pagination .next',
            '[aria-label*="more" i]',
            '[aria-label*="load" i]'
        ]
        
        for selector in load_more_selectors:
            try:
                elements = scraper.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements:
                        try:
                            if elem.is_displayed() and elem.is_enabled():
                                scraper.driver.execute_script("arguments[0].scrollIntoView(true);", elem)
                                time.sleep(1)
                                elem.click()
                                time.sleep(3)  # Wait for content to load
                                logger.info(f"Clicked load more button: {selector}")
                                return True
                        except Exception as e:
                            logger.debug(f"Could not click element {selector}: {e}")
                            continue
            except Exception as e:
                logger.debug(f"Error finding load more button {selector}: {e}")
                continue
        
        return False
    
    def _assign_products_to_categories(self, products_data: Dict, categories: Dict):
        """Assign products to their appropriate categories"""
        if not categories:
            return
        
        # Reset category product lists
        for cat_name in categories:
            if 'products' not in categories[cat_name]:
                categories[cat_name]['products'] = []
        
        # Assign each product to a category
        for product in products_data.get('products', []):
            assigned = False
            
            # Try to use product's category field if available
            product_category = product.get('category', '').strip()
            if product_category:
                for cat_name, cat_data in categories.items():
                    if product_category.lower() in cat_name.lower() or cat_name.lower() in product_category.lower():
                        cat_data['products'].append(product)
                        assigned = True
                        break
            
            # Try to categorize by product name/description
            if not assigned:
                category_name = self._categorize_product(product)
                if category_name in categories:
                    categories[category_name]['products'].append(product)
                    assigned = True
            
            # Assign to "General" if no match found
            if not assigned:
                if 'General' in categories:
                    categories['General']['products'].append(product)
                elif categories:
                    # Assign to first available category
                    first_cat = list(categories.keys())[0]
                    categories[first_cat]['products'].append(product)
        
        # Update products_data categories structure
        products_data['categories'] = {}
        for cat_name, cat_data in categories.items():
            products_data['categories'][cat_name] = cat_data.get('products', [])
    
    def _handle_pagination(self, scraper: SeleniumScraper, url: str, brand_name: str, 
                          existing_products: Dict, max_pages: int = 10) -> Dict:
        """Handle pagination or 'load more' buttons (legacy method)"""
        try:
            # Look for "Load More" button
            load_more_selectors = [
                'button[data-load-more]',
                '.load-more',
                'button:contains("Load More")',
                'a:contains("More")',
                '.pagination .next'
            ]
            
            page = 1
            while page < max_pages:
                load_more_found = False
                
                for selector in load_more_selectors:
                    try:
                        elements = scraper.find_elements(By.CSS_SELECTOR, selector) if By else []
                        if elements and By and elements[0].is_displayed():
                            scraper.click_element(By.CSS_SELECTOR, selector)
                            time.sleep(3)  # Wait for content to load
                            
                            # Extract new products
                            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                            new_products = self._extract_products_from_soup(soup, url, brand_name)
                            
                            # Merge with existing
                            existing_products['products'].extend(new_products['products'])
                            existing_products['categories'].update(new_products['categories'])
                            
                            load_more_found = True
                            page += 1
                            break
                    except:
                        continue
                
                if not load_more_found:
                    break
            
            logger.info(f"Loaded {page} pages of products")
            return existing_products
            
        except Exception as e:
            logger.warning(f"Error handling pagination: {e}")
            return existing_products
    
    def _convert_collections_to_category_tree(self, collections_data: Dict) -> Dict:
        """
        Convert Architonic collections format to category_tree format
        
        Args:
            collections_data: Dictionary with collection names as keys
            
        Returns:
            category_tree format compatible with app.py
        """
        category_tree = {}
        
        for collection_name, collection_info in collections_data.items():
            # Skip if collection_name is None
            if not collection_name:
                continue
            # Clean collection name (remove product count)
            clean_name = collection_name.split('\n')[0].strip()
            
            # Convert products to expected format
            formatted_products = []
            for product in collection_info.get('products', []):
                formatted_product = {
                    'name': product.get('name', 'Unknown'),
                    'description': product.get('description', ''),
                    'image_url': product.get('image_url'),
                    'source_url': product.get('url', ''),
                    'brand': collection_info.get('category', clean_name),
                    'price': None,
                    'price_range': 'Contact for price',
                    'features': [],
                    'specifications': {},
                    'category_path': [clean_name],
                    'product_id': product.get('product_id', '')
                }
                formatted_products.append(formatted_product)
            
            # Create category structure
            if clean_name not in category_tree:
                category_tree[clean_name] = {
                    'subcategories': {}
                }
            
            # Use "General" as subcategory
            category_tree[clean_name]['subcategories']['General'] = {
                'products': formatted_products
            }
        
        return category_tree

