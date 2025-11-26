"""
Universal Brand Scraper - Works with all website formats
Combines multiple detection strategies with intelligent fallbacks
"""

import requests
from bs4 import BeautifulSoup
import logging
import time
import re
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from utils.selenium_scraper import SeleniumScraper
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    By = None
    logger.warning("Selenium not available")

try:
    from utils.architonic_scraper import ArchitonicScraper
    ARCHITONIC_AVAILABLE = True
except ImportError:
    ARCHITONIC_AVAILABLE = False


class CategoryTreeBuilder:
    """Builds and validates category hierarchy to eliminate duplicates"""
    
    def __init__(self):
        self.category_products = {}  # Track products per category for duplicate detection
        
    def build_tree(self, raw_categories: Dict) -> Dict:
        """
        Organize flat categories into proper tree structure
        Eliminates 'general' subcategories that duplicate parent products
        """
        tree = {}
        parent_categories = {}
        subcategories = {}
        
        # Separate parent categories from subcategories
        for coll_name, coll_info in raw_categories.items():
            if coll_info.get('subcategory'):
                # This is a subcategory
                parent = coll_info.get('category')
                if parent not in subcategories:
                    subcategories[parent] = []
                subcategories[parent].append(coll_name)
            else:
                # This is a parent category
                parent_categories[coll_name] = coll_info
        
        # Build final tree - only include parents if they have no subcategories
        for coll_name, coll_info in raw_categories.items():
            category = coll_info.get('category')
            
            # Skip parent categories that have subcategories (avoid "general" issue)
            if not coll_info.get('subcategory') and category in subcategories:
                logger.info(f"Skipping parent category '{coll_name}' - has subcategories, avoiding duplicate products")
                continue
                
            tree[coll_name] = coll_info
        
        return tree
    
    def detect_duplicate_categories(self, categories: Dict, product_urls: Dict[str, set]) -> List[str]:
        """
        Detect categories that contain the same products as their subcategories
        Returns list of category names to skip
        """
        duplicates = []
        
        # Group by parent category
        parent_map = {}
        for coll_name, coll_info in categories.items():
            parent = coll_info.get('category')
            if parent not in parent_map:
                parent_map[parent] = {'parent': None, 'children': []}
            
            if coll_info.get('subcategory'):
                parent_map[parent]['children'].append(coll_name)
            else:
                parent_map[parent]['parent'] = coll_name
        
        # Check for duplicates
        for parent, info in parent_map.items():
            if info['parent'] and info['children']:
                parent_urls = product_urls.get(info['parent'], set())
                
                # Combine all child URLs
                child_urls = set()
                for child in info['children']:
                    child_urls.update(product_urls.get(child, set()))
                
                # If parent has same products as children, it's a duplicate
                if parent_urls and child_urls:
                    overlap = len(parent_urls & child_urls) / len(parent_urls) if parent_urls else 0
                    if overlap > 0.8:  # 80% overlap threshold
                        logger.info(f"Detected duplicate category: '{info['parent']}' (80%+ overlap with subcategories)")
                        duplicates.append(info['parent'])
        
        return duplicates
    
    def validate_structure(self, tree: Dict) -> bool:
        """Ensure hierarchy makes logical sense"""
        # Check for circular references
        categories = set()
        for coll_info in tree.values():
            cat = coll_info.get('category')
            subcat = coll_info.get('subcategory')
            
            if cat and subcat and cat == subcat:
                logger.warning(f"Invalid hierarchy: category equals subcategory '{cat}'")
                return False
            
            categories.add(cat)
        
        return True


class UniversalBrandScraper:
    """Universal scraper that adapts to different website structures"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.rate_limit_delay = 1.0
        
        # Enhanced configuration
        self.config = {
            'parallel_collections': False,  # Disabled by default for stability
            'max_workers': 3,
            'enable_caching': True,
            'smart_pagination': True,
            'detect_general_category': True,  # Eliminate "general" duplicates
            'min_products_per_category': 1,
            'max_pagination_depth': 10,
            'hierarchy_validation': True
        }
        
        # Initialize tree builder
        self.tree_builder = CategoryTreeBuilder()
        self._page_cache = {}  # Cache for repeated page requests
    
    def check_robots_allowed(self, website: str) -> bool:
        """Check if scraping is allowed by robots.txt"""
        try:
            rp = urllib.robotparser.RobotFileParser()
            robots_url = urljoin(website, '/robots.txt')
            rp.set_url(robots_url)
            rp.read()
            
            # Check if our user agent can fetch the site
            return rp.can_fetch(self.headers['User-Agent'], website)
        except Exception as e:
            logger.warning(f"Could not read robots.txt: {e}")
            return True  # Allow by default if robots.txt is not accessible
    
    def scrape_brand_website(self, website: str, brand_name: str, use_selenium: bool = True) -> Dict:
        """
        Main entry point - intelligently scrapes any brand website
        Returns structured hierarchical data
        """
        logger.info(f"Starting universal scrape of {website}")
        
        # Check robots.txt (non-blocking, just warning)
        if not self.check_robots_allowed(website):
            logger.warning(f"⚠️  Scraping not allowed by robots.txt for {website}, continuing anyway")
        
        # Check if Architonic
        if ARCHITONIC_AVAILABLE and 'architonic.com' in website.lower():
            logger.info("Detected Architonic website")
            architonic_scraper = ArchitonicScraper(use_selenium=use_selenium)
            return architonic_scraper.scrape_collection(website, brand_name)
        
        # Determine if JavaScript is needed
        needs_js = use_selenium or self._detect_javascript_required(website)
        
        if needs_js and SELENIUM_AVAILABLE:
            return self._scrape_with_selenium(website, brand_name)
        else:
            return self._scrape_with_requests(website, brand_name)
    
    def _detect_javascript_required(self, website: str) -> bool:
        """Detect if website requires JavaScript"""
        try:
            response = requests.get(website, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check for common JS framework indicators
            js_indicators = [
                soup.find('div', id=re.compile(r'(root|app|react)', re.I)),
                soup.find('script', src=re.compile(r'(react|vue|angular)', re.I)),
                len(soup.find_all('div')) < 10  # Very sparse HTML
            ]
            
            return any(js_indicators)
        except:
            return True  # Default to Selenium if unsure
    
    def _scrape_with_requests(self, website: str, brand_name: str) -> Dict:
        """Scrape using requests - for static sites"""
        try:
            response = requests.get(website, headers=self.headers, timeout=15)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            collections = self._detect_hierarchy_universal(soup, website)
            
            result = {
                'brand': brand_name,
                'source': 'Brand Website',
                'scraped_at': datetime.now().isoformat(),
                'total_products': 0,
                'total_collections': len(collections),
                'collections': {},
                'all_products': []
            }
            
            all_products = []
            
            for coll_name, coll_info in collections.items():
                logger.info(f"Scraping collection: {coll_name}")
                time.sleep(self.rate_limit_delay)
                
                products = self._scrape_collection_universal(
                    coll_info['url'], brand_name, coll_info
                )
                
                result['collections'][coll_name] = {
                    'url': coll_info['url'],
                    'category': coll_info.get('category'),
                    'subcategory': coll_info.get('subcategory'),
                    'product_count': len(products),
                    'products': products
                }
                all_products.extend(products)
            
            # Apply cross-collection deduplication if enabled
            if self.config.get('detect_general_category', True) and len(result['collections']) > 1:
                logger.info("Applying cross-collection deduplication...")
                result['collections'] = self._cross_collection_deduplicate(result['collections'])
                
                # Rebuild all_products list
                all_products = []
                for coll_data in result['collections'].values():
                    all_products.extend(coll_data.get('products', []))
            
            result['all_products'] = all_products
            result['total_products'] = len(all_products)
            
            logger.info(f"Scraping complete: {len(all_products)} total products across {len(result['collections'])} collections")
            return result
            
        except Exception as e:
            logger.error(f"Error in requests scraping: {e}")
            return self._empty_result(brand_name)
    
    def _scrape_with_selenium(self, website: str, brand_name: str) -> Dict:
        """Scrape using Selenium - for dynamic sites"""
        scraper = SeleniumScraper(headless=True, timeout=60)
        
        try:
            logger.info(f"Loading with Selenium: {website}")
            soup = scraper.get_page(website, wait_time=15)
            scraper.scroll_to_bottom(pause_time=2.0)
            time.sleep(3)
            
            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
            
            # Detect dynamic submenus using Selenium hover (for dropdown menus)
            logger.info("Detecting dynamic submenus with Selenium hover...")
            dynamic_collections = self._detect_dynamic_submenus_with_selenium(scraper, website)
            time.sleep(1)  # Wait for any animations
            
            # Get updated page source after revealing submenus
            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
            
            # Detect hierarchy using multiple strategies
            collections = self._detect_hierarchy_universal(soup, website)
            
            # Merge dynamic collections with initial collections
            if dynamic_collections:
                logger.info(f"Merging {len(dynamic_collections)} dynamic subcategories with {len(collections)} initial collections")
                collections.update(dynamic_collections)
            
            result = {
                'brand': brand_name,
                'source': 'Brand Website (Selenium)',
                'scraped_at': datetime.now().isoformat(),
                'total_products': 0,
                'total_collections': len(collections),
                'collections': {},
                'all_products': []
            }
            
            all_products = []
            
            # Scrape each collection
            # Convert to list to allow appending during iteration if needed, 
            # but here we'll do a second pass for discovered subcategories
            
            # First pass: Scrape initial collections
            initial_collections = list(collections.items())
            for coll_name, coll_info in initial_collections:
                logger.info(f"Scraping collection: {coll_name}")
                time.sleep(self.rate_limit_delay)
                
                products = self._scrape_collection_with_selenium(
                    scraper, coll_info['url'], brand_name, coll_info
                )
                
                result['collections'][coll_name] = {
                    'url': coll_info['url'],
                    'category': coll_info.get('category'),
                    'subcategory': coll_info.get('subcategory'),
                    'product_count': len(products),
                    'products': products
                }
                all_products.extend(products)
            
            # Discovery pass: Check for subcategories if none were found
            if all_products and not any(c.get('subcategory') for c in collections.values()):
                logger.info("No subcategories detected. Attempting to discover from product breadcrumbs...")
                
                # Group products by category
                products_by_cat = {}
                for p in all_products:
                    cat = p.get('category')
                    if cat:
                        products_by_cat.setdefault(cat, []).append(p)
                
                new_collections = {}
                for cat, cat_products in products_by_cat.items():
                    discovered = self._discover_subcategories_from_products(cat_products, scraper)
                    if discovered:
                        logger.info(f"Discovered {len(discovered)} subcategories for {cat}")
                        for sub_name, sub_url in discovered.items():
                            coll_key = f"{cat} > {sub_name}"
                            if coll_key not in collections and coll_key not in new_collections:
                                new_collections[coll_key] = {
                                    'url': sub_url,
                                    'category': cat,
                                    'subcategory': sub_name
                                }
                
                # Second pass: Scrape discovered subcategories
                if new_collections:
                    logger.info(f"Scraping {len(new_collections)} discovered subcategories...")
                    for coll_name, coll_info in new_collections.items():
                        logger.info(f"Scraping subcategory: {coll_name}")
                        time.sleep(self.rate_limit_delay)
                        
                        products = self._scrape_collection_with_selenium(
                            scraper, coll_info['url'], brand_name, coll_info
                        )
                        
                        result['collections'][coll_name] = {
                            'url': coll_info['url'],
                            'category': coll_info.get('category'),
                            'subcategory': coll_info.get('subcategory'),
                            'product_count': len(products),
                            'products': products
                        }
                        all_products.extend(products)
            
            # Apply cross-collection deduplication if enabled
            if self.config.get('detect_general_category', True) and len(result['collections']) > 1:
                logger.info("Applying cross-collection deduplication...")
                result['collections'] = self._cross_collection_deduplicate(result['collections'])
                
                # Rebuild all_products list
                all_products = []
                for coll_data in result['collections'].values():
                    all_products.extend(coll_data.get('products', []))
            
            result['all_products'] = all_products
            result['total_products'] = len(all_products)
            
            logger.info(f"Scraping complete: {len(all_products)} total products across {len(result['collections'])} collections")
            return result
            
        except Exception as e:
            logger.error(f"Error in Selenium scraping: {e}")
            return self._empty_result(brand_name)
        finally:
            scraper.close()
    
    def _scrape_collection_with_selenium(self, scraper: SeleniumScraper, url: str, 
                                        brand_name: str, coll_info: Dict) -> List[Dict]:
        """Scrape a collection using Selenium with pagination"""
        products = []
        
        try:
            logger.info(f"Navigating to collection: {url}")
            scraper.get_page(url, wait_time=10)
            
            # For typology pages, wait longer for JavaScript to load products
            is_typology = '/typologies/' in url.lower()
            if is_typology:
                logger.info("Typology page detected - waiting for JavaScript to load products...")
                scraper.scroll_to_bottom(pause_time=2.0)
                time.sleep(5)  # Extra wait for JS-rendered content
                # Scroll again to trigger lazy loading
                scraper.scroll_to_bottom(pause_time=1.0)
                time.sleep(3)
            else:
                scraper.scroll_to_bottom(pause_time=2.0)
                time.sleep(3)
            
            page_count = 0
            max_pages = 10
            
            while page_count < max_pages:
                page_count += 1
                logger.info(f"Scraping page {page_count} of collection: {coll_info.get('category', 'Unknown')}")
                
                soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                page_products = self._extract_products_from_page(soup, url, brand_name, coll_info)
                
                logger.info(f"Found {len(page_products)} products on page {page_count}")
                
                # Deduplicate
                existing_urls = {p['source_url'] for p in products}
                new_products = 0
                for prod in page_products:
                    if prod['source_url'] not in existing_urls:
                        # Enrich product with description before adding
                        self._enrich_product_with_description(prod, scraper)
                        products.append(prod)
                        existing_urls.add(prod['source_url'])
                        new_products += 1
                
                logger.info(f"Added {new_products} new products with descriptions (total: {len(products)})")
                
                # If no products found on this page, stop pagination
                if len(page_products) == 0:
                    logger.info("No products found on this page, stopping pagination")
                    break
                
                # Try to find next page
                if not self._try_next_page(scraper):
                    logger.info("No next page found, stopping pagination")
                    break
                
                time.sleep(1.5)
            
            logger.info(f"Collection scraping complete. Total products: {len(products)}")
            
        except Exception as e:
            logger.error(f"Error scraping collection with Selenium {url}: {e}")
        
        return products
    
    def _detect_hierarchy_universal(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """
        Universal hierarchy detection using multiple strategies with smart validation
        Returns: {collection_name: {'url': ..., 'category': ..., 'subcategory': ...}}
        """
        raw_collections = {}
        
        # Strategy 0: Special handling for typology-based sites (LAS.it style)
        # Check if we're on a products page with typologies
        if '/products' in base_url.lower() or '/product' in base_url.lower():
            typology_collections = self._detect_typology_categories(soup, base_url)
            if typology_collections:
                logger.info(f"Found {len(typology_collections)} typology categories (LAS.it style)")
                raw_collections.update(typology_collections)
                # If we found typologies, skip other strategies to avoid navigation noise
                logger.info(f"Detected {len(raw_collections)} raw collections (typology-based)")
                if self.config.get('detect_general_category', True):
                    collections = self.tree_builder.build_tree(raw_collections)
                    logger.info(f"After hierarchy optimization: {len(collections)} collections")
                else:
                    collections = raw_collections
                if self.config.get('hierarchy_validation', True):
                    if not self.tree_builder.validate_structure(collections):
                        logger.warning("Hierarchy validation failed, using raw collections")
                        collections = raw_collections
                logger.info(f"Final collections count: {len(collections)}")
                return collections
        
        # Strategy 1: Navigation menu with submenus (primary)
        nav_collections = self._detect_from_navigation(soup, base_url)
        raw_collections.update(nav_collections)
        
        # Strategy 2: Category grid/list on homepage
        if not raw_collections:
            grid_collections = self._detect_from_category_grid(soup, base_url)
            raw_collections.update(grid_collections)
        
        # Strategy 3: Footer links
        if not raw_collections:
            footer_collections = self._detect_from_footer(soup, base_url)
            raw_collections.update(footer_collections)
        
        # Strategy 4: Sitemap or all products link
        if not raw_collections:
            sitemap_collections = self._detect_from_sitemap(soup, base_url)
            raw_collections.update(sitemap_collections)
        
        logger.info(f"Detected {len(raw_collections)} raw collections")
        
        # Apply smart hierarchy building if enabled
        if self.config.get('detect_general_category', True):
            collections = self.tree_builder.build_tree(raw_collections)
            logger.info(f"After hierarchy optimization: {len(collections)} collections (removed {len(raw_collections) - len(collections)} duplicates)")
        else:
            collections = raw_collections
        
        # Validate structure if enabled
        if self.config.get('hierarchy_validation', True):
            if not self.tree_builder.validate_structure(collections):
                logger.warning("Hierarchy validation failed, using raw collections")
                collections = raw_collections
        
        logger.info(f"Final collections count: {len(collections)}")
        return collections
    
    def _detect_typology_categories(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """
        Detect typology categories for LAS.it and similar sites
        Looks for /typologies/ links and extracts category names
        """
        collections = {}
        
        # Find all typology links
        typology_links = soup.find_all('a', href=re.compile(r'/typologies/', re.I))
        logger.info(f"Found {len(typology_links)} typology links")
        
        for link in typology_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            full_url = urljoin(base_url, href)
            
            # Get category name from link text or nearby heading
            text = link.get_text(strip=True)
            
            # Skip generic links
            if text.lower() in ['find out more', 'read more', 'view', 'see more', '']:
                # Try to get from parent/section heading
                parent = link.find_parent(['div', 'article', 'section', 'li'])
                if parent:
                    heading = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if heading:
                        text = heading.get_text(strip=True)
            
            # If still no text, extract from URL
            if not text or len(text) < 2:
                url_parts = [p for p in href.split('/') if p]
                if 'typologies' in url_parts:
                    typology_index = url_parts.index('typologies')
                    if typology_index + 1 < len(url_parts):
                        category_slug = url_parts[typology_index + 1]
                        text = category_slug.replace('-', ' ').replace('_', ' ').title()
            
            if text and len(text) >= 2:
                # Clean up text
                text = self._clean_category_name(text)
                if text and text not in collections:
                    collections[text] = {
                        'url': full_url,
                        'category': text,
                        'subcategory': None
                    }
                    logger.info(f"  ✓ Found typology: {text} -> {full_url}")
        
        return collections
    
    def _detect_from_navigation(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Detect categories from navigation menu with enhanced submenu detection"""
        collections = {}
        parent_has_children = set()  # Track which parents have subcategories
        
        # Find navigation
        nav_selectors = [
            ('nav', {}),
            ('div', {'class': re.compile(r'(nav|menu|header)', re.I)}),
            ('ul', {'class': re.compile(r'(nav|menu)', re.I)})
        ]
        
        for tag, attrs in nav_selectors:
            navs = soup.find_all(tag, attrs)
            for nav in navs:
                # Find all links
                links = nav.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '').strip()
                    text = link.get_text(strip=True)
                    
                    # Filter for category-like links
                    if self._is_category_link(href, text):
                        full_url = urljoin(base_url, href)
                        clean_name = self._clean_category_name(text)
                        
                        if clean_name and full_url not in [c['url'] for c in collections.values()]:
                            # Check for subcategories - look for multiple submenu patterns
                            parent = link.find_parent(['li', 'div'])
                            submenu = None
                            
                            if parent:
                                # Try multiple submenu selectors
                                submenu = (
                                    parent.find(['ul', 'div'], class_=re.compile(r'(sub|dropdown|child)', re.I)) or
                                    parent.find('ul') or  # Generic ul as fallback
                                    parent.find('div', class_=re.compile(r'menu', re.I))
                                )
                            
                            if submenu:
                                # Has subcategories - mark parent and add children
                                parent_has_children.add(clean_name)
                                sublinks = submenu.find_all('a', href=True)
                                
                                for sublink in sublinks:
                                    subhref = sublink.get('href', '').strip()
                                    subtext = sublink.get_text(strip=True)
                                    
                                    if self._is_category_link(subhref, subtext):
                                        sub_full_url = urljoin(base_url, subhref)
                                        sub_clean_name = self._clean_category_name(subtext)
                                        
                                        if sub_clean_name and sub_clean_name != clean_name:
                                            coll_key = f"{clean_name} > {sub_clean_name}"
                                            collections[coll_key] = {
                                                'url': sub_full_url,
                                                'category': clean_name,
                                                'subcategory': sub_clean_name
                                            }
                                
                                # Also add parent category (will be filtered by tree builder if needed)
                                collections[clean_name] = {
                                    'url': full_url,
                                    'category': clean_name,
                                    'subcategory': None
                                }
                            else:
                                # Top-level category without subcategories
                                collections[clean_name] = {
                                    'url': full_url,
                                    'category': clean_name,
                                    'subcategory': None
                                }
        
        logger.info(f"Navigation detection: {len(collections)} total, {len(parent_has_children)} parents with children")
        return collections
    
    def _detect_typology_categories(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """
        Detect typology categories for LAS.it and similar sites
        Looks for /typologies/ links and extracts category names
        """
        collections = {}
        
        # Find all typology links
        typology_links = soup.find_all('a', href=re.compile(r'/typologies/', re.I))
        logger.info(f"Found {len(typology_links)} typology links")
        
        for link in typology_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            full_url = urljoin(base_url, href)
            
            # Get category name from link text or nearby heading
            text = link.get_text(strip=True)
            
            # Skip generic links
            if text.lower() in ['find out more', 'read more', 'view', 'see more', '']:
                # Try to get from parent/section heading
                parent = link.find_parent(['div', 'article', 'section', 'li'])
                if parent:
                    heading = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if heading:
                        text = heading.get_text(strip=True)
            
            # If still no text, extract from URL
            if not text or len(text) < 2:
                url_parts = [p for p in href.split('/') if p]
                if 'typologies' in url_parts:
                    typology_index = url_parts.index('typologies')
                    if typology_index + 1 < len(url_parts):
                        category_slug = url_parts[typology_index + 1]
                        text = category_slug.replace('-', ' ').replace('_', ' ').title()
            
            if text and len(text) >= 2:
                # Clean up text
                text = self._clean_category_name(text)
                if text and text not in collections:
                    collections[text] = {
                        'url': full_url,
                        'category': text,
                        'subcategory': None
                    }
                    logger.info(f"  ✓ Found typology: {text} -> {full_url}")
        
        return collections
    
    def _detect_from_category_grid(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Detect categories from grid/list on homepage"""
        collections = {}
        
        # Look for category grids
        grid_selectors = [
            ('div', {'class': re.compile(r'(category|collection|product-cat)', re.I)}),
            ('li', {'class': re.compile(r'(category|collection)', re.I)})
        ]
        
        for tag, attrs in grid_selectors:
            items = soup.find_all(tag, attrs)
            for item in items:
                link = item.find('a', href=True)
                if link:
                    href = link.get('href', '').strip()
                    text = link.get_text(strip=True) or item.get_text(strip=True)
                    
                    if self._is_category_link(href, text):
                        full_url = urljoin(base_url, href)
                        clean_name = self._clean_category_name(text)
                        
                        if clean_name:
                            collections[clean_name] = {
                                'url': full_url,
                                'category': clean_name,
                                'subcategory': None
                            }
        
        return collections

    
    def _detect_from_footer(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Detect categories from footer links"""
        collections = {}
        
        footer = soup.find('footer') or soup.find('div', class_=re.compile(r'footer', re.I))
        if footer:
            links = footer.find_all('a', href=True)
            for link in links:
                href = link.get('href', '').strip()
                text = link.get_text(strip=True)
                
                if self._is_category_link(href, text):
                    full_url = urljoin(base_url, href)
                    clean_name = self._clean_category_name(text)
                    
                    if clean_name:
                        collections[clean_name] = {
                            'url': full_url,
                            'category': clean_name,
                            'subcategory': None
                        }
        
        return collections
    
    def _detect_from_sitemap(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Try to find sitemap or all products page"""
        collections = {}
        
        # Look for sitemap links
        sitemap_link = soup.find('a', href=re.compile(r'(sitemap|all-products|products)', re.I))
        if sitemap_link:
            # This would require additional scraping
            pass
        
        return collections
    
    def _is_category_link(self, href: str, text: str) -> bool:
        """Determine if a link is likely a category"""
        if not href or not text:
            return False
        
        # Exclude common non-category links
        exclude_patterns = [
            r'#', r'javascript:', r'mailto:', r'tel:',
            r'(login|register|account|cart|checkout|contact|about|faq)',
            r'(facebook|twitter|instagram|linkedin|youtube)',
            r'\.(pdf|jpg|png|gif|zip)'
        ]
        
        for pattern in exclude_patterns:
            if re.search(pattern, href, re.I) or re.search(pattern, text, re.I):
                return False
        
        # Include category-like patterns
        include_patterns = [
            r'(category|collection|product|shop|furniture)',
            r'(chair|desk|table|sofa|storage|office)'
        ]
        
        for pattern in include_patterns:
            if re.search(pattern, href, re.I):
                return True
        # Check text length (categories usually have short names)
        return 2 <= len(text) <= 50

    def _clean_category_name(self, name: str) -> str:
        """Clean up category name (e.g. 'Open submenu (Chairs)' -> 'Chairs')"""
        if not name:
            return ""
        
        # Normalize whitespace
        name = " ".join(name.split())
        
        # Remove "Open/Close submenu" prefixes/suffixes
        # Matches: "Open submenu", "Close submenu", "Open submenu (Chairs)", "Close submenu [Chairs]"
        name = re.sub(r'^(Open|Close)\s+submenu\s*[\(\[]?', '', name, flags=re.I)
        name = re.sub(r'[\)\]]$', '', name)
        
        # Remove "Toggle" prefix
        name = re.sub(r'^Toggle\s+', '', name, flags=re.I)
        
        # Remove counts (e.g. "Chairs (10)")
        name = re.sub(r'\s*\(\d+\)$', '', name)
        
        # Remove common navigation artifacts
        name = re.sub(r'^(View|See|Show|All)\s+', '', name, flags=re.I)
        
        return name.strip()
    
    def _extract_product_features(self, soup: BeautifulSoup) -> List[str]:
        """Extract product features/specifications from product page"""
        features = []
        
        try:
            # Look for feature lists
            feature_lists = soup.find_all(['ul', 'ol'], class_=re.compile(r'(feature|spec|benefit|specification)', re.I))
            
            for feature_list in feature_lists[:2]:  # Limit to 2 lists
                for item in feature_list.find_all('li')[:10]:  # Max 10 features per list
                    text = item.get_text(strip=True)
                    if text and len(text) < 200:  # Reasonable feature length
                        features.append(text)
            
            # Also look for specification tables
            spec_tables = soup.find_all('table', class_=re.compile(r'(spec|feature|detail)', re.I))
            for table in spec_tables[:1]:  # Limit to 1 table
                rows = table.find_all('tr')[:10]  # Max 10 rows
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        # Format as "Key: Value"
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        if key and value and len(key) < 50 and len(value) < 100:
                            features.append(f"{key}: {value}")
        
        except Exception as e:
            logger.debug(f"Error extracting features: {e}")
        
        return features[:15]  # Limit total features to 15
    
    def _try_next_page(self, scraper: SeleniumScraper) -> bool:
        """Try to navigate to next page"""
        next_selectors = [
            "a[rel='next']",
            "a.next",
            "button.next",
            ".pagination a:last-child",
            "a:contains('Next')",
            "button:contains('Load More')"
        ]
        
        for selector in next_selectors:
            try:
                elements = scraper.driver.find_elements("css selector", selector)
                for elem in elements:
                    if elem.is_displayed() and elem.is_enabled():
                        elem.click()
                        time.sleep(2)
                        return True
            except:
                continue
        
        return False
    
    def _enrich_product_with_description(self, product: Dict, scraper: SeleniumScraper):
        """Enrich a product with description by visiting its detail page"""
        try:
            if not product.get('source_url'):
                return
            
            scraper.get_page(product['source_url'], wait_time=3)
            soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
            
            # Extract description using multiple strategies
            description = None
            
            # Strategy 1: Common description selectors
            desc_selectors = [
                'div.entry-content',
                'div.product-content',
                'div.content p',
                'article p',
                'section.product-description',
                'div.description',
                'div.woocommerce-product-details__short-description',
                'div.product-short-description',
                'div[itemprop="description"]',
                'div#tab-description',
                'div.woocommerce-Tabs-panel--description'
            ]
            
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    # Remove script, style, nav, header, footer
                    for tag in desc_elem.find_all(['script', 'style', 'nav', 'header', 'footer', 'button', 'a']):
                        tag.decompose()
                    
                    desc_text = desc_elem.get_text(separator=' ', strip=True)
                    if desc_text and len(desc_text) > 30:  # Minimum description length
                        description = desc_text
                        break
            
            # Strategy 2: Try to find main content area
            if not description:
                main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile(r'content|main', re.I))
                if main_content:
                    # Get all paragraphs
                    paragraphs = main_content.find_all('p')
                    if paragraphs:
                        desc_text = ' '.join(p.get_text(strip=True) for p in paragraphs[:5])
                        if desc_text and len(desc_text) > 30:
                            description = desc_text
            
            # Clean up description
            if description:
                # Remove extra whitespace
                description = re.sub(r'\s+', ' ', description).strip()
                # Remove common unwanted text
                unwanted = ['add to cart', 'add to wishlist', 'share:', 'sku:', 'categories:', 'tags:', 'read more', 'view all']
                for unwanted_text in unwanted:
                    description = re.sub(unwanted_text, '', description, flags=re.IGNORECASE)
                description = description.strip()
                
                # Limit description length
                if len(description) > 1000:
                    description = description[:1000] + '...'
            
            product['description'] = description or ''
            
        except Exception as e:
            logger.debug(f"Could not fetch description for {product.get('source_url', 'unknown')}: {e}")
            product['description'] = ''
    
    def fetch_product_details(self, product_url: str, use_selenium: bool = False) -> Dict:
        """
        Fetch detailed product information from a product page
        Returns: {'description': str, 'image_url': str, 'features': list, 'price': str}
        """
        details = {
            'description': None,
            'image_url': None,
            'features': [],
            'price': None
        }
        
        try:
            if use_selenium and SELENIUM_AVAILABLE:
                scraper = SeleniumScraper(headless=True, timeout=30)
                try:
                    scraper.get_page(product_url, wait_time=5)
                    soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                finally:
                    scraper.close()
            else:
                response = requests.get(product_url, headers=self.headers, timeout=10)
                soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract description
            desc_selectors = [
                soup.find('div', class_=re.compile(r'product.*description', re.I)),
                soup.find('div', class_=re.compile(r'description', re.I)),
                soup.find('div', {'id': re.compile(r'description', re.I)}),
                soup.find('div', class_=re.compile(r'product.*content', re.I))
            ]
            
            for desc_elem in desc_selectors:
                if desc_elem:
                    # Remove script and style tags
                    for tag in desc_elem.find_all(['script', 'style']):
                        tag.decompose()
                    
                    desc_text = desc_elem.get_text(separator=' ', strip=True)
                    if desc_text and len(desc_text) > 20:
                        details['description'] = desc_text
                        break
            
            # Extract product features/specifications
            features = self._extract_product_features(soup)
            if features:
                details['features'] = features
            
            # Extract main image
            img_selectors = [
                soup.find('img', class_=re.compile(r'product.*image', re.I)),
                soup.find('div', class_=re.compile(r'product.*gallery', re.I)),
                soup.find('figure', class_=re.compile(r'product', re.I))
            ]
            
            for img_container in img_selectors:
                if img_container:
                    img = img_container if img_container.name == 'img' else img_container.find('img')
                    if img:
                        image_url = img.get('src') or img.get('data-src') or img.get('data-large-image')
                        if image_url:
                            details['image_url'] = urljoin(product_url, image_url)
                            break
            
            # Features already extracted above via _extract_product_features
            
            # Extract price
            price_selectors = [
                soup.find('span', class_=re.compile(r'price', re.I)),
                soup.find('div', class_=re.compile(r'price', re.I)),
                soup.find('p', class_=re.compile(r'price', re.I))
            ]
            
            for price_elem in price_selectors:
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    if price_text and any(char.isdigit() for char in price_text):
                        details['price'] = price_text
                        break
            
        except Exception as e:
            logger.error(f"Error fetching product details from {product_url}: {e}")
        
        return details
    
    def _discover_subcategories_from_products(self, products: List[Dict], scraper) -> Dict[str, str]:
        """
        Visit a few products to discover subcategories via breadcrumbs
        Returns {subcategory_name: subcategory_url}
        """
        discovered = {}
        # Limit to first 3 products to save time
        for prod in products[:3]:
            url = prod.get('source_url')
            if not url: continue
            
            try:
                logger.info(f"Checking product for subcategories: {url}")
                scraper.get_page(url, wait_time=5)
                soup = BeautifulSoup(scraper.driver.page_source, 'html.parser')
                breadcrumbs = self.extract_breadcrumb_links(soup)
                
                # Look for parent category in breadcrumbs and take the next item
                parent_category = prod.get('collection') or prod.get('category')
                if not parent_category:
                    continue
                    
                for i, (name, link) in enumerate(breadcrumbs):
                    # loose match for parent category
                    clean_name = self._clean_category_name(name).lower()
                    clean_parent = parent_category.lower()
                    
                    if clean_name in clean_parent or clean_parent in clean_name:
                        # Check if there is a next item that is NOT the product itself
                        if i + 1 < len(breadcrumbs):
                            sub_name, sub_link = breadcrumbs[i+1]
                            # Verify it's not the product name (approximate check)
                            if sub_link and sub_link != url:
                                clean_sub = self._clean_category_name(sub_name)
                                if clean_sub and clean_sub.lower() != clean_parent:
                                    discovered[clean_sub] = sub_link
            except Exception as e:
                logger.warning(f"Error discovering subcategories from {url}: {e}")
                
        return discovered

    def extract_breadcrumb_links(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extract breadcrumb links (name, url) from product page"""
        
        # Common breadcrumb selectors
        selectors = [
            (['nav', 'div', 'ul', 'ol'], {'class': re.compile(r'(breadcrumb|bread-crumb|path)', re.I)}),
            ('div', {'id': re.compile(r'(breadcrumb)', re.I)}),
        ]
        
        for tag, attrs in selectors:
            # Use find_all to get all potential containers
            containers = soup.find_all(tag, attrs)
            for container in containers:
                breadcrumbs = []
                links = container.find_all('a', href=True)
                for link in links:
                    text = link.get_text(strip=True)
                    href = link.get('href')
                    if text and href and text not in ['Home', '>', '/', '»']:
                        breadcrumbs.append((text, href))
                
                # If we found a container with links, return it
                if breadcrumbs:
                    return breadcrumbs
        
        return []

    def _empty_result(self, brand_name: str) -> Dict:
        """Return empty result structure"""
        return {
            'brand': brand_name,
            'source': 'Brand Website',
            'scraped_at': datetime.now().isoformat(),
            'total_products': 0,
            'total_collections': 0,
            'collections': {},
            'all_products': []
        }

    def _extract_products_from_page(self, soup: BeautifulSoup, page_url: str, 
                                   brand_name: str, coll_info: Dict) -> List[Dict]:
        """Extract products from a page using universal selectors"""
        products = []
        
        # Multiple container patterns - WooCommerce first, then generic
        container_selectors = [
            # WooCommerce specific (li or div)
            ('li', {'class': re.compile(r'product\s|type-product', re.I)}),
            ('div', {'class': re.compile(r'product\s|type-product', re.I)}),
            ('div', {'class': re.compile(r'product-grid-item', re.I)}),
            ('div', {'class': re.compile(r'product-item', re.I)}),
            
            # Generic product containers
            ('div', {'class': re.compile(r'product-card', re.I)}),
            ('article', {'class': re.compile(r'product', re.I)}),
            ('div', {'class': re.compile(r'(item|card).*product', re.I)}),
            
            # Fallback
            ('div', {'class': re.compile(r'(item|card)', re.I)})
        ]
        
        containers = []
        for tag, attrs in container_selectors:
            found = soup.find_all(tag, attrs, limit=200)
            if found:
                logger.debug(f"Found {len(found)} containers with {tag} {attrs}")
                containers.extend(found)
                if len(containers) > 20:  # Found enough
                    break
        
        logger.info(f"Found {len(containers)} potential product containers")
        
        for container in containers:
            product = self._extract_product_from_container(container, page_url, brand_name, coll_info)
            if product:
                products.append(product)
        
        logger.info(f"Extracted {len(products)} products from page")
        return products
    
    def _extract_product_from_container(self, container: BeautifulSoup, base_url: str,
                                       brand_name: str, coll_info: Dict) -> Optional[Dict]:
        """Extract product info from a container element with enhanced filtering"""
        try:
            # Find link first (most reliable)
            link_elem = container.find('a', href=True)
            product_url = urljoin(base_url, link_elem['href']) if link_elem else None
            
            # Skip if URL looks like a category
            if product_url:
                # Skip category URLs
                if any(pattern in product_url.lower() for pattern in ['/product-category/', '/category/', '/typologies/']):
                    # But allow if it's a product within typology (deeper path)
                    if '/typologies/' in product_url.lower():
                        path_parts = [p for p in product_url.split('/') if p]
                        typology_index = [i for i, p in enumerate(path_parts) if 'typologies' in p.lower()]
                        if typology_index and len(path_parts) <= typology_index[0] + 2:
                            # This is just the typology category page, not a product
                            return None
                    else:
                        return None
            
            # Find title - try multiple strategies
            title = None
            
            # Strategy 1: Look for title/name/product class
            title_elem = container.find(['h2', 'h3', 'h4', 'a', 'span', 'div'], class_=re.compile(r'(title|name|product.*name|woocommerce-loop-product__title)', re.I))
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Strategy 2: If no title found, try any heading in the container
            if not title:
                for tag in ['h2', 'h3', 'h4', 'h5']:
                    heading = container.find(tag)
                    if heading:
                        title = heading.get_text(strip=True)
                        break
            
            # Strategy 3: If still no title, use the link text
            if not title and link_elem:
                title = link_elem.get_text(strip=True)
            
            # Strategy 4: Try to find any text in the container (last resort)
            if not title:
                # Get all text but limit length
                all_text = container.get_text(strip=True)
                if all_text and len(all_text) < 200:  # Reasonable title length
                    title = all_text
            
            # VALIDATION: Filter out invalid titles (navigation items, etc.)
            if not title:
                return None
                
            # Check for navigation keywords
            nav_keywords = [
                'open submenu', 'close submenu', 'toggle', 'menu', 'back', 
                'search', 'cart', 'account', 'login', 'register', 'checkout',
                'view all', 'read more', 'select options', 'add to cart',
                'filter', 'sort', 'previous', 'next'
            ]
            
            title_lower = title.lower()
            if any(keyword in title_lower for keyword in nav_keywords):
                return None
                
            # Check for very short titles or just numbers
            if len(title) < 2 or title.isdigit():
                return None
            
            # Find image
            img = container.find('img')
            image_url = None
            if img:
                image_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src') or img.get('data-original')
                if image_url:
                    image_url = urljoin(base_url, image_url)
            
            # Find price
            price_elem = container.find(['span', 'div'], class_=re.compile(r'price', re.I))
            price = self._parse_price(price_elem.get_text(strip=True)) if price_elem else None
            
            # Only return if we have at least a title or a product URL
            if title or product_url:
                product = {
                    'brand': brand_name,
                    'model': title or 'Unknown Product',
                    'image_url': image_url,
                    'price': price,
                    'source_url': product_url or base_url,
                    'collection': coll_info.get('collection', 'General'),
                    'category': coll_info.get('category', 'General'),
                    'subcategory': coll_info.get('subcategory', 'General'),
                    'description': ''  # Will be populated if fetch_product_details is called
                }
                return product
            
            return None
            
        except Exception as e:
            # logger.debug(f"Error extracting product from container: {e}")
            return None

    def _parse_price(self, price_str: str) -> Optional[str]:
        """Parse price string"""
        if not price_str:
            return None
        # Extract numbers and currency symbols
        match = re.search(r'[\d,.]+', price_str)
        return match.group(0) if match else None

    def _try_next_page(self, scraper) -> bool:
        """
        Smart pagination detection - tries multiple strategies to find and click next page
        Returns True if successfully navigated to next page
        """
        if not self.config.get('smart_pagination', True):
            return False
        
        try:
            # Strategy 1: Look for "Next" button/link
            next_selectors = [
                ('a', {'class': re.compile(r'next', re.I)}),
                ('a', {'rel': 'next'}),
                ('button', {'class': re.compile(r'next', re.I)}),
                ('a', {'aria-label': re.compile(r'next', re.I)})
            ]
            
            for tag, attrs in next_selectors:
                next_elem = scraper.driver.find_elements('css selector', f"{tag}")
                for elem in next_elem:
                    # Check if element text or attributes match "next"
                    if ('next' in elem.text.lower() or 
                        'next' in elem.get_attribute('class').lower() or
                        'next' in (elem.get_attribute('aria-label') or '').lower()):
                        # Check if not disabled
                        if 'disabled' not in elem.get_attribute('class').lower():
                            elem.click()
                            time.sleep(2)
                            return True
            
            # Strategy 2: Look for numbered pagination and click next number
            current_page = scraper.driver.find_elements('css selector', '.page.current, .pagination .active, [aria-current="page"]')
            if current_page:
                try:
                    current_num = int(current_page[0].text)
                    next_page_link = scraper.driver.find_elements('css selector', f'a[href*="page={current_num + 1}"], a[href*="p={current_num + 1}"]')
                    if next_page_link:
                        next_page_link[0].click()
                        time.sleep(2)
                        return True
                except:
                    pass
            
            return False
            
        except Exception as e:
            logger.debug(f"Pagination detection failed: {e}")
            return False
    
    def _cross_collection_deduplicate(self, all_collections: Dict) -> Dict:
        """
        Deduplicate products across all collections
        Ensures each product appears in only one subcategory
        """
        product_map = {}  # URL -> (collection_name, product_data)
        
        # First pass: collect all products
        for coll_name, coll_data in all_collections.items():
            products = coll_data.get('products', [])
            for product in products:
                url = product.get('source_url')
                if url:
                    if url not in product_map:
                        product_map[url] = (coll_name, product)
                    else:
                        # Product exists in multiple collections
                        # Prefer subcategory over parent category
                        existing_coll, existing_prod = product_map[url]
                        
                        # Check if current collection is more specific
                        current_has_subcat = coll_data.get('subcategory') is not None
                        existing_has_subcat = all_collections[existing_coll].get('subcategory') is not None
                        
                        if current_has_subcat and not existing_has_subcat:
                            # Current is more specific, replace
                            product_map[url] = (coll_name, product)
                            logger.debug(f"Product {url} moved from '{existing_coll}' to '{coll_name}' (more specific)")
        
        # Second pass: rebuild collections with deduplicated products
        deduplicated = {}
        for coll_name, coll_data in all_collections.items():
            deduplicated[coll_name] = coll_data.copy()
            deduplicated[coll_name]['products'] = []
        
        # Assign each product to its final collection
        for url, (coll_name, product) in product_map.items():
            deduplicated[coll_name]['products'].append(product)
        
        # Update product counts
        for coll_name in deduplicated:
            deduplicated[coll_name]['product_count'] = len(deduplicated[coll_name]['products'])
        
        return deduplicated
    
    def _scrape_collection_universal(self, url: str, brand_name: str, coll_info: Dict) -> List[Dict]:
        """Universal collection scraper using requests (for static sites)"""
        products = []
        
        try:
            # Check cache first
            if self.config.get('enable_caching') and url in self._page_cache:
                logger.debug(f"Using cached page for {url}")
                soup = self._page_cache[url]
            else:
                response = requests.get(url, headers=self.headers, timeout=15)
                soup = BeautifulSoup(response.content, 'html.parser')
                if self.config.get('enable_caching'):
                    self._page_cache[url] = soup
            
            # Extract products from page
            products = self._extract_products_from_page(soup, url, brand_name, coll_info)
            
        except Exception as e:
            logger.error(f"Error scraping collection {url}: {e}")
        
        return products

    def _detect_dynamic_submenus_with_selenium(self, scraper, base_url: str):
        """
        Detect and extract dynamic dropdown submenus by directly interacting with menu items using Selenium
        Returns additional collections found in submenus
        """
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            # Import By - use local import as primary, fallback to module-level if available
            try:
                from selenium.webdriver.common.by import By
            except ImportError:
                # Try to use module-level By if it exists
                try:
                    By = globals().get('By')
                    if By is None:
                        logger.warning("Selenium By is not available")
                        return {}
                except (NameError, KeyError):
                    logger.warning("Selenium By is not available")
                    return {}
            
            logger.info("Extracting subcategories from dynamic menus...")
            
            additional_collections = {}
            actions = ActionChains(scraper.driver)
            
            # Find main navigation menu items
            nav_selectors = [
                "nav a",
                ".menu-item > a",
                ".nav-item > a",
                "header nav a",
                "[class*='menu'] > li > a",
                "[class*='nav'] > li > a"
            ]
            
            for selector in nav_selectors:
                try:
                    menu_items = scraper.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if not menu_items or len(menu_items) == 0:
                        continue
                    
                    logger.debug(f"Found {len(menu_items)} menu items with selector: {selector}")
                    
                    # Process each menu item
                    for item in menu_items[:15]:  # Limit to first 15 items
                        try:
                            if not item.is_displayed():
                                continue
                            
                            parent_text = item.text.strip()
                            if not parent_text or len(parent_text) > 50:
                                continue
                            
                            # Clean parent category name
                            parent_category = self._clean_category_name(parent_text)
                            if not parent_category:
                                continue
                            
                            # Hover over the item to reveal submenu
                            actions.move_to_element(item).perform()
                            time.sleep(0.7)  # Wait for dropdown
                            
                            # Look for submenu links
                            try:
                                # Find parent element (li)
                                parent_li = item.find_element(By.XPATH, "..")
                                
                                # Look for submenu within parent
                                submenu_selectors = [
                                    ".//ul",
                                    ".//*[contains(@class, 'sub')]",
                                    ".//*[contains(@class, 'dropdown')]",
                                    ".//*[contains(@class, 'child')]"
                                ]
                                
                                submenu = None
                                for sub_sel in submenu_selectors:
                                    try:
                                        submenu = parent_li.find_element(By.XPATH, sub_sel)
                                        if submenu and submenu.is_displayed():
                                            break
                                    except:
                                        continue
                                
                                if submenu and submenu.is_displayed():
                                    # Extract all links from submenu
                                    sublinks = submenu.find_elements(By.TAG_NAME, "a")
                                    
                                    for sublink in sublinks:
                                        try:
                                            if not sublink.is_displayed():
                                                continue
                                            
                                            sub_text = sublink.text.strip()
                                            sub_href = sublink.get_attribute("href")
                                            
                                            if not sub_text or not sub_href:
                                                continue
                                            
                                            # Clean subcategory name
                                            subcategory = self._clean_category_name(sub_text)
                                            if not subcategory or subcategory == parent_category:
                                                continue
                                            
                                            # Check if it's a valid category link
                                            if self._is_category_link(sub_href, sub_text):
                                                coll_key = f"{parent_category} > {subcategory}"
                                                
                                                if coll_key not in additional_collections:
                                                    additional_collections[coll_key] = {
                                                        'url': sub_href,
                                                        'category': parent_category,
                                                        'subcategory': subcategory
                                                    }
                                                    logger.info(f"✓ Found subcategory: {coll_key}")
                                        
                                        except Exception as e:
                                            logger.debug(f"Error processing sublink: {e}")
                                            continue
                            
                            except Exception as e:
                                logger.debug(f"Error finding submenu for '{parent_text}': {e}")
                                continue
                        
                        except Exception as e:
                            logger.debug(f"Error processing menu item: {e}")
                            continue
                    
                    # If we found subcategories with this selector, we're done
                    if additional_collections:
                        logger.info(f"Successfully extracted {len(additional_collections)} subcategories")
                        break
                        
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            if not additional_collections:
                logger.info("No dynamic subcategories found in navigation")
            
            return additional_collections
            
        except Exception as e:
            logger.warning(f"Error detecting dynamic submenus: {e}")
            return {}

