"""
Brand scraper using requests + BeautifulSoup
Primary scraping method - no API limits, fast, reliable
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
import urllib.robotparser
from typing import Dict, List
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class RequestsBrandScraper:
    """
    Brand scraper using requests and BeautifulSoup
    """
    
    def __init__(self, delay: float = 1.0, fetch_descriptions: bool = True):
        """
        Initialize scraper
        
        Args:
            delay: Delay between requests in seconds (be polite)
            fetch_descriptions: If True, visits each product page to get full description
        """
        self.delay = delay
        self.fetch_descriptions = fetch_descriptions
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
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
    
    def scrape_brand_website(self, website: str, brand_name: str, limit: int = 100) -> Dict:
        """
        Scrape entire brand website
        
        Args:
            website: Brand website URL
            brand_name: Name of the brand
            limit: Maximum number of products per category
            
        Returns:
            Dictionary with scraped data
        """
        logger.info(f"Starting scrape for {brand_name} at {website}")
        
        try:
            # Check robots.txt (non-blocking, just warning)
            if not self.check_robots_allowed(website):
                logger.warning(f"⚠️  Scraping not allowed by robots.txt for {website}, continuing anyway")
            
            # First, check if the site requires JavaScript
            js_required = self._detect_javascript_required(website)
            if js_required:
                logger.warning(f"⚠️  Site appears to require JavaScript. Consider using Selenium scraper for better results.")
                # Continue anyway, but log the warning
            # Get main page to find categories
            response = self.session.get(website, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find category links
            categories = self._find_categories(soup, website)
            logger.info(f"Found {len(categories)} categories")
            
            # Special handling for LAS.it and similar sites with typologies
            # If we're on /products/ page, look for typology links on that page
            if '/products' in website.lower() or '/product' in website.lower():
                typology_links = soup.find_all('a', href=re.compile(r'/typologies/', re.I))
                if typology_links:
                    logger.info(f"Found {len(typology_links)} typology links on products page")
                    for link in typology_links:
                        href = link.get('href', '').strip()
                        text = link.get_text(strip=True)
                        
                        # Skip "Find out more" and similar generic links
                        if not text or len(text) < 2 or text.lower() in ['find out more', 'read more', 'view', 'see more']:
                            # Try to get text from parent or nearby element
                            parent = link.find_parent(['div', 'article', 'section'])
                            if parent:
                                # Look for heading nearby
                                heading = parent.find(['h2', 'h3', 'h4', 'h5'])
                                if heading:
                                    text = heading.get_text(strip=True)
                        
                        if text and len(text) >= 2 and text.lower() not in ['find out more', 'read more']:
                            full_url = urljoin(website, href)
                            if text not in categories:
                                categories[text] = full_url
                                logger.info(f"Added typology category: {text} -> {full_url}")
            
            # If no categories found but we're on a products page, treat it as a single category
            if not categories and ('/products' in website.lower() or '/product' in website.lower()):
                logger.info("No categories found, treating current page as 'All Products' category")
                categories = {'All Products': website}
            
            # If still no categories, try to find product links directly on the page
            if not categories:
                logger.info("No categories found, looking for products directly on page")
                products = self._scrape_product_list(website, 'General', 'All', limit)
                if products:
                    category_tree = {
                        'General': {
                            'subcategories': {
                                'All': {'products': products}
                            }
                        }
                    }
                    return {
                        'success': True,
                        'brand': brand_name,
                        'website': website,
                        'category_tree': category_tree,
                        'total_products': len(products),
                        'requires_javascript': js_required,
                        'categories_found': 1
                    }
            
            # Scrape each category
            all_products = {}
            category_tree = {}
            
            for category_name, category_url in categories.items():
                logger.info(f"Scraping category: {category_name}")
                
                # Get subcategories
                subcategories = self._find_subcategories(category_url, category_name)
                
                if subcategories:
                    # Has subcategories
                    category_tree[category_name] = {'subcategories': {}}
                    
                    for subcat_name, subcat_url in subcategories.items():
                        logger.info(f"  Scraping subcategory: {subcat_name}")
                        products = self._scrape_product_list(subcat_url, category_name, subcat_name, limit)
                        
                        if products:
                            category_tree[category_name]['subcategories'][subcat_name] = {
                                'products': products
                            }
                        
                        time.sleep(self.delay)
                else:
                    # No subcategories, scrape category directly
                    products = self._scrape_product_list(category_url, category_name, 'General', limit)
                    
                    if products:
                        category_tree[category_name] = {
                            'subcategories': {
                                'General': {'products': products}
                            }
                        }
                
                time.sleep(self.delay)
            
            total_products = sum(
                len(sub.get('products', []))
                for cat in category_tree.values()
                for sub in cat.get('subcategories', {}).values()
            )
            
            # Count categories found
            categories_found = len(category_tree)
            
            return {
                'success': True,
                'brand': brand_name,
                'website': website,
                'category_tree': category_tree,
                'total_products': total_products,
                'requires_javascript': js_required,  # Include JS detection flag
                'categories_found': categories_found  # Include category count
            }
            
        except Exception as e:
            logger.error(f"Error scraping {brand_name}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _find_categories(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """Find main product categories from homepage - dynamically detects various website structures"""
        categories = {}
        seen_urls = set()
        
        # Skip common non-category navigation items
        skip_texts = ['home', 'about', 'contact', 'blog', 'news', 'login', 'register', 
                     'cart', 'checkout', 'account', 'search', 'menu', 'close', 'more',
                     'view all', 'see all', 'all products', 'shop', 'store', 'back',
                     'reserved area', 'download', 'projects', 'designer', 'company', 'history',
                     'environment', 'certifications', 'showroom', 'whistleblowing', 'find out more',
                     'share by', 'share', 'mail', 'email']
        
        # Strategy 1: Look for navigation menus with common patterns
        nav_patterns = [
            ('nav', {}),
            ('nav', {'class': lambda x: x and any(c in str(x).lower() for c in ['menu', 'nav', 'category', 'header'])}),
            ('ul', {'class': lambda x: x and any(c in str(x).lower() for c in ['menu', 'nav', 'category'])}),
            ('div', {'class': lambda x: x and any(c in str(x).lower() for c in ['menu', 'nav', 'navigation', 'header-menu'])}),
            ('header', {}),
        ]
        
        for tag, attrs in nav_patterns:
            nav_elements = soup.find_all(tag, attrs) if attrs else soup.find_all(tag)
            
            for nav in nav_elements:
                links = nav.find_all('a', href=True)
                
                for link in links:
                    href = link.get('href', '').strip()
                    text = link.get_text(strip=True)
                    
                    if not href or not text or len(text) < 2:
                        continue
                    
                    # Skip common non-category items
                    if text.lower() in skip_texts:
                        continue
                    
                    # Build full URL
                    full_url = urljoin(base_url, href)
                    
                    # Skip if already seen or is homepage
                    if full_url in seen_urls or full_url.rstrip('/') == base_url.rstrip('/'):
                        continue
                    
                    # Skip external links
                    parsed_base = urlparse(base_url)
                    parsed_link = urlparse(full_url)
                    if parsed_link.netloc and parsed_link.netloc != parsed_base.netloc:
                        continue
                    
                    # Detect category URLs - multiple patterns
                    url_path = parsed_link.path.lower()
                    
                    # Pattern 1: WooCommerce (/product-category/)
                    if '/product-category/' in url_path:
                        url_parts = url_path.split('/product-category/')[-1].split('/')
                        if len(url_parts) <= 2:  # Main category only
                            seen_urls.add(full_url)
                            categories[text] = full_url
                            continue
                    
                    # Pattern 2: Generic product/category patterns
                    category_indicators = ['/products/', '/product/', '/category/', '/categories/', 
                                        '/catalog/', '/collections/', '/collection/']
                    product_indicators = ['/product/', '/item/', '/p/', '/detail/']
                    
                    # Check if it's a category page (not a specific product)
                    is_category = any(ind in url_path for ind in category_indicators)
                    is_product = any(ind in url_path and '/product/' not in url_path.replace(ind, '')[:20] 
                                   for ind in product_indicators)
                    
                    # Pattern 3: LAS.it style - links in navigation that go to /en/products/ or similar
                    if '/products' in url_path or '/product' in url_path:
                        # Check if it's a category listing page (not a specific product)
                        # Products usually have longer paths or IDs
                        path_depth = len([p for p in url_path.split('/') if p])
                        if path_depth <= 3:  # Shallow path = likely category
                            seen_urls.add(full_url)
                            categories[text] = full_url
                            continue
                    
                    # Pattern 3b: LAS.it typologies pattern - /typologies/furniture/, /typologies/seating/, etc.
                    if '/typologies/' in url_path:
                        # This is definitely a category page
                        seen_urls.add(full_url)
                        categories[text] = full_url
                        continue
                    
                    # Pattern 4: If link is in navigation and has reasonable text, treat as category
                    if is_category and not is_product:
                        seen_urls.add(full_url)
                        categories[text] = full_url
                        continue
                    
                    # Pattern 5: Check if URL contains category-like keywords
                    category_keywords = ['furniture', 'seating', 'chair', 'desk', 'table', 'sofa', 
                                       'storage', 'office', 'wall', 'accessory']
                    if any(kw in url_path for kw in category_keywords) and path_depth <= 3:
                        seen_urls.add(full_url)
                        categories[text] = full_url
                        continue
        
        # Strategy 2: If we're on a products page, try to find category links on that page
        if '/products' in base_url.lower() or '/product' in base_url.lower():
            logger.info("Detected products page, looking for category links on page")
            category_links = soup.find_all('a', href=True)
            
            for link in category_links:
                href = link.get('href', '').strip()
                text = link.get_text(strip=True)
                
                if not href or not text or len(text) < 2:
                    continue
                
                if text.lower() in skip_texts:
                    continue
                
                full_url = urljoin(base_url, href)
                if full_url in seen_urls:
                    continue
                
                # Look for links that might be categories
                parsed_link = urlparse(full_url)
                url_path = parsed_link.path.lower()
                
                # Check if it's a category-style link
                if any(ind in url_path for ind in category_indicators) or '/products' in url_path:
                    # Make sure it's not too deep (products are usually deeper)
                    path_depth = len([p for p in url_path.split('/') if p])
                    if path_depth <= 4 and text not in categories:
                        seen_urls.add(full_url)
                        categories[text] = full_url
        
        # Strategy 3: Look for category cards/sections on homepage
        category_card_patterns = [
            ('div', {'class': lambda x: x and any(c in str(x).lower() for c in ['category', 'collection', 'product-cat'])}),
            ('article', {'class': lambda x: x and 'category' in str(x).lower()}),
        ]
        
        for tag, attrs in category_card_patterns:
            cards = soup.find_all(tag, attrs)
            for card in cards:
                link = card.find('a', href=True)
                if link:
                    href = link.get('href', '').strip()
                    text = link.get_text(strip=True) or card.get_text(strip=True)
                    
                    if not href or not text or len(text) < 2:
                        continue
                    
                    if text.lower() in skip_texts:
                        continue
                    
                    full_url = urljoin(base_url, href)
                    if full_url not in seen_urls and text not in categories:
                        seen_urls.add(full_url)
                        categories[text] = full_url
        
        logger.info(f"Found {len(categories)} categories: {list(categories.keys())}")
        return categories
    
    def _find_subcategories(self, category_url: str, category_name: str) -> Dict[str, str]:
        """Find subcategories within a category"""
        subcategories = {}
        
        try:
            response = self.session.get(category_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check if page requires JavaScript (has many containers but few links)
            product_containers = soup.find_all(['div', 'article', 'li'], 
                                             class_=lambda x: x and any(c in str(x).lower() for c in ['product', 'item', 'card']))
            all_links = soup.find_all('a', href=True)
            product_links = [link for link in all_links 
                           if any(p in link.get('href', '').lower() for p in ['/product/', '/item/']) 
                           and '/typologies/' not in link.get('href', '').lower()]
            
            # If we have many containers but few product links, likely JS-rendered
            if len(product_containers) > 10 and len(product_links) < 5:
                logger.warning(f"Category {category_name} appears to require JavaScript (found {len(product_containers)} containers but only {len(product_links)} product links)")
                # Return empty to trigger fallback or indicate JS needed
                return {}
            
            # Enhanced subcategory detection - multiple strategies
            # Strategy 1: Look for subcategory listings on category page
            subcats_from_page = self._detect_subcategories_on_page(soup, category_url, category_name)
            subcategories.update(subcats_from_page)
            
            # Strategy 2: Look for subcategory links in navigation/filters
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href', '').strip()
                text = link.get_text(strip=True)
                
                if not href or not text or len(text) < 2:
                    continue
                
                # Check if it's a subcategory of current category
                if category_url in href and href != category_url:
                    # Check if URL pattern suggests subcategory
                    if '/product-category/' in href or '/category/' in href:
                        clean_name = self._clean_category_name(text)
                        if clean_name and clean_name != category_name and clean_name not in subcategories:
                            subcategories[clean_name] = href
            
            return subcategories
            
        except Exception as e:
            logger.warning(f"Error finding subcategories for {category_name}: {e}")
            return {}
    
    def _scrape_product_list(self, url: str, category: str, subcategory: str, limit: int) -> List[Dict]:
        """Scrape products from a category/subcategory page with pagination support"""
        products = []
        page = 1
        seen_urls = set()
        
        while len(products) < limit:
            try:
                # Handle pagination - common patterns
                page_url = url
                if page > 1:
                    if '?' in url:
                        page_url = f"{url}&paged={page}"
                    else:
                        page_url = f"{url}?paged={page}" if not url.endswith('/') else f"{url}page/{page}/"
                
                logger.info(f"Scraping page {page}: {page_url}")
                response = self.session.get(page_url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find product links - multiple patterns for different website structures
                product_selectors = [
                    # WooCommerce patterns
                    'a.woocommerce-LoopProduct-link',
                    'a.product-link',
                    'h2.woocommerce-loop-product__title a',
                    'a[href*="/product/"]',
                    # Generic product patterns
                    'article.product a',
                    'div.product-item a',
                    'div.product-card a',
                    'li.product a',
                    'a[href*="/item/"]',
                    'a[href*="/detail/"]',
                    # LAS.it and similar patterns
                    'a[href*="/products/"]',
                    'div.product a',
                    'article a[href*="/product"]',
                ]
                
                product_links = []
                for selector in product_selectors:
                    try:
                        found = soup.select(selector)
                        if found:
                            product_links.extend(found)
                            logger.debug(f"Found {len(found)} products using selector: {selector}")
                    except Exception as e:
                        logger.debug(f"Selector {selector} failed: {e}")
                        continue
                
                # Fallback: find all links that look like product pages
                if not product_links:
                    all_links = soup.find_all('a', href=True)
                    for link in all_links:
                        href = link.get('href', '')
                        
                        # Skip non-http links
                        if href.startswith('mailto:') or href.startswith('tel:') or href.startswith('javascript:'):
                            continue
                        
                        # Look for product-like URLs but exclude category URLs
                        if any(pattern in href.lower() for pattern in ['/product/', '/item/', '/detail/', '/p/']):
                            if '/product-category/' not in href.lower() and '/category/' not in href.lower() and '/typologies/' not in href.lower():
                                product_links.append(link)
                        # Also check for products in /products/ path (LAS.it style)
                        elif '/products/' in href.lower():
                            # Make sure it's not just the category page and not a typology
                            path_parts = [p for p in href.split('/') if p]
                            if len(path_parts) > 3 and '/typologies/' not in href.lower():  # Deeper path = likely a product
                                product_links.append(link)
                        # LAS.it specific: products might be in /typologies/[category]/[product]/
                        elif '/typologies/' in href.lower():
                            path_parts = [p for p in href.split('/') if p]
                            # If it has more than typologies/category, it's likely a product
                            typology_index = [i for i, p in enumerate(path_parts) if 'typologies' in p.lower()]
                            if typology_index and len(path_parts) > typology_index[0] + 2:
                                product_links.append(link)
                
                # If no product links found, try to extract from containers
                if not product_links:
                    logger.info(f"No product links found, trying to extract from containers...")
                    # Look for product containers that might have product info
                    containers = soup.find_all(['div', 'article', 'li'], 
                                              class_=lambda x: x and any(c in str(x).lower() for c in ['product', 'item', 'card']))
                    
                    for container in containers[:limit]:
                        # Try to find any link in container
                        link = container.find('a', href=True)
                        if link:
                            href = link.get('href', '')
                            # Skip if it's a category link
                            if '/typologies/' in href.lower() or '/category/' in href.lower():
                                continue
                            product_links.append(link)
                    
                    # If still no links, try to extract product info from containers directly
                    if not product_links and len(containers) > 0:
                        logger.warning(f"Found {len(containers)} product containers but no links - page likely requires JavaScript")
                        # Return what we have so far
                        break
                
                # If no products found, we've reached the end
                if not product_links:
                    logger.info(f"No more products found on page {page}")
                    break
                
                page_products_count = 0
                
                for link in product_links:
                    if len(products) >= limit:
                        break
                    
                    product_url = link.get('href')
                    if not product_url:
                        continue
                    
                    # Make absolute URL
                    if product_url.startswith('/'):
                        parsed = urlparse(url)
                        product_url = f"{parsed.scheme}://{parsed.netloc}{product_url}"
                    
                    # Skip if already seen
                    if product_url in seen_urls:
                        continue
                    
                    seen_urls.add(product_url)
                    
                    # Get product name - try multiple strategies
                    product_name = None
                    
                    # Strategy 1: Link text
                    product_name = link.get_text(strip=True)
                    
                    # Strategy 2: Title attribute
                    if not product_name or len(product_name) < 3:
                        product_name = link.get('title', '')
                    
                    # Strategy 3: Look for heading or product name element nearby
                    if not product_name or len(product_name) < 3:
                        # Check parent or sibling elements
                        parent = link.find_parent(['div', 'article', 'li'])
                        if parent:
                            # Look for headings
                            heading = parent.find(['h2', 'h3', 'h4', 'h5'])
                            if heading:
                                product_name = heading.get_text(strip=True)
                            # Look for product name class
                            if not product_name:
                                name_elem = parent.find(['span', 'div'], class_=re.compile(r'(name|title|product.*name)', re.I))
                                if name_elem:
                                    product_name = name_elem.get_text(strip=True)
                    
                    # Strategy 4: Extract from URL if still no name
                    if not product_name or len(product_name) < 3:
                        # Try to extract name from URL slug
                        url_parts = [p for p in product_url.split('/') if p]
                        if url_parts:
                            last_part = url_parts[-1].replace('-', ' ').replace('_', ' ')
                            if len(last_part) > 3:
                                product_name = last_part.title()
                    
                    # Clean product name
                    if product_name:
                        product_name = re.sub(r'\s+', ' ', product_name).strip()
                    
                    # Skip if still no valid name
                    if not product_name or len(product_name) < 3:
                        continue
                    
                    # Skip navigation items
                    if any(skip in product_name.lower() for skip in ['add to', 'cart', 'wishlist', 'home', 'showing', 
                                                                     'filter', 'sort', 'categories', 'read more', 
                                                                     'view', 'learn more', 'find out more', 'share',
                                                                     'whistleblowing', 'mail', 'email', 'download']):
                        continue
                    
                    # Skip mailto: and other non-http links
                    if product_url.startswith('mailto:') or product_url.startswith('tel:') or product_url.startswith('javascript:'):
                        continue
                    
                    # Find associated image
                    img = link.find('img')
                    image_url = None
                    if img:
                        image_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    
                    # Create product entry
                    product = {
                        'name': product_name,
                        'description': '',
                        'image_url': image_url,
                        'source_url': product_url,
                        'brand': category.split()[0] if category else 'Unknown',
                        'price': None,
                        'price_range': 'Contact for price',
                        'features': [],
                        'specifications': {},
                        'category_path': [category, subcategory]
                    }
                    
                    # Fetch detailed product info if enabled
                    if self.fetch_descriptions:
                        self._enrich_product_details(product)
                    
                    products.append(product)
                    page_products_count += 1
                
                logger.info(f"Page {page}: Found {page_products_count} products (Total: {len(products)})")
                
                # If no new products on this page, stop pagination
                if page_products_count == 0:
                    break
                
                page += 1
                time.sleep(self.delay)
                
            except Exception as e:
                logger.error(f"Error scraping page {page} from {url}: {e}")
                break
        
        return products
    
    def _enrich_product_details(self, product: Dict):
        """Visit individual product page to get full description and details"""
        try:
            response = self.session.get(product['source_url'], timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract description - common WooCommerce patterns
            description = ''
            
            # Method 1: Try WC tabs wrapper (OTTIMO uses this)
            tabs_wrapper = soup.find('div', class_='wc-tabs-wrapper')
            if tabs_wrapper:
                text = tabs_wrapper.get_text(strip=True)
                # Extract description between "Description" and "Additional"
                if 'Description' in text:
                    desc_start = text.find('Description') + len('Description')
                    desc_end = text.find('Additional', desc_start)
                    if desc_end > desc_start:
                        description = text[desc_start:desc_end].strip()
                    else:
                        description = text[desc_start:desc_start+500].strip()
            
            # Method 2: Try various description selectors
            if not description:
                desc_selectors = [
                    'div.woocommerce-product-details__short-description',
                    'div.product-short-description',
                    'div[itemprop="description"]',
                    'div.entry-summary p',
                    'div.summary p',
                    # LAS.it specific patterns
                    'div.entry-content',
                    'div.product-content',
                    'div.content p',
                    'article p',
                    'section.product-description',
                    'div.description'
                ]
                
                for selector in desc_selectors:
                    desc_elem = soup.select_one(selector)
                    if desc_elem:
                        # Remove script and style tags
                        for tag in desc_elem.find_all(['script', 'style', 'nav', 'header', 'footer']):
                            tag.decompose()
                        description = desc_elem.get_text(separator=' ', strip=True)
                        if description and len(description) > 20:
                            break
            
            # Method 3: Try full description tab
            if not description:
                full_desc_selectors = [
                    'div#tab-description',
                    'div.woocommerce-Tabs-panel--description',
                    'div.product-description'
                ]
                
                for selector in full_desc_selectors:
                    desc_elem = soup.select_one(selector)
                    if desc_elem:
                        # Get text from paragraphs
                        paragraphs = desc_elem.find_all('p')
                        if paragraphs:
                            description = ' '.join(p.get_text(strip=True) for p in paragraphs[:3])
                        else:
                            description = desc_elem.get_text(strip=True)[:500]
                        break
            
            # Clean up description
            if description:
                description = re.sub(r'\s+', ' ', description).strip()
                # Remove common unwanted text
                unwanted = ['add to cart', 'add to wishlist', 'share:', 'sku:', 'categories:', 'tags:']
                for unwanted_text in unwanted:
                    description = re.sub(unwanted_text, '', description, flags=re.IGNORECASE)
                description = description.strip()
            
            product['description'] = description
            
            # Try to extract price if available
            price_selectors = [
                'p.price span.amount',
                'span.woocommerce-Price-amount',
                'span.price-amount'
            ]
            
            for selector in price_selectors:
                price_elem = soup.select_one(selector)
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    product['price_range'] = price_text
                    break
            
            # Extract product features/specifications
            features = self._extract_product_features(soup)
            if features:
                product['features'] = features
            
            time.sleep(self.delay * 0.5)  # Half delay for product pages
            
        except Exception as e:
            logger.warning(f"Could not enrich product {product['name']}: {e}")
            # Continue without description if fetch fails
    
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
    
    def _detect_subcategories_on_page(self, soup: BeautifulSoup, base_url: str, parent_category: str) -> Dict[str, str]:
        """
        Detect subcategories listed on a category page
        Returns dict: {subcategory_name: subcategory_url}
        """
        subcategories = {}
        
        # Common patterns for subcategory listings on category pages
        selectors = [
            # WooCommerce product categories
            ('ul', {'class': re.compile(r'product.*categor', re.I)}),
            ('div', {'class': re.compile(r'product.*categor', re.I)}),
            # Generic category listings
            ('div', {'class': re.compile(r'categor.*list|categor.*grid', re.I)}),
            ('ul', {'class': re.compile(r'sub.*categor|child.*categor', re.I)}),
            # Sidebar categories
            ('aside', {'class': re.compile(r'categor|sidebar', re.I)}),
            ('div', {'class': re.compile(r'widget.*categor', re.I)}),
            # Filter/sidebar patterns
            ('div', {'class': re.compile(r'filter|facet', re.I)}),
        ]
        
        for tag, attrs in selectors:
            containers = soup.find_all(tag, attrs)
            for container in containers:
                # Find links in this container
                links = container.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '').strip()
                    name = link.get_text(strip=True)
                    
                    if not href or not name or len(name) < 2:
                        continue
                    
                    # Check if this looks like a subcategory URL
                    if '/product-category/' in href or '/category/' in href:
                        full_url = urljoin(base_url, href)
                        # Make sure it's not the same as the parent
                        if full_url != base_url:
                            clean_name = self._clean_category_name(name)
                            if clean_name and clean_name != parent_category:
                                subcategories[clean_name] = full_url
        
        return subcategories
    
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
    
    def _detect_javascript_required(self, website: str) -> bool:
        """Detect if website requires JavaScript rendering"""
        try:
            response = self.session.get(website, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check for common JavaScript framework indicators
            scripts = soup.find_all('script')
            js_indicators = [
                'react', 'vue', 'angular', 'next.js', 'nuxt',
                'data-react', 'data-vue', 'ng-', 'v-bind',
                'application/json', 'window.__INITIAL_STATE__'
            ]
            
            page_text = response.text.lower()
            for indicator in js_indicators:
                if indicator in page_text:
                    logger.info(f"Detected JavaScript framework: {indicator}")
                    return True
            
            # Check if page has minimal content (suggesting JS rendering)
            text_content = soup.get_text(strip=True)
            if len(text_content) < 500:
                logger.info("Page has minimal content, likely requires JavaScript")
                return True
            
            # Check for empty product containers (common in JS-rendered sites)
            product_containers = soup.find_all(['div', 'article', 'li'], 
                                               class_=re.compile(r'(product|item|card)', re.I))
            if len(product_containers) > 0:
                # Check if containers are empty (JS-rendered)
                empty_count = sum(1 for container in product_containers[:5] 
                                if len(container.get_text(strip=True)) < 10)
                if empty_count >= 3:
                    logger.info("Found empty product containers, likely requires JavaScript")
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error detecting JavaScript requirement: {e}")
            return False

