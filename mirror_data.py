#!/usr/bin/env python3
# mirror_data.py

import requests
import os
import hashlib
import json
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

class DataMirror:
    def __init__(self, base_url="https://data.bzerox.org/mainnet/", local_dir="data"):
        self.base_url = base_url.rstrip('/') + '/'
        self.local_dir = local_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataMirror/1.0 (GitHub Backup Bot)'
        })
        self.stats = {
            'downloaded': 0,
            'updated': 0,
            'errors': 0,
            'skipped': 0
        }
        self.files_found = []
        
        # Alternative source for comparison and fallback
        self.alt_base_url = "https://b0x-token.github.io/B0x_scripts_auto/mainnetB0x/"
        self.primary_available = False
        self.alt_available = False
        
    def test_server_availability(self, url, name="Server", is_github_pages=False):
        """Test if a source server is responding with content"""
        max_retries = 3
        base_timeout = 10
        
        for attempt in range(max_retries):
            try:
                timeout = base_timeout * (attempt + 1)
                print(f"  Attempt {attempt + 1}/{max_retries} (timeout: {timeout}s)...", end=" ")
                
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                
                # Check if we get actual content
                if len(response.content) < 100:
                    print(f"minimal content ({len(response.content)} bytes)")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return False
                
                # For GitHub Pages, be more lenient with error detection
                if is_github_pages:
                    # Try to parse as directory listing
                    soup = BeautifulSoup(response.content, 'html.parser')
                    links = soup.find_all('a', href=True)
                    
                    # If we find file links, it's likely a valid directory
                    file_links = [link for link in links if '.' in link.get('href', '')]
                    if len(file_links) > 0:
                        print(f"valid directory with {len(file_links)} files ({len(response.content)} bytes)")
                        return True
                    
                    # Check if it's a GitHub 404 page (these have specific markers)
                    if 'github.io' in url.lower() and response.status_code == 404:
                        print("GitHub 404 page detected")
                        return False
                    
                    # If it has HTML structure but no obvious errors, consider it valid
                    if soup.find('html') and not soup.find(text=lambda t: '404' in str(t).lower()):
                        print(f"appears valid ({len(response.content)} bytes)")
                        return True
                else:
                    # For regular servers, check for actual HTTP error pages
                    # Only flag as error if we have HTTP error status OR obvious error content
                    content_lower = response.text.lower()
                    
                    # Only check for error indicators if status code suggests an error
                    if response.status_code >= 400:
                        print(f"HTTP error {response.status_code}")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                        return False
                    
                    # For successful status codes, check for obvious error pages
                    strict_error_indicators = ['<title>404', '<title>error', 'page not found', 'server error occurred']
                    if any(indicator in content_lower for indicator in strict_error_indicators):
                        print("error page detected")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                        return False
                
                print(f"responding normally ({len(response.content)} bytes)")
                return True
                
            except requests.exceptions.Timeout as e:
                print(f"timeout")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
            except requests.exceptions.ConnectionError as e:
                print(f"connection failed")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
            except Exception as e:
                print(f"error: {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        print(f"  {name} unavailable after {max_retries} attempts")
        return False
    
    def get_file_hash(self, filepath):
        """Get MD5 hash of local file"""
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()


    def compare_json_sources(self, filename):
        """Compare JSON files from both sources and return the one with the most recent block"""
        primary_url = urljoin(self.base_url, filename)
        alt_url = urljoin(self.alt_base_url, filename)
        
        print(f"\nComparing {filename} from both sources...")
        
        # Determine which field to check based on filename
        if filename == 'uniswap_v4_data_testnet.json':
            block_field = 'current_block'
        else:
            block_field = 'latest_block_number'
        
        primary_data = None
        alt_data = None
        primary_block = None
        alt_block = None
        
        # Try to fetch from primary source (if available)
        if self.primary_available:
            try:
                response = self.session.get(primary_url, timeout=30)
                response.raise_for_status()
                primary_data = response.json()
                primary_block = primary_data.get(block_field, 0)
                print(f"  Primary source: {block_field} = {primary_block}")
            except Exception as e:
                print(f"  Primary source error: {e}")
        else:
            print(f"  Primary source unavailable, skipping")
        
        # Try to fetch from alternative source (if available)
        if self.alt_available:
            try:
                response = self.session.get(alt_url, timeout=30)
                response.raise_for_status()
                alt_data = response.json()
                alt_block = alt_data.get(block_field, 0)
                print(f"  Alternative source: {block_field} = {alt_block}")
            except Exception as e:
                print(f"  Alternative source error: {e}")
        else:
            print(f"  Alternative source unavailable, skipping")
        
        # Determine which source to use
        if primary_data is None and alt_data is None:
            print(f"  Both sources failed for {filename}")
            return None, None
        elif primary_data is None:
            print(f"  Using alternative source (primary unavailable)")
            return alt_data, alt_url
        elif alt_data is None:
            print(f"  Using primary source (alternative unavailable)")
            return primary_data, primary_url
        elif alt_block > primary_block:
            print(f"  Using alternative source (block {alt_block} > {primary_block})")
            return alt_data, alt_url
        else:
            print(f"  Using primary source (block {primary_block} >= {alt_block})")
            return primary_data, primary_url

    
    def download_file(self, url, local_path, override_content=None):
        """Download a single file (or save override_content if provided)"""
        try:
            if override_content is not None:
                # Use provided content instead of downloading
                content = override_content
                if isinstance(content, dict):
                    content = json.dumps(content, indent=2).encode('utf-8')
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                content = response.content
            
            # Validate file content (make sure it's not empty or error page)
            if len(content) == 0:
                print(f"Skipping empty file: {url}")
                self.stats['skipped'] += 1
                return False
            
            # Create directory if needed
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Check if file changed (compare hash)
            new_hash = hashlib.md5(content).hexdigest()
            old_hash = self.get_file_hash(local_path)
            
            if old_hash != new_hash:
                with open(local_path, 'wb') as f:
                    f.write(content)
                
                if old_hash is None:
                    print(f"Downloaded: {os.path.basename(local_path)} ({len(content)} bytes)")
                    self.stats['downloaded'] += 1
                else:
                    print(f"Updated: {os.path.basename(local_path)} ({len(content)} bytes)")
                    self.stats['updated'] += 1
                return True
            else:
                self.stats['skipped'] += 1
                return False
                
        except Exception as e:
            print(f"Error saving {local_path}: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_directory_listing(self, url):
        """Get list of files from directory listing"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            files = []
            
            # Look for links that appear to be files
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Skip parent directory links and fragments
                if href in ['..', '../'] or href.startswith('#'):
                    continue
                    
                # If it's a relative link, make it absolute
                if not href.startswith('http'):
                    href = urljoin(url, href)
                
                # Check if it looks like a file (has extension) or subdirectory
                if '.' in os.path.basename(href) or href.endswith('/'):
                    files.append(href)
            
            return files
            
        except Exception as e:
            print(f"Error getting directory listing from {url}: {e}")
            return []
    
    def mirror_directory(self, url, local_subdir=""):
        """Recursively mirror a directory"""
        local_path = os.path.join(self.local_dir, local_subdir)
        
        print(f"Scanning: {url}")
        files = self.get_directory_listing(url)
        
        if not files:
            print(f"No files found in {url}")
            return
        
        for file_url in files:
            # Parse the file URL to get relative path
            parsed = urlparse(file_url)
            rel_path = parsed.path.replace(urlparse(self.base_url).path, '').lstrip('/')
            local_file_path = os.path.join(self.local_dir, rel_path)
            filename = os.path.basename(file_url)
            
            if file_url.endswith('/'):
                # It's a subdirectory, recurse
                self.mirror_directory(file_url, rel_path)
            else:
                # Check if this is a special file that needs comparison
                if filename in ['uu_mined_blocks_testnet.json', 'uniswap_v4_data_testnet.json']:
                    best_data, best_url = self.compare_json_sources(filename)
                    if best_data is not None:
                        self.files_found.append(best_url)
                        self.download_file(best_url, local_file_path, override_content=best_data)
                    else:
                        self.stats['errors'] += 1
                else:
                    # Regular file, download normally
                    self.files_found.append(file_url)
                    self.download_file(file_url, local_file_path)
    
    def mirror_from_alt_source(self):
        """Mirror comparison files from alternative source when primary is down"""
        print("\nAttempting to update comparison files from alternative source...")
        
        comparison_files = ['uu_mined_blocks_testnet.json', 'uniswap_v4_data_testnet.json']
        
        for filename in comparison_files:
            alt_url = urljoin(self.alt_base_url, filename)
            local_file_path = os.path.join(self.local_dir, filename)
            
            try:
                print(f"\nFetching {filename} from alternative source...")
                response = self.session.get(alt_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                block_num = data.get('latest_block_number', 'unknown')
                print(f"  Alternative source: latest_block_number = {block_num}")
                
                self.files_found.append(alt_url)
                self.download_file(alt_url, local_file_path, override_content=data)
                
            except Exception as e:
                print(f"  Error fetching {filename} from alternative source: {e}")
                self.stats['errors'] += 1
    
    def create_status_file(self, success=True):
        """Create status file for workflow"""
        status = "SUCCESS" if success else "FAILED"
        with open("mirror_status.txt", "w") as f:
            f.write(status)
    
    def create_index(self):
        """Create an index file with metadata"""
        index_data = {
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'source_url': self.base_url,
            'alternative_source_url': self.alt_base_url,
            'primary_available': self.primary_available,
            'alternative_available': self.alt_available,
            'stats': self.stats,
            'files': []
        }
        
        # Walk through all downloaded files
        for root, dirs, files in os.walk(self.local_dir):
            for file in files:
                if file in ['index.json', 'README.md']:
                    continue
                    
                filepath = os.path.join(root, file)
                rel_path = os.path.relpath(filepath, self.local_dir)
                
                stat_info = os.stat(filepath)
                index_data['files'].append({
                    'path': rel_path,
                    'size': stat_info.st_size,
                    'modified': datetime.fromtimestamp(stat_info.st_mtime).isoformat() + 'Z',
                    'md5': self.get_file_hash(filepath)
                })
        
        # Save index
        index_path = os.path.join(self.local_dir, 'index.json')
        with open(index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
            
        # Create README
        readme_path = os.path.join(self.local_dir, 'README.md')
        with open(readme_path, 'w') as f:
            primary_status = "Available" if self.primary_available else "Unavailable"
            alt_status = "Available" if self.alt_available else "Unavailable"
            
            f.write(f"""# Data Backup

This directory contains a backup mirror of [{self.base_url}]({self.base_url})

**Alternative Source:** [{self.alt_base_url}]({self.alt_base_url})

**Last Updated:** {index_data['last_updated']}

## Source Status (Latest Run)
- Primary Source: {primary_status}
- Alternative Source: {alt_status}

## Important Notes
- This is a backup mirror that only updates when at least one source server is available
- For `uu_mined_blocks_testnet.json` and `uniswap_v4_data_testnet.json`, the backup automatically selects whichever source has the highest `latest_block_number`
- If the primary source is down, the script will attempt to update comparison files from the alternative source
- If both sources are down, no changes will be made to preserve existing data
- Files are only updated when their content actually changes

## Statistics (Latest Run)
- Files Downloaded: {self.stats['downloaded']}
- Files Updated: {self.stats['updated']}
- Files Skipped (no changes): {self.stats['skipped']}
- Errors: {self.stats['errors']}
- Total Files in Backup: {len(index_data['files'])}

## Files in Backup
""")
            
            # Group files by extension
            files_by_ext = {}
            for file_info in index_data['files']:
                ext = os.path.splitext(file_info['path'])[1] or 'no extension'
                if ext not in files_by_ext:
                    files_by_ext[ext] = []
                files_by_ext[ext].append(file_info)
            
            for ext in sorted(files_by_ext.keys()):
                f.write(f"\n### {ext.upper()} Files\n")
                for file_info in sorted(files_by_ext[ext], key=lambda x: x['path']):
                    size_kb = file_info['size'] / 1024
                    f.write(f"- [`{file_info['path']}`]({file_info['path']}) ({size_kb:.1f} KB)\n")

def main():
    mirror = DataMirror()
    
    print("Starting data backup mirror...")
    print(f"Primary Source: {mirror.base_url}")
    print(f"Alternative Source: {mirror.alt_base_url}")
    print(f"Target: {mirror.local_dir}/")
    print("-" * 60)
    
    # Test both server availability
    print("\nTesting source availability...")
    print("Primary source:")
    mirror.primary_available = mirror.test_server_availability(
        mirror.base_url, 
        "Primary source",
        is_github_pages=False
    )
    
    print("\nAlternative source:")
    mirror.alt_available = mirror.test_server_availability(
        mirror.alt_base_url, 
        "Alternative source",
        is_github_pages=True
    )
    
    # Check if we can proceed
    if not mirror.primary_available and not mirror.alt_available:
        print("\nBoth sources are unavailable")
        print("Preserving existing backup - no changes made")
        mirror.create_status_file(success=False)
        sys.exit(0)
    
    # Create local directory
    os.makedirs(mirror.local_dir, exist_ok=True)
    
    # If primary is available, do full mirror
    if mirror.primary_available:
        print("\nPrimary source available - performing full mirror")
        mirror.mirror_directory(mirror.base_url)
    else:
        print("\nPrimary source unavailable - attempting partial update from alternative source")
        mirror.mirror_from_alt_source()
    
    # Validate we found files
    if not mirror.files_found:
        print("\nNo files were found during mirroring")
        print("This could indicate server issues - preserving existing backup")
        mirror.create_status_file(success=False)
        sys.exit(0)
    
    # Create index and README
    mirror.create_index()
    
    # Mark as successful
    mirror.create_status_file(success=True)
    
    print("-" * 60)
    print("Backup mirror complete!")
    print(f"Files found: {len(mirror.files_found)}")
    print(f"Downloaded: {mirror.stats['downloaded']}")
    print(f"Updated: {mirror.stats['updated']}")
    print(f"Skipped: {mirror.stats['skipped']}")
    print(f"Errors: {mirror.stats['errors']}")

if __name__ == "__main__":
    main()
