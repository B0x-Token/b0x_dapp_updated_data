#!/usr/bin/env python3
# mirror_data.py

import requests
import os
import hashlib
import json
import sys
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
        
        # Alternative source for comparison
        self.alt_base_url = "https://b0x-token.github.io/B0x_scripts_auto/mainnetB0x/"
        
    def test_server_availability(self):
        """Test if the source server is responding with content"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            # Check if we get actual content (not just a blank page)
            if len(response.content) < 100:  # Very small response likely means server issues
                print(f"⚠️  Server returned minimal content ({len(response.content)} bytes)")
                return False
                
            # Check for common error indicators
            content_lower = response.text.lower()
            error_indicators = ['error', '404', '500', 'not found', 'server error', 'maintenance']
            
            if any(indicator in content_lower for indicator in error_indicators):
                print(f"⚠️  Server appears to be showing error page")
                return False
                
            print(f"✓ Server is responding normally ({len(response.content)} bytes)")
            return True
            
        except Exception as e:
            print(f"⚠️  Server availability test failed: {e}")
            return False
    
    def get_file_hash(self, filepath):
        """Get MD5 hash of local file"""
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def compare_json_sources(self, filename):
        """Compare JSON files from both sources and return the one with latest_block_number"""
        primary_url = urljoin(self.base_url, filename)
        alt_url = urljoin(self.alt_base_url, filename)
        
        print(f"\n🔍 Comparing {filename} from both sources...")
        
        primary_data = None
        alt_data = None
        primary_block = None
        alt_block = None
        
        # Try to fetch from primary source
        try:
            response = self.session.get(primary_url, timeout=30)
            response.raise_for_status()
            primary_data = response.json()
            primary_block = primary_data.get('latest_block_number', 0)
            print(f"  Primary source: latest_block_number = {primary_block}")
        except Exception as e:
            print(f"  ⚠️  Primary source error: {e}")
        
        # Try to fetch from alternative source
        try:
            response = self.session.get(alt_url, timeout=30)
            response.raise_for_status()
            alt_data = response.json()
            alt_block = alt_data.get('latest_block_number', 0)
            print(f"  Alternative source: latest_block_number = {alt_block}")
        except Exception as e:
            print(f"  ⚠️  Alternative source error: {e}")
        
        # Determine which source to use
        if primary_data is None and alt_data is None:
            print(f"  ❌ Both sources failed for {filename}")
            return None, None
        elif primary_data is None:
            print(f"  ✓ Using alternative source (primary unavailable)")
            return alt_data, alt_url
        elif alt_data is None:
            print(f"  ✓ Using primary source (alternative unavailable)")
            return primary_data, primary_url
        elif alt_block > primary_block:
            print(f"  ✓ Using alternative source (block {alt_block} > {primary_block})")
            return alt_data, alt_url
        else:
            print(f"  ✓ Using primary source (block {primary_block} >= {alt_block})")
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
                print(f"⚠️  Skipping empty file: {url}")
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
                    print(f"✓ Downloaded: {os.path.basename(local_path)} ({len(content)} bytes)")
                    self.stats['downloaded'] += 1
                else:
                    print(f"↻ Updated: {os.path.basename(local_path)} ({len(content)} bytes)")
                    self.stats['updated'] += 1
                return True
            else:
                self.stats['skipped'] += 1
                return False
                
        except Exception as e:
            print(f"✗ Error saving {local_path}: {e}")
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
            print(f"⚠️  No files found in {url}")
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
                # Check if this is the special file that needs comparison
                if filename == 'uu_mined_blocks_testnet.json':
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
            f.write(f"""# Data Backup

This directory contains a backup mirror of [{self.base_url}]({self.base_url})

**Alternative Source:** [{self.alt_base_url}]({self.alt_base_url})

**Last Updated:** {index_data['last_updated']}

## ⚠️ Important Notes
- This is a backup mirror that only updates when the source server is available
- For `uu_mined_blocks_testnet.json`, the backup automatically selects whichever source has the highest `latest_block_number`
- If the source server is down, no changes will be made to preserve existing data
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
    
    print("🔄 Starting data backup mirror...")
    print(f"Primary Source: {mirror.base_url}")
    print(f"Alternative Source: {mirror.alt_base_url}")
    print(f"Target: {mirror.local_dir}/")
    print("-" * 60)
    
    # Test server availability first
    if not mirror.test_server_availability():
        print("❌ Source server is not available or returning errors")
        print("🛡️  Preserving existing backup - no changes made")
        mirror.create_status_file(success=False)
        sys.exit(0)
    
    # Create local directory
    os.makedirs(mirror.local_dir, exist_ok=True)
    
    # Mirror the directory
    mirror.mirror_directory(mirror.base_url)
    
    # Validate we found files
    if not mirror.files_found:
        print("❌ No files were found during mirroring")
        print("🛡️  This could indicate server issues - preserving existing backup")
        mirror.create_status_file(success=False)
        sys.exit(0)
    
    # Create index and README
    mirror.create_index()
    
    # Mark as successful
    mirror.create_status_file(success=True)
    
    print("-" * 60)
    print("✅ Backup mirror complete!")
    print(f"📁 Files found: {len(mirror.files_found)}")
    print(f"⬇️  Downloaded: {mirror.stats['downloaded']}")
    print(f"🔄 Updated: {mirror.stats['updated']}")
    print(f"⏭️  Skipped: {mirror.stats['skipped']}")
    print(f"❌ Errors: {mirror.stats['errors']}")

if __name__ == "__main__":
    main()
