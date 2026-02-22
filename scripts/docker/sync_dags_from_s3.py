#!/usr/bin/env python3
"""
S3 DAG Sync Utility for Apache Airflow
======================================

This utility synchronizes DAG files from an S3-compatible storage bucket
to the Airflow DAGs folder. It supports AWS S3, MinIO, Yandex Object Storage,
and other S3-compatible services.

Environment Variables:
---------------------
S3_ENDPOINT_URL: S3 endpoint URL (optional, for non-AWS S3)
S3_BUCKET: S3 bucket name containing DAG files
S3_DAGS_PREFIX: Prefix/path within bucket where DAGs are stored (default: "")
S3_ACCESS_KEY_ID: AWS/S3 access key ID
S3_SECRET_ACCESS_KEY: AWS/S3 secret access key
S3_REGION: AWS/S3 region (default: us-east-1)
S3_VERIFY_SSL: Verify SSL certificates (default: true)
AIRFLOW_DAGS_FOLDER: Local DAGs folder path (default: /opt/airflow/dags)
S3_SYNC_INTERVAL_SECONDS: Sync interval in seconds (default: 300)
S3_DELETE_EXTRA_LOCAL: Delete local files not in S3 (default: false)
S3_DOWNLOAD_ON_STARTUP: Download all DAGs on first run (default: true)
"""

import os
import sys
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Set, Tuple
import argparse

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except ImportError:
    print("ERROR: boto3 is not installed. Please install it with: pip install boto3")
    sys.exit(1)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("s3-dag-sync")


class S3DAGSync:
    """S3 DAG synchronization utility"""

    def __init__(self):
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL")
        self.bucket_name = os.getenv("S3_BUCKET")
        self.dags_prefix = os.getenv("S3_DAGS_PREFIX", "").lstrip("/")
        self.access_key = os.getenv("S3_ACCESS_KEY_ID")
        self.secret_key = os.getenv("S3_SECRET_ACCESS_KEY")
        self.region = os.getenv("S3_REGION", "us-east-1")
        self.verify_ssl = os.getenv("S3_VERIFY_SSL", "true").lower() == "true"
        self.dags_folder = Path(os.getenv("AIRFLOW_DAGS_FOLDER", "/opt/airflow/dags"))
        self.sync_interval = int(os.getenv("S3_SYNC_INTERVAL_SECONDS", "300"))
        self.delete_extra = os.getenv("S3_DELETE_EXTRA_LOCAL", "false").lower() == "true"
        self.download_on_startup = os.getenv("S3_DOWNLOAD_ON_STARTUP", "true").lower() == "true"

        # Validate required environment variables
        if not self.bucket_name:
            raise ValueError("S3_BUCKET environment variable is required")
        if not self.access_key or not self.secret_key:
            raise ValueError("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY environment variables are required")

        self.s3_client = self._create_s3_client()
        logger.info(f"Initialized S3 DAG Sync for bucket: {self.bucket_name}")
        logger.info(f"DAGs prefix: '{self.dags_prefix}'")
        logger.info(f"Local DAGs folder: {self.dags_folder}")
        logger.info(f"Sync interval: {self.sync_interval} seconds")
        logger.info(f"Delete extra local files: {self.delete_extra}")

    def _create_s3_client(self):
        """Create and return S3 client with configured credentials"""
        session_config = {
            'aws_access_key_id': self.access_key,
            'aws_secret_access_key': self.secret_key,
            'region_name': self.region,
        }

        if self.endpoint_url:
            session_config['endpoint_url'] = self.endpoint_url
            logger.info(f"Using custom S3 endpoint: {self.endpoint_url}")
        else:
            logger.info("Using AWS S3 endpoint")

        # Create config with connection settings
        config = Config(
            connect_timeout=30,
            read_timeout=30,
            retries={'max_attempts': 3}
        )

        # For SSL verification, pass it directly to boto3.client()
        if not self.verify_ssl:
            # Suppress SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session_config['verify'] = False

        return boto3.client('s3', **session_config, config=config)

    def _get_s3_objects(self) -> Dict[str, str]:
        """Get all objects from S3 bucket with their ETags"""
        objects = {}
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            kwargs = {'Bucket': self.bucket_name}
            if self.dags_prefix:
                kwargs['Prefix'] = self.dags_prefix
            
            for page in paginator.paginate(**kwargs):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Get relative path after prefix
                        if self.dags_prefix and key.startswith(self.dags_prefix + "/"):
                            rel_path = key[len(self.dags_prefix) + 1:]
                        elif self.dags_prefix == "" or key == self.dags_prefix:
                            rel_path = key
                        else:
                            continue
                        
                        # Only include files (not directories)
                        if rel_path and not rel_path.endswith("/"):
                            objects[rel_path] = obj['ETag'].strip('"')
            
            logger.info(f"Found {len(objects)} DAG files in S3")
            return objects
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to list S3 objects: {e}")
            raise

    def _get_local_files(self) -> Dict[str, str]:
        """Get all local DAG files with their MD5 hashes"""
        files = {}
        
        if not self.dags_folder.exists():
            logger.warning(f"Local DAGs folder does not exist: {self.dags_folder}")
            return files
        
        for file_path in self.dags_folder.rglob("*.py"):
            rel_path = str(file_path.relative_to(self.dags_folder))
            try:
                with open(file_path, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()
                files[rel_path] = file_hash
            except Exception as e:
                logger.warning(f"Failed to hash file {rel_path}: {e}")
        
        logger.info(f"Found {len(files)} local DAG files")
        return files

    def _download_file(self, s3_key: str, local_path: Path):
        """Download a single file from S3"""
        try:
            # Create parent directories if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            logger.debug(f"Downloaded: {s3_key}")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            raise

    def _sync_once(self) -> Tuple[int, int, int]:
        """
        Perform a single sync operation
        
        Returns:
            Tuple of (downloaded_count, updated_count, deleted_count)
        """
        logger.info("=" * 60)
        logger.info("Starting DAG synchronization")
        
        # Get S3 and local file lists
        s3_objects = self._get_s3_objects()
        local_files = self._get_local_files()
        
        # Build S3 key for each relative path
        s3_keys = {}
        for rel_path, etag in s3_objects.items():
            if self.dags_prefix:
                s3_key = f"{self.dags_prefix}/{rel_path}" if rel_path else self.dags_prefix
            else:
                s3_key = rel_path
            s3_keys[rel_path] = s3_key
        
        # Determine what to download/update
        to_download = set()
        to_update = set()
        
        for rel_path, s3_etag in s3_objects.items():
            if rel_path not in local_files:
                to_download.add(rel_path)
            elif local_files[rel_path] != s3_etag:
                to_update.add(rel_path)
        
        # Determine what to delete
        to_delete = set()
        if self.delete_extra:
            to_delete = set(local_files.keys()) - set(s3_objects.keys())
        
        # Execute changes
        downloaded_count = 0
        updated_count = 0
        deleted_count = 0
        
        # Download new files
        for rel_path in to_download:
            try:
                s3_key = s3_keys[rel_path]
                local_path = self.dags_folder / rel_path
                self._download_file(s3_key, local_path)
                downloaded_count += 1
                logger.info(f"[NEW] {rel_path}")
            except Exception as e:
                logger.error(f"Failed to download {rel_path}: {e}")
        
        # Update changed files
        for rel_path in to_update:
            try:
                s3_key = s3_keys[rel_path]
                local_path = self.dags_folder / rel_path
                self._download_file(s3_key, local_path)
                updated_count += 1
                logger.info(f"[UPDATE] {rel_path}")
            except Exception as e:
                logger.error(f"Failed to update {rel_path}: {e}")
        
        # Delete extra files
        for rel_path in to_delete:
            try:
                local_path = self.dags_folder / rel_path
                local_path.unlink()
                deleted_count += 1
                logger.info(f"[DELETE] {rel_path}")
            except Exception as e:
                logger.error(f"Failed to delete {rel_path}: {e}")
        
        logger.info("-" * 60)
        logger.info(f"Sync completed: {downloaded_count} new, {updated_count} updated, {deleted_count} deleted")
        
        return downloaded_count, updated_count, deleted_count

    def test_connection(self):
        """Test S3 connection and configuration"""
        logger.info("Testing S3 connection...")
        try:
            # Check if bucket exists and is accessible
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✓ Successfully connected to bucket: {self.bucket_name}")
            
            # List objects to verify permissions
            objects = self._get_s3_objects()
            logger.info(f"✓ Successfully listed {len(objects)} objects in bucket")
            
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                logger.error(f"✗ Bucket not found: {self.bucket_name}")
            elif error_code == '403':
                logger.error(f"✗ Access denied to bucket: {self.bucket_name}")
            else:
                logger.error(f"✗ Connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Connection test failed: {e}")
            return False

    def run_once(self):
        """Run sync once and exit"""
        logger.info("Running single sync operation...")
        self._sync_once()
        logger.info("Single sync operation completed")

    def run_daemon(self):
        """Run sync daemon with scheduled intervals"""
        logger.info("Starting S3 DAG sync daemon...")
        logger.info(f"Press Ctrl+C to stop")
        
        # Initial sync on startup
        if self.download_on_startup:
            logger.info("Performing initial sync on startup...")
            self._sync_once()
        
        # Scheduled sync loop
        try:
            while True:
                logger.info(f"Next sync in {self.sync_interval} seconds...")
                time.sleep(self.sync_interval)
                
                try:
                    self._sync_once()
                except Exception as e:
                    logger.error(f"Sync failed: {e}", exc_info=True)
        except KeyboardInterrupt:
            logger.info("Shutting down S3 DAG sync daemon...")
            sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="S3 DAG Sync Utility for Apache Airflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test S3 connection and exit'
    )
    
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run sync once and exit'
    )
    
    args = parser.parse_args()
    
    try:
        sync = S3DAGSync()
        
        if args.test:
            success = sync.test_connection()
            sys.exit(0 if success else 1)
        elif args.once:
            sync.run_once()
        else:
            sync.run_daemon()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()