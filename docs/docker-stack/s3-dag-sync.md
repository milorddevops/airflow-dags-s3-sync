# S3 DAG Sync Utility for Apache Airflow

This document describes the S3 DAG synchronization utility that synchronizes DAG files from S3-compatible storage to the Airflow DAGs folder.

## Overview

The S3 DAG sync utility (`sync-dags-from-s3`) automatically synchronizes DAG files from an S3 bucket to the Airflow DAGs directory. It supports:

- **AWS S3** - Amazon Web Services S3
- **MinIO** - Self-hosted S3-compatible object storage
- **Yandex Object Storage** - Yandex Cloud's S3-compatible service
- **Any S3-compatible storage** - Any service that implements the S3 API

## Features

- ✅ Automatic synchronization at configurable intervals
- ✅ Incremental updates (only downloads changed files)
- ✅ Support for multiple S3 providers
- ✅ Connection testing and validation
- ✅ Optional deletion of local files not in S3
- ✅ Comprehensive logging
- ✅ Runs as a daemon or one-time sync

## Installation

The utility is included in both the production (`Dockerfile`) and CI (`Dockerfile.ci`) Airflow Docker images. It's located at `/usr/local/bin/sync-dags-from-s3`.

### Verify Installation

```bash
docker run --rm apache/airflow:latest sync-dags-from-s3 --help
```

## Environment Variables

Configure the utility using the following environment variables:

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `S3_BUCKET` | S3 bucket name containing DAG files | `my-airflow-dags` |
| `S3_ACCESS_KEY_ID` | S3 access key ID | `AKIAIOSFODNN7EXAMPLE` |
| `S3_SECRET_ACCESS_KEY` | S3 secret access key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |

### Optional Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S3_ENDPOINT_URL` | Custom S3 endpoint URL (for non-AWS S3) | AWS default | `https://storage.yandexcloud.net` |
| `S3_DAGS_PREFIX` | Prefix/path within bucket where DAGs are stored | `""` (root) | `production/` or `dags/` |
| `S3_REGION` | AWS/S3 region | `us-east-1` | `ru-central1` |
| `S3_VERIFY_SSL` | Verify SSL certificates | `true` | `false` |
| `AIRFLOW_DAGS_FOLDER` | Local DAGs folder path | `/opt/airflow/dags` | `/dags` |
| `S3_SYNC_INTERVAL_SECONDS` | Sync interval in seconds | `300` (5 min) | `60` |
| `S3_DELETE_EXTRA_LOCAL` | Delete local files not in S3 | `false` | `true` |
| `S3_DOWNLOAD_ON_STARTUP` | Download all DAGs on first run | `true` | `false` |

## Configuration Examples

### AWS S3 Configuration

```yaml
# docker-compose.yml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:latest
    environment:
      # AWS S3 Configuration
      S3_BUCKET: my-airflow-dags
      S3_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      S3_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      S3_REGION: us-east-1
      
      # Optional: Sync every 60 seconds
      S3_SYNC_INTERVAL_SECONDS: 60
```

### MinIO Configuration

```yaml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:latest
    environment:
      # MinIO Configuration
      S3_BUCKET: airflow-dags
      S3_ACCESS_KEY_ID: minioadmin
      S3_SECRET_ACCESS_KEY: minioadmin
      S3_ENDPOINT_URL: http://minio:9000
      S3_REGION: us-east-1
      S3_VERIFY_SSL: false  # Disable SSL for self-signed certificates
      
      # Optional: Use prefix to organize DAGs
      S3_DAGS_PREFIX: production/
      
  minio:
    image: minio/minio
    command: server /data
```

### Yandex Object Storage Configuration

```yaml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:latest
    environment:
      # Yandex Object Storage Configuration
      S3_BUCKET: my-airflow-dags
      S3_ACCESS_KEY_ID: ${YC_ACCESS_KEY_ID}
      S3_SECRET_ACCESS_KEY: ${YC_SECRET_ACCESS_KEY}
      S3_ENDPOINT_URL: https://storage.yandexcloud.net
      S3_REGION: ru-central1
```

### Ceph/RGW Configuration

```yaml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:latest
    environment:
      # Ceph RGW Configuration
      S3_BUCKET: airflow-dags
      S3_ACCESS_KEY_ID: ${CEPH_ACCESS_KEY}
      S3_SECRET_ACCESS_KEY: ${CEPH_SECRET_KEY}
      S3_ENDPOINT_URL: http://ceph-rgw:7480
      S3_REGION: default
      S3_VERIFY_SSL: false
```

## Usage

### Running as a Daemon

The utility runs as a daemon, synchronizing files at the interval specified by `S3_SYNC_INTERVAL_SECONDS`.

```bash
# Start the daemon (runs in foreground)
sync-dags-from-s3

# Or run it in the background
sync-dags-from-s3 &

# Or integrate with your container startup
# In your entrypoint script or docker-compose:
command: >
  bash -c "
  sync-dags-from-s3 &
  exec airflow webserver
  "
```

### Running Once

Perform a single synchronization and exit:

```bash
sync-dags-from-s3 --once
```

### Testing Connection

Test S3 connection and configuration without syncing:

```bash
sync-dags-from-s3 --test
```

### Getting Help

```bash
sync-dags-from-s3 --help
```

## Integration with Airflow

### Option 1: Run Alongside Airflow (Recommended)

Run the sync utility as a background process alongside Airflow:

```yaml
# docker-compose.yml
version: '3.8'
services:
  airflow-scheduler:
    image: apache/airflow:latest
    environment:
      # S3 Sync Configuration
      S3_BUCKET: ${S3_BUCKET}
      S3_ACCESS_KEY_ID: ${S3_ACCESS_KEY_ID}
      S3_SECRET_ACCESS_KEY: ${S3_SECRET_ACCESS_KEY}
      # ... other environment variables
    command: >
      bash -c "
      sync-dags-from-s3 &&
      exec airflow scheduler
      "
```

### Option 2: Dedicated Sync Service

Run the sync utility in a separate container:

```yaml
# docker-compose.yml
version: '3.8'
services:
  s3-sync:
    image: apache/airflow:latest
    environment:
      S3_BUCKET: ${S3_BUCKET}
      S3_ACCESS_KEY_ID: ${S3_ACCESS_KEY_ID}
      S3_SECRET_ACCESS_KEY: ${S3_SECRET_ACCESS_KEY}
      S3_SYNC_INTERVAL_SECONDS: 300
      AIRFLOW_DAGS_FOLDER: /dags
    volumes:
      - ./dags:/dags
    command: sync-dags-from-s3
    restart: unless-stopped
```

### Option 3: Kubernetes Sidecar

Deploy as a sidecar container in Kubernetes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
spec:
  template:
    spec:
      containers:
      - name: airflow-scheduler
        image: apache/airflow:latest
        # ... scheduler configuration
        
      - name: s3-dag-sync
        image: apache/airflow:latest
        env:
        - name: S3_BUCKET
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: bucket
        - name: S3_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: access-key-id
        - name: S3_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: secret-access-key
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
        command: ["/usr/bin/dumb-init", "--", "/usr/local/bin/sync-dags-from-s3"]
```

### Custom S3 Providers

The utility works with any S3-compatible storage. Configure it by setting:
- `S3_ENDPOINT_URL`: The service endpoint
- `S3_REGION`: The region (may not apply to all providers)
- `S3_VERIFY_SSL`: SSL verification (often needed for self-hosted solutions)

### DAG Organization

Using `S3_DAGS_PREFIX`:

```
S3 Bucket Structure:
├── production/
│   ├── dag1.py
│   ├── dag2.py
│   └── folder/
│       └── dag3.py
└── development/
    ├── test_dag.py
    └── experimental/
```

Configuration:
```yaml
S3_DAGS_PREFIX: production/
```

### Multiple Environments

Sync DAGs from different buckets or prefixes for different environments:

```yaml
# Production
S3_BUCKET: airflow-dags-prod
S3_DAGS_PREFIX: production/
S3_SYNC_INTERVAL_SECONDS: 300

# Staging
S3_BUCKET: airflow-dags-staging
S3_DAGS_PREFIX: staging/
S3_SYNC_INTERVAL_SECONDS: 60  # More frequent for development
```

## Monitoring and Logging

### Log Output

The utility logs to stdout with the following format:

```
2026-02-21 00:00:00,000 - s3-dag-sync - INFO - Initialized S3 DAG Sync for bucket: my-dags
2026-02-21 00:00:00,001 - s3-dag-sync - INFO - DAGs prefix: 'production/'
2026-02-21 00:00:00,002 - s3-dag-sync - INFO - Found 25 DAG files in S3
2026-02-21 00:00:00,003 - s3-dag-sync - INFO - [NEW] example_dag.py
2026-02-21 00:00:00,004 - s3-dag-sync - INFO - [UPDATE] another_dag.py
2026-02-21 00:00:00,005 - s3-dag-sync - INFO - Sync completed: 1 new, 1 updated, 0 deleted
```

### Log Levels

- **INFO**: Normal operations (sync started, files synced, etc.)
- **WARNING**: Non-critical issues (file permissions, network retries)
- **ERROR**: Critical failures (connection errors, authentication failures)

### Health Checks

For Kubernetes or orchestration systems, implement health checks:

```bash
# Check if sync process is running
pgrep -f "sync-dags-from-s3" && echo "healthy" || echo "unhealthy"

# Or check last sync time
if [ $(find /opt/airflow/dags -name "*.py" -mmin -5 | wc -l) -gt 0 ]; then
  echo "healthy"
else
  echo "unhealthy"
fi
```

## FAQ

**Q: Does this work with Airflow's built-in S3 DAG loading?**

A: No, this is a separate utility that downloads DAGs from S3 to the local filesystem. Airflow then loads them normally from the local DAGs folder. This approach is useful when you want to:
- Use S3 as a distribution mechanism
- Have full control over sync timing and behavior

**Q: What happens if the sync utility fails?**

A: The utility logs the error and continues running. It will retry on the next interval. Airflow continues to use the last successfully synced DAGs.

**Q: Can I use multiple S3 buckets?**

A: Run multiple instances of the sync utility with different configurations, each syncing to different prefixes in the DAGs folder.

**Q: Does this delete local DAGs that I manually created?**

A: Only if `S3_DELETE_EXTRA_LOCAL=true`. The default is `false`, so locally created DAGs are preserved.

**Q: How do I handle large DAG repositories?**

A: Use `S3_DAGS_PREFIX` to organize DAGs, and consider increasing `S3_SYNC_INTERVAL_SECONDS` to avoid frequent full scans.

## Contributing

Found a bug or have a feature request? Please contribute to the Apache Airflow project:

- GitHub: https://github.com/apache/airflow
- Issue Tracker: https://github.com/apache/airflow/issues

## License

This utility is part of Apache Airflow and is licensed under the Apache License 2.0.