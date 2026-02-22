# Airflow Fork with S3 DAG Sync

## What is this?

Fork of Apache Airflow with built-in S3 DAG synchronization. DAGs are automatically synced from S3-compatible storage (AWS S3, MinIO, Yandex Object Storage, etc.) to Airflow pods.

## Problem it solves

Standard Airflow requires DAGs to be either:
- Baked into Docker images (slow deployment cycle)
- Mounted via persistent volumes (operational complexity)
- Synced via git-sync (requires git repository)

This fork adds a lightweight S3 sync sidecar that continuously downloads DAGs from S3 storage - simple, fast, and works with any S3-compatible service.

## How to use

### Docker (simple)

```bash
docker run -d \
  -e S3_BUCKET=my-dags \
  -e S3_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  -e S3_SECRET_ACCESS_KEY=YOUR_SECRET_KEY \
  -e S3_ENDPOINT_URL=http://minio:9000 \
  -v ./dags:/opt/airflow/dags \
  airflow-s3-sync \
  sync-dags-from-s3
```

### Kubernetes/Helm

#### 1. Build the image

```bash
docker build -t airflow-s3-sync .
```

#### 2. Configure S3 sync in values.yaml

```yaml
dags:
  s3Sync:
    enabled: true
    bucket: my-dags-bucket
    accessKeyId: "YOUR_ACCESS_KEY"
    secretAccessKey: "YOUR_SECRET_KEY"
    endpointUrl: "https://s3.amazonaws.com"  # or MinIO/Yandex endpoint
    region: "us-east-1"
    dagsPrefix: ""  # optional path prefix in bucket
    syncIntervalSeconds: 300  # sync every 5 minutes
```

### 3. Deploy with Helm

```bash
helm install airflow ./chart -f values.yaml
```

## Key features

- **Automatic sync**: DAGs sync from S3 at configurable intervals
- **Incremental updates**: Only changed files are downloaded
- **Multi-provider support**: AWS S3, MinIO, Yandex, Ceph, etc.
- **Helm integration**: Native sidecar container in official chart
- **Zero code changes**: Works with standard Airflow DAGs

## CLI options

```bash
sync-dags-from-s3           # Run as daemon (syncs continuously)
sync-dags-from-s3 --once    # Single sync, then exit
sync-dags-from-s3 --test    # Test S3 connection, then exit
```

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `S3_BUCKET` | Yes | - | S3 bucket name |
| `S3_ACCESS_KEY_ID` | Yes | - | Access key |
| `S3_SECRET_ACCESS_KEY` | Yes | - | Secret key |
| `S3_ENDPOINT_URL` | No | AWS | Custom endpoint (MinIO, Yandex, etc.) |
| `S3_REGION` | No | us-east-1 | Region |
| `S3_DAGS_PREFIX` | No | "" | Path prefix in bucket |
| `S3_SYNC_INTERVAL_SECONDS` | No | 300 | Sync interval |
| `S3_VERIFY_SSL` | No | true | Verify SSL certs |
| `S3_DELETE_EXTRA_LOCAL` | No | false | Delete local files not in S3 |
| `AIRFLOW_DAGS_FOLDER` | No | /opt/airflow/dags | Local DAGs path |

## Documentation

- [S3 DAG Sync Guide](docs/docker-stack/README_S3_DAG_SYNC.md) - Quick start
- [Full Documentation](docs/docker-stack/s3-dag-sync.md) - All configuration options
- [Docker Compose Examples](docs/docker-stack/docker-compose/) - AWS, MinIO, Yandex

## What's added

1. **Script**: `scripts/docker/sync_dags_from_s3.py` - Python utility for S3 sync
2. **Helm Chart**: Modified `chart/values.yaml` with `dags.s3Sync` configuration
3. **Templates**: Sidecar container templates for all Airflow components

## Upstream

Based on Apache Airflow. Sync your fork with upstream regularly to get Airflow updates.
