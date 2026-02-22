# S3 DAG Sync - Quick Start Guide

The S3 DAG sync utility automatically synchronizes DAG files from S3-compatible storage (AWS S3, MinIO, Yandex Object Storage, etc.) to your Airflow DAGs folder.

## 🚀 Quick Start

### 1. Choose Your S3 Provider

- **AWS S3**: Use with Amazon Web Services
- **MinIO**: Self-hosted S3-compatible storage
- **Yandex Object Storage**: Yandex Cloud's S3 service
- **Any S3-compatible storage**: Ceph, Wasabi, DigitalOcean Spaces, etc.

### 2. Configure Environment Variables

**Required:**
```bash
S3_BUCKET=your-bucket-name
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
```

**Optional (Provider-specific):**
```bash
# For non-AWS S3 (MinIO, Yandex, etc.)
S3_ENDPOINT_URL=https://your-s3-endpoint.com
S3_REGION=your-region

# Sync behavior
S3_SYNC_INTERVAL_SECONDS=300  # Sync every 5 minutes
S3_DAGS_PREFIX=dags/          # Path in bucket
S3_VERIFY_SSL=true              # SSL verification
```

### 3. Start Airflow with DAG Sync

#### Option A: Using Docker Compose (Recommended)

Choose an example from `docs/docker-stack/docker-compose/`:

```bash
# AWS S3
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-aws.yml up -d

# MinIO (includes local MinIO server)
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-minio.yml up -d

# Yandex Object Storage
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-yandex.yml up -d
```

#### Option B: Using Helm Chart

Configure S3 DAG sync in your `values.yaml`:

```yaml
dags:
  s3Sync:
    enabled: true
    bucket: my-airflow-dags
    prefix: dags/
    accessKeyId:
      valueFrom:
        secretKeyRef:
          name: s3-credentials
          key: access-key-id
    secretAccessKey:
      valueFrom:
        secretKeyRef:
          name: s3-credentials
          key: secret-access-key
    endpoint: https://s3.amazonaws.com  # Optional: for non-AWS S3
    region: us-east-1
    verifySSL: true
    interval: 300
    deleteExtraLocal: false
    downloadOnStartup: true
```

Or using plain environment variables (not recommended for production):

```yaml
dags:
  s3Sync:
    enabled: true
    bucket: my-airflow-dags
    accessKeyId: "AKIAIOSFODNN7EXAMPLE"
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    endpoint: "https://s3.amazonaws.com"
    region: "us-east-1"
```

Deploy with the S3 sync sidecar enabled:

```bash
helm install airflow apache/airflow -f values.yaml
```

The S3 sync container will be added as a sidecar to all Airflow components that need DAG access (webserver, scheduler, workers, triggerer, dag-processor).

#### Option C: Manual Docker Run

```bash
docker run -d \
  --name airflow-webserver \
  -p 8080:8080 \
  -e S3_BUCKET=my-dags \
  -e S3_ACCESS_KEY_ID=your_key \
  -e S3_SECRET_ACCESS_KEY=your_secret \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e _AIRFLOW_WWW_USER_CREATE=true \
  -e _AIRFLOW_WWW_USER_USERNAME=admin \
  -e _AIRFLOW_WWW_USER_PASSWORD=admin \
  apache/airflow:latest \
  bash -c "sync-dags-from-s3 && exec airflow webserver"
```

#### Option C: Kubernetes

Add as a sidecar container in your Airflow deployment:

```yaml
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

### 4. Verify It's Working

Check logs for sync activity:

```bash
docker logs -f airflow-webserver
```

You should see:
```
2026-02-21 00:00:00 - s3-dag-sync - INFO - Initialized S3 DAG Sync for bucket: my-dags
2026-02-21 00:00:00 - s3-dag-sync - INFO - Found X DAG files in S3
2026-02-21 00:00:00 - s3-dag-sync - INFO - [NEW] example_dag.py
2026-02-21 00:00:00 - s3-dag-sync - INFO - Sync completed: X new, 0 updated, 0 deleted
```

Access Airflow at http://localhost:8080 and verify your DAGs appear!

## 📚 Documentation

### Full Documentation
- **[Complete Guide](s3-dag-sync.md)**: Detailed documentation with all features, configuration options, troubleshooting, and best practices.

### Example Configurations
- **[AWS S3](docker-compose/s3-dag-sync-aws.yml)**: Complete Airflow stack with AWS S3
- **[MinIO](docker-compose/s3-dag-sync-minio.yml)**: Includes local MinIO server for testing
- **[Yandex Object Storage](docker-compose/s3-dag-sync-yandex.yml)**: Yandex Cloud integration

## 🔧 Configuration Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `S3_BUCKET` | S3 bucket name | `my-airflow-dags` |
| `S3_ACCESS_KEY_ID` | S3 access key ID | `AKIAIOSFODNN7EXAMPLE` |
| `S3_SECRET_ACCESS_KEY` | S3 secret access key | `wJalrXUtnFEMI...` |

### Common Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT_URL` | AWS default | Custom S3 endpoint (for MinIO, Yandex, etc.) |
| `S3_REGION` | `us-east-1` | AWS/S3 region |
| `S3_DAGS_PREFIX` | `""` | Prefix/path in bucket |
| `S3_SYNC_INTERVAL_SECONDS` | `300` | Sync interval (seconds) |
| `S3_VERIFY_SSL` | `true` | Verify SSL certificates |
| `S3_DELETE_EXTRA_LOCAL` | `false` | Delete local files not in S3 |
| `S3_DOWNLOAD_ON_STARTUP` | `true` | Download all on first run |

## 🎯 Common Use Cases

### Development with MinIO

Perfect for local development and testing:

```bash
# MinIO includes a full S3-compatible server
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-minio.yml up -d

# Access MinIO console at http://localhost:9001
# Username: minioadmin
# Password: minioadmin
# Create bucket: airflow-dags
# Upload your DAGs
```

### Production with AWS S3

Use AWS S3 for production workloads:

```bash
# Create .env file:
echo "AWS_ACCESS_KEY_ID=your_key" > .env
echo "AWS_SECRET_ACCESS_KEY=your_secret" >> .env
echo "S3_BUCKET=your-bucket" >> .env

# Start Airflow
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-aws.yml up -d
```

### Regional Deployment with Yandex

Deploy in Yandex Cloud region:

```bash
# Create .env file:
echo "YC_ACCESS_KEY_ID=your_key" > .env
echo "YC_SECRET_ACCESS_KEY=your_secret" >> .env
echo "S3_BUCKET=your-bucket" >> .env
echo "YC_REGION=ru-central1" >> .env

# Start Airflow
docker-compose -f docs/docker-stack/docker-compose/s3-dag-sync-yandex.yml up -d
```

## 🧪 Testing

### Test Connection Without Syncing

```bash
docker run --rm \
  -e S3_BUCKET=test-bucket \
  -e S3_ACCESS_KEY_ID=your_key \
  -e S3_SECRET_ACCESS_KEY=your_secret \
  apache/airflow:latest \
  sync-dags-from-s3 --test
```

### Run Single Sync

```bash
docker run --rm \
  -e S3_BUCKET=test-bucket \
  -e S3_ACCESS_KEY_ID=your_key \
  -e S3_SECRET_ACCESS_KEY=your_secret \
  -v $(pwd)/dags:/opt/airflow/dags \
  apache/airflow:latest \
  sync-dags-from-s3 --once
```

## 🐛 Troubleshooting

### Connection Issues

**Problem**: `Bucket not found` or `Access denied`

**Solutions**:
1. Verify `S3_BUCKET` name is correct
2. Check credentials have proper permissions
3. For custom endpoints, verify `S3_ENDPOINT_URL`
4. Run `sync-dags-from-s3 --test` to diagnose

### DAGs Not Appearing

**Problem**: DAGs in S3 not showing up in Airflow

**Solutions**:
1. Check `S3_DAGS_PREFIX` matches your bucket structure
2. Verify files have `.py` extension
3. Check logs for specific errors
4. Ensure `AIRFLOW_DAGS_FOLDER` is correct (default: `/opt/airflow/dags`)

### SSL Errors

**Problem**: `SSL verification failed`

**Solutions**:
1. For self-signed certificates (MinIO, Ceph): Set `S3_VERIFY_SSL=false`
2. For production: Ensure proper CA certificates are installed

## 📖 Advanced Topics

### Multiple Environments

Sync from different buckets for different environments:

```yaml
# Production
S3_BUCKET: airflow-dags-prod
S3_DAGS_PREFIX: production/
S
# Staging
S3_BUCKET: airflow-dags-staging
S3_DAGS_PREFIX: staging/
```

### DAG Organization

Organize DAGs with prefixes:

```
S3 Bucket:
├── production/
│   ├── critical/
│   │   └── dag1.py
│   ├── regular/
│   │   └── dag2.py
└── development/
    └── test_dag.py
```

Configuration:
```bash
S3_DAGS_PREFIX=production/
```