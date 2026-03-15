# Substrate Deployment Options

Three self-contained deployment options are provided. Choose the one that
matches your environment.

| Option             | Use case                  | Persistence           | Replicas |
|--------------------|---------------------------|-----------------------|----------|
| Docker Compose     | Local dev, single machine | Named volume (SQLite) | 1        |
| ECS Fargate + ALB  | AWS-native staging / prod | Amazon EFS (SQLite)   | 1        |
| Kubernetes         | Any K8s cluster           | PVC (SQLite)          | 1        |

> **Scaling note:** All three options require `replicas: 1`. SQLite does not
> support concurrent writes from multiple processes. If you need horizontal
> scale, replace the SQLite backend with the file backend on a shared NFS/EFS
> mount and accept the consistency trade-offs, or wait for a future release
> with a multi-writer backend.

> **TLS note:** None of these options terminate TLS at the Substrate level.
> Place a reverse proxy (Caddy, nginx, AWS ALB with ACM) in front of the
> container and forward plain HTTP to port 4566.

---

## Docker Compose (recommended for local dev)

### Quick start

```bash
# Clone the repo (config files are included)
git clone https://github.com/scttfrdmn/substrate
cd substrate

# Start Substrate in the background
docker compose up -d

# Verify it is healthy (~15 s for SQLite to initialise)
curl http://localhost:4566/health
# {"status":"ok"}

# Stop
docker compose down

# Stop and wipe all recorded data
docker compose down -v
```

### Useful commands

```bash
docker compose ps              # Show container status
docker compose logs -f substrate  # Tail logs
docker compose restart substrate  # Apply config changes
```

### Point your application at Substrate

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 mb s3://my-bucket --no-sign-request
aws dynamodb list-tables --no-sign-request
```

See [`configs/substrate-local.yaml`](../configs/substrate-local.yaml) for
all configuration options. After editing, run `docker compose restart substrate`.

---

## ECS Fargate + ALB

See [`ecs/README.md`](ecs/README.md) for the full step-by-step guide.

**Summary:**

1. Create an EFS file system and access point.
2. Fill in the six `{{PLACEHOLDERS}}` in [`ecs/task-definition.json`](ecs/task-definition.json).
3. Register the task definition and create an ECS service with `--desired-count 1`.
4. Put an ALB in front with a `/health` health-check.

---

## Kubernetes

```bash
# Dry-run first
kubectl apply --dry-run=client -f k8s/

# Deploy
kubectl apply -f k8s/

# Verify
kubectl get pods -l app=substrate
kubectl port-forward svc/substrate 4566:4566
curl http://localhost:4566/health
```

The manifests assume the `default` namespace. Override with
`-n <namespace>` or edit the manifests directly.

For external access, edit [`k8s/deployment.yaml`](k8s/deployment.yaml) and
change the Service `type` from `ClusterIP` to `LoadBalancer`.
