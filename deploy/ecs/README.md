# Deploying Substrate on ECS Fargate + ALB

This guide deploys Substrate as a single Fargate task behind an Application
Load Balancer, with SQLite state stored on Amazon EFS.

## Prerequisites

- AWS CLI v2, authenticated with sufficient permissions
- Existing VPC with at least two private subnets in different AZs
- An ECS cluster with Fargate capacity provider enabled

Set shell variables used throughout this guide:

```bash
AWS_REGION=us-east-1
VPC_ID=vpc-xxxxxxxxxxxxxxxxx
SUBNET_IDS="subnet-aaa,subnet-bbb"   # comma-separated, at least 2
CLUSTER_NAME=my-cluster
```

---

## Step 1 — Create the EFS file system

```bash
EFS_ID=$(aws efs create-file-system \
  --region "$AWS_REGION" \
  --performance-mode generalPurpose \
  --throughput-mode bursting \
  --encrypted \
  --tags Key=Name,Value=substrate-data \
  --query 'FileSystemId' --output text)
echo "EFS: $EFS_ID"
```

Create an access point so the container runs as `nobody` (uid/gid 65534):

```bash
AP_ID=$(aws efs create-access-point \
  --file-system-id "$EFS_ID" \
  --posix-user Uid=65534,Gid=65534 \
  --root-directory "Path=/substrate,CreationInfo={OwnerUid=65534,OwnerGid=65534,Permissions=755}" \
  --query 'AccessPointId' --output text)
echo "Access Point: $AP_ID"
```

---

## Step 2 — Create the CloudWatch log group

```bash
CW_LOG_GROUP=/ecs/substrate
aws logs create-log-group --log-group-name "$CW_LOG_GROUP" --region "$AWS_REGION"
```

---

## Step 3 — Create IAM roles

### Execution role (pulls image, writes logs)

```bash
aws iam create-role \
  --role-name substrate-task-execution \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy \
  --role-name substrate-task-execution \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

EXEC_ROLE_ARN=$(aws iam get-role --role-name substrate-task-execution --query 'Role.Arn' --output text)
```

### Task role (EFS access)

```bash
aws iam create-role \
  --role-name substrate-task \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy \
  --role-name substrate-task \
  --policy-name efs-access \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"elasticfilesystem:ClientMount\",\"elasticfilesystem:ClientWrite\",\"elasticfilesystem:DescribeMountTargets\"],\"Resource\":\"arn:aws:elasticfilesystem:${AWS_REGION}:*:file-system/${EFS_ID}\"}]}"

TASK_ROLE_ARN=$(aws iam get-role --role-name substrate-task --query 'Role.Arn' --output text)
```

---

## Step 4 — Create security groups

```bash
# ECS task security group
ECS_SG=$(aws ec2 create-security-group \
  --group-name substrate-ecs \
  --description "Substrate ECS tasks" \
  --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)

# EFS security group
EFS_SG=$(aws ec2 create-security-group \
  --group-name substrate-efs \
  --description "Substrate EFS mount targets" \
  --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)

# ALB security group
ALB_SG=$(aws ec2 create-security-group \
  --group-name substrate-alb \
  --description "Substrate ALB" \
  --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)

# ALB: inbound HTTP from anywhere (restrict in production)
aws ec2 authorize-security-group-ingress --group-id "$ALB_SG" \
  --protocol tcp --port 4566 --cidr 0.0.0.0/0

# ECS: inbound 4566 from ALB only
aws ec2 authorize-security-group-ingress --group-id "$ECS_SG" \
  --protocol tcp --port 4566 --source-group "$ALB_SG"

# EFS: inbound NFS from ECS tasks
aws ec2 authorize-security-group-ingress --group-id "$EFS_SG" \
  --protocol tcp --port 2049 --source-group "$ECS_SG"
```

---

## Step 5 — Create EFS mount targets

One mount target per AZ (adjust subnet IDs as needed):

```bash
for SUBNET in $(echo "$SUBNET_IDS" | tr ',' ' '); do
  aws efs create-mount-target \
    --file-system-id "$EFS_ID" \
    --subnet-id "$SUBNET" \
    --security-groups "$EFS_SG"
done
```

Wait for mount targets to become available (~30 s):

```bash
aws efs describe-mount-targets --file-system-id "$EFS_ID" \
  --query 'MountTargets[*].LifeCycleState'
```

---

## Step 6 — Register the task definition

Fill in the placeholders and register:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

sed \
  -e "s|{{TASK_EXECUTION_ROLE_ARN}}|${EXEC_ROLE_ARN}|g" \
  -e "s|{{TASK_ROLE_ARN}}|${TASK_ROLE_ARN}|g" \
  -e "s|{{EFS_FILE_SYSTEM_ID}}|${EFS_ID}|g" \
  -e "s|{{EFS_ACCESS_POINT_ID}}|${AP_ID}|g" \
  -e "s|{{CLOUDWATCH_LOG_GROUP}}|${CW_LOG_GROUP}|g" \
  -e "s|{{AWS_REGION}}|${AWS_REGION}|g" \
  task-definition.json > task-definition-rendered.json

aws ecs register-task-definition \
  --cli-input-json file://task-definition-rendered.json \
  --region "$AWS_REGION"
```

---

## Step 7 — Create the ALB and target group

```bash
# Public subnets for the ALB (replace with your public subnet IDs)
PUBLIC_SUBNETS="subnet-pub1,subnet-pub2"

ALB_ARN=$(aws elbv2 create-load-balancer \
  --name substrate-alb \
  --subnets $(echo "$PUBLIC_SUBNETS" | tr ',' ' ') \
  --security-groups "$ALB_SG" \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text)

TG_ARN=$(aws elbv2 create-target-group \
  --name substrate-tg \
  --protocol HTTP \
  --port 4566 \
  --vpc-id "$VPC_ID" \
  --target-type ip \
  --health-check-path /health \
  --query 'TargetGroups[0].TargetGroupArn' --output text)

aws elbv2 create-listener \
  --load-balancer-arn "$ALB_ARN" \
  --protocol HTTP \
  --port 4566 \
  --default-actions Type=forward,TargetGroupArn="$TG_ARN"

ALB_DNS=$(aws elbv2 describe-load-balancers \
  --load-balancer-arns "$ALB_ARN" \
  --query 'LoadBalancers[0].DNSName' --output text)
echo "ALB DNS: $ALB_DNS"
```

---

## Step 8 — Create the ECS service

```bash
aws ecs create-service \
  --cluster "$CLUSTER_NAME" \
  --service-name substrate \
  --task-definition substrate \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$(echo $SUBNET_IDS | tr ',' ',')],securityGroups=[$ECS_SG],assignPublicIp=DISABLED}" \
  --load-balancers "targetGroupArn=$TG_ARN,containerName=substrate,containerPort=4566" \
  --region "$AWS_REGION"
```

---

## Step 9 — Verify

```bash
# Wait for the task to reach RUNNING (~60 s)
aws ecs describe-services \
  --cluster "$CLUSTER_NAME" \
  --services substrate \
  --query 'services[0].{status:status,running:runningCount,desired:desiredCount}'

curl "http://${ALB_DNS}:4566/health"
# {"status":"ok"}
```

---

## Step 10 — TLS (optional)

1. Request an ACM certificate for your domain.
2. Add an HTTPS listener on port 443 with the ACM certificate.
3. Add a redirect rule on port 80 → HTTPS.
4. Update your DNS to point to `$ALB_DNS`.

---

## Notes

- **SQLite + EFS:** WAL mode is used automatically. Because EFS provides
  distributed locking, a single writer is sufficient — keep `--desired-count 1`.
- **Upgrades:** Update the task definition image tag and run
  `aws ecs update-service --force-new-deployment`.
- **Cost:** 0.25 vCPU / 512 MB Fargate + EFS standard storage.
  Estimate ~$8–12/month for low-traffic dev use.
