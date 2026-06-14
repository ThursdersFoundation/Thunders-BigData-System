# Kubernetes Deployment Guide

This guide provides comprehensive instructions for deploying Thunders-BigData-System on a Kubernetes cluster. It covers everything from initial setup to production-grade configuration, including autoscaling, service mesh integration, and monitoring.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Namespace and Resource Quotas](#namespace-and-resource-quotas)
3. [Deploying Core Services](#deploying-core-services)
4. [ConfigMaps and Secrets Management](#configmaps-and-secrets-management)
5. [Horizontal Pod Autoscaling](#horizontal-pod-autoscaling)
6. [Service Mesh (Istio) Integration](#service-mesh-istio-integration)
7. [Monitoring with Prometheus Operator](#monitoring-with-prometheus-operator)
8. [Troubleshooting Common Issues](#troubleshooting-common-issues)

---

## Prerequisites

Before deploying Thunders-BigData-System on Kubernetes, ensure you have the following tools and infrastructure in place.

### Required Tools

| Tool | Minimum Version | Purpose |
|------|----------------|---------|
| `kubectl` | v1.27+ | Kubernetes CLI for cluster interaction |
| `helm` | v3.12+ | Package manager for templated deployments |
| `kustomize` | v5.0+ | Kubernetes manifest customization (optional) |
| `istioctl` | v1.20+ | Istio service mesh CLI (if using Istio) |
| `jq` | v1.6+ | JSON processing for scripts |
| `yq` | v4.0+ | YAML processing for configuration |

Install `kubectl` and `helm` on macOS:

```bash
brew install kubectl helm
```

Install on Linux:

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### Cluster Requirements

- **Kubernetes version**: 1.27 or later (1.28+ recommended)
- **Node count**: Minimum 10 nodes for production (m5.4xlarge or equivalent)
- **Total CPU**: 160+ cores across the cluster
- **Total Memory**: 640+ GB across the cluster
- **Storage**: SSD-backed StorageClass (`gp3` on AWS, `premium-rwo` on GCP) for database and stateful workloads
- **Network**: CNI plugin with NetworkPolicy support (Calico, Cilium, or AWS VPC CNI)
- **Ingress**: NGINX Ingress Controller or AWS ALB Ingress Controller

### Verify Cluster Access

```bash
# Check cluster connectivity
kubectl cluster-info

# Verify node capacity
kubectl get nodes -o wide

# Check available StorageClasses
kubectl get storageclass
```

---

## Namespace and Resource Quotas

Thunders-BigData-System uses a dedicated namespace with resource quotas to ensure fair resource allocation and prevent noisy-neighbor issues in shared clusters.

### Create Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: thunders-bigdata
  labels:
    name: thunders-bigdata
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/enforce-version: v1.28
    istio-injection: enabled
```

Apply with:

```bash
kubectl apply -f namespace.yaml
```

### Resource Quotas

Resource quotas prevent the Thunders namespace from consuming more than its allocated share of cluster resources:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: thunders-bigdata-quota
  namespace: thunders-bigdata
spec:
  hard:
    requests.cpu: "80"
    requests.memory: 320Gi
    limits.cpu: "160"
    limits.memory: 640Gi
    persistentvolumeclaims: "50"
    services: "30"
    pods: "200"
    configmaps: "50"
    secrets: "50"
  scopes: []
```

### Limit Ranges

Limit ranges set default and maximum resource constraints for individual pods and containers:

```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: thunders-bigdata-limits
  namespace: thunders-bigdata
spec:
  limits:
    - type: Container
      default:
        cpu: "500m"
        memory: "512Mi"
      defaultRequest:
        cpu: "100m"
        memory: "128Mi"
      max:
        cpu: "16"
        memory: "64Gi"
      maxLimitRequestRatio:
        cpu: "8"
        memory: "4"
    - type: PersistentVolumeClaim
      max:
        storage: "5Ti"
      min:
        storage: "1Gi"
```

---

## Deploying Core Services

Thunders-BigData-System consists of several core services that must be deployed in the correct order due to dependencies.

### Service Dependency Order

1. PostgreSQL (metadata store)
2. Redis (cache and session store)
3. Apache Kafka (message bus)
4. Schema Registry (Kafka schema management)
5. Spark Master and Workers (batch processing)
6. Flink JobManager and TaskManagers (stream processing)
7. Thunders Application (core API and processing)
8. Ingress Controller (external access)

### Application Deployment

The primary application deployment is defined in `deployment/kubernetes/deployment.yaml`. Below is a reference configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thunders-bigdata
  namespace: thunders-bigdata
  labels:
    app: thunders-bigdata
    component: application
    version: v1.0.0
    environment: production
spec:
  replicas: 3
  revisionHistoryLimit: 5
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: thunders-bigdata
      component: application
  template:
    metadata:
      labels:
        app: thunders-bigdata
        component: application
        version: v1.0.0
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: thunders-bigdata
      terminationGracePeriodSeconds: 120
      initContainers:
        - name: wait-for-dependencies
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              echo "Waiting for PostgreSQL..."
              until nc -z ${POSTGRES_HOST} ${POSTGRES_PORT}; do sleep 2; done
              echo "Waiting for Redis..."
              until nc -z ${REDIS_HOST} ${REDIS_PORT}; do sleep 2; done
              echo "Waiting for Kafka..."
              until nc -z ${KAFKA_BROKER_HOST} ${KAFKA_BROKER_PORT}; do sleep 2; done
              echo "All dependencies ready"
          envFrom:
            - configMapRef:
                name: thunders-bigdata-config
      containers:
        - name: thunders-app
          image: thunders-bigdata:latest
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
            - name: metrics
              containerPort: 9090
              protocol: TCP
          envFrom:
            - configMapRef:
                name: thunders-bigdata-config
            - secretRef:
                name: thunders-bigdata-secrets
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "2000m"
              memory: "4Gi"
          livenessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 60
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /ready
              port: http
            initialDelaySeconds: 30
            periodSeconds: 15
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            runAsNonRoot: true
            runAsUser: 1000
            capabilities:
              drop:
                - ALL
```

### Deploy with Helm

For production deployments, we recommend using the official Helm chart:

```bash
# Add the Thunders Helm repository
helm repo add thunders https://charts.thunders.io
helm repo update

# Install with production values
helm install thunders-bds thunders/thunders-bigdata-system \
  --namespace thunders-bigdata --create-namespace \
  -f values-production.yaml

# Verify the deployment
kubectl get pods -n thunders-bigdata
kubectl get svc -n thunders-bigdata
```

### Kafka StatefulSet

Kafka is deployed as a StatefulSet for stable network identity and persistent storage:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: thunders-kafka
  namespace: thunders-bigdata
spec:
  serviceName: thunders-kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: thunders-kafka
  template:
    metadata:
      labels:
        app: thunders-kafka
    spec:
      containers:
        - name: kafka
          image: bitnami/kafka:3.6.1
          ports:
            - containerPort: 9092
              name: kafka
          env:
            - name: KAFKA_CFG_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: KAFKA_CFG_PROCESS_ROLES
              value: "broker,controller"
            - name: KAFKA_CFG_CONTROLLER_QUORUM_VOTERS
              value: "thunders-kafka-0.thunders-kafka-headless:9093,thunders-kafka-1.thunders-kafka-headless:9093,thunders-kafka-2.thunders-kafka-headless:9093"
          volumeMounts:
            - name: kafka-data
              mountPath: /bitnami/kafka
          resources:
            requests:
              cpu: "1"
              memory: "4Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
  volumeClaimTemplates:
    - metadata:
        name: kafka-data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: ssd-storage
        resources:
          requests:
            storage: 100Gi
```

---

## ConfigMaps and Secrets Management

### ConfigMap for Application Configuration

The application ConfigMap centralizes all non-sensitive configuration. See `deployment/kubernetes/configmap.yaml` for the complete reference:

```bash
kubectl apply -f deployment/kubernetes/configmap.yaml
```

Key configuration sections include:

- **Database**: PostgreSQL connection parameters and pool settings
- **Redis**: Cache host, port, and connection limits
- **Kafka**: Broker addresses, consumer/producer settings, and topic configurations
- **Spark**: Master URL, executor resources, and tuning parameters
- **Storage**: S3 bucket names and endpoints
- **Monitoring**: Metrics and tracing endpoints

### Secrets Management

For production deployments, use an external secrets manager such as HashiCorp Vault or AWS Secrets Manager with the Secrets Store CSI Driver.

#### Option 1: Kubernetes Native Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: thunders-bigdata-secrets
  namespace: thunders-bigdata
type: Opaque
stringData:
  POSTGRES_PASSWORD: <your-secure-password>
  REDIS_AUTH_TOKEN: <your-redis-token>
  KAFKA_SASL_PASSWORD: <your-kafka-password>
  S3_ACCESS_KEY: <your-access-key>
  S3_SECRET_KEY: <your-secret-key>
```

Create from literal values:

```bash
kubectl create secret generic thunders-bigdata-secrets \
  --namespace thunders-bigdata \
  --from-literal=POSTGRES_PASSWORD='$(openssl rand -base64 32)' \
  --from-literal=REDIS_AUTH_TOKEN='$(openssl rand -base64 32)' \
  --from-literal=KAFKA_SASL_PASSWORD='$(openssl rand -base64 32)'
```

#### Option 2: HashiCorp Vault Integration

Install the Vault Agent Injector to dynamically inject secrets:

```bash
helm repo add hashicorp https://helm.releases.hashicorp.com
helm install vault hashicorp/vault \
  --namespace vault --create-namespace
```

Annotate pods for secret injection:

```yaml
annotations:
  vault.hashicorp.com/agent-inject: "true"
  vault.hashicorp.com/role: "thunders-bigdata"
  vault.hashicorp.com/agent-inject-secret-db-creds: "secret/data/thunders/database"
  vault.hashicorp.com/agent-inject-template-db-creds: |
    {{- with secret "secret/data/thunders/database" -}}
    POSTGRES_PASSWORD={{ .Data.data.password }}
    {{- end }}
```

#### Option 3: AWS Secrets Manager with CSI Driver

```bash
# Install the Secrets Store CSI Driver
helm repo add secrets-store-csi-driver https://kubernetes-sigs.github.io/secrets-store-csi-driver/charts
helm install csi-secrets-store secrets-store-csi-driver/secrets-store-csi-driver \
  --namespace kube-system

# Install the AWS provider
kubectl apply -f https://raw.githubusercontent.com/aws/secrets-store-csi-driver-provider-aws/main/deployment/aws-provider-installer.yaml
```

---

## Horizontal Pod Autoscaling

Thunders-BigData-System supports automatic horizontal scaling based on CPU, memory, and custom metrics.

### Standard HPA Configuration

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: thunders-bigdata-hpa
  namespace: thunders-bigdata
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: thunders-bigdata
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
    - type: Pods
      pods:
        metric:
          name: http_requests_per_second
        target:
          type: AverageValue
          averageValue: "1000"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60
        - type: Percent
          value: 50
          periodSeconds: 60
      selectPolicy: Max
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 120
        - type: Percent
          value: 10
          periodSeconds: 120
      selectPolicy: Min
```

### Custom Metrics with Prometheus Adapter

To scale on custom metrics (e.g., Kafka consumer lag), install the Prometheus Adapter:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring \
  --set prometheus.url=http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local \
  --set rules.custom[0].seriesQuery='kafka_consumer_group_lag{namespace!="",topic!=""}' \
  --set rules.custom[0].resources.overrides.namespace.resource=namespace \
  --set rules.custom[0].name.as='kafka_consumer_lag' \
  --set rules.custom[0].metricsQuery='avg(kafka_consumer_group_lag{namespace="{{.namespace}}"}) by (topic)'
```

Then create an HPA based on Kafka lag:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: thunders-consumer-lag-hpa
  namespace: thunders-bigdata
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: thunders-bigdata
  minReplicas: 3
  maxReplicas: 30
  metrics:
    - type: External
      external:
        metric:
          name: kafka_consumer_lag
        target:
          type: AverageValue
          averageValue: "10000"
```

### KEDA Event-Driven Autoscaling

For more advanced event-driven scaling, use KEDA (Kubernetes Event-Driven Autoscaling):

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace
```

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: thunders-kafka-scaler
  namespace: thunders-bigdata
spec:
  scaleTargetRef:
    name: thunders-bigdata
  minReplicaCount: 3
  maxReplicaCount: 30
  cooldownPeriod: 120
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: thunders-kafka.thunders-bigdata.svc.cluster.local:9092
        consumerGroup: thunders-bigdata-consumers
        lagThreshold: "5000"
        topic: thunders-raw-events
```

---

## Service Mesh (Istio) Integration

Istio provides advanced traffic management, security (mTLS), and observability for the Thunders microservices architecture.

### Install Istio

```bash
# Download and install Istio
curl -L https://istio.io/downloadIstio | sh -
cd istio-1.20.0
export PATH=$PWD/bin:$PATH

# Install with production profile
istioctl install --set profile=production -y

# Enable sidecar injection for the namespace
kubectl label namespace thunders-bigdata istio-injection=enabled
```

### Traffic Management

Control traffic routing and implement canary deployments:

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: thunders-bigdata-vs
  namespace: thunders-bigdata
spec:
  hosts:
    - api.thunders.example.com
  gateways:
    - thunders-gateway
  http:
    - match:
        - headers:
            x-canary:
              exact: "true"
      route:
        - destination:
            host: thunders-bigdata
            port:
              number: 8080
            subset: canary
          weight: 100
    - route:
        - destination:
            host: thunders-bigdata
            port:
              number: 8080
            subset: stable
          weight: 95
        - destination:
            host: thunders-bigdata
            port:
              number: 8080
            subset: canary
          weight: 5
---
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: thunders-bigdata-dr
  namespace: thunders-bigdata
spec:
  host: thunders-bigdata
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 200
      http:
        h2UpgradePolicy: DEFAULT
        http1MaxPendingRequests: 200
        http2MaxRequests: 200
    outlierDetection:
      consecutive5xxErrors: 3
      interval: 30s
      baseEjectionTime: 30s
      maxEjectionPercent: 50
  subsets:
    - name: stable
      labels:
        version: v1.0.0
    - name: canary
      labels:
        version: v1.1.0
```

### mTLS and Authorization

Enforce strict mTLS and define authorization policies:

```yaml
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: thunders-bigdata-mtls
  namespace: thunders-bigdata
spec:
  mtls:
    mode: STRICT
---
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: thunders-bigdata-policy
  namespace: thunders-bigdata
spec:
  selector:
    matchLabels:
      app: thunders-bigdata
  rules:
    - from:
        - source:
            principals:
              - "cluster.local/ns/thunders-bigdata/sa/thunders-ingress"
            namespaces:
              - "thunders-bigdata"
      to:
        - operation:
            methods: ["GET", "POST"]
            paths: ["/api/v1/*"]
```

---

## Monitoring with Prometheus Operator

The Prometheus Operator (kube-prometheus-stack) provides full-stack monitoring for the Thunders platform.

### Install Prometheus Operator

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --set prometheus.prometheusSpec.retention=30d \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.storageClassName=gp3 \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=100Gi \
  --set grafana.adminPassword=$(openssl rand -base64 24)
```

### ServiceMonitor for Thunders

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: thunders-bigdata-metrics
  namespace: thunders-bigdata
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app: thunders-bigdata
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
      scrapeTimeout: 10s
```

### PrometheusRule for Alerting

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: thunders-bigdata-alerts
  namespace: thunders-bigdata
  labels:
    release: kube-prometheus-stack
spec:
  groups:
    - name: thunders-bigdata.rules
      rules:
        - alert: ThundersHighErrorRate
          expr: rate(http_requests_total{status=~"5..", app="thunders-bigdata"}[5m]) / rate(http_requests_total{app="thunders-bigdata"}[5m]) > 0.05
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "High error rate detected on Thunders API"
            description: "Error rate is {{ $value | humanizePercentage }} over the last 5 minutes"

        - alert: ThundersKafkaConsumerLag
          expr: kafka_consumer_group_lag{consumer_group="thunders-bigdata-consumers"} > 100000
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Kafka consumer lag is high"
            description: "Consumer group {{ $labels.consumer_group }} has a lag of {{ $value }} on topic {{ $labels.topic }}"

        - alert: ThundersPodCrashLooping
          expr: rate(kube_pod_container_status_restarts_total{namespace="thunders-bigdata"}[15m]) > 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Pod {{ $labels.pod }} is crash looping"
            description: "Pod {{ $labels.pod }} in namespace {{ $labels.namespace }} has restarted {{ $value }} times in the last 15 minutes"

        - alert: ThundersHighMemoryUsage
          expr: container_memory_working_set_bytes{namespace="thunders-bigdata", container!="POD"} / container_spec_memory_limit_bytes{namespace="thunders-bigdata", container!="POD"} > 0.9
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "High memory usage on {{ $labels.pod }}"
            description: "Memory usage is {{ $value | humanizePercentage }} of limit"
```

---

## Troubleshooting Common Issues

### Pods Stuck in `Pending` State

**Symptoms**: Pods remain in `Pending` state and never schedule.

**Diagnosis**:

```bash
kubectl describe pod <pod-name> -n thunders-bigdata
```

**Common Causes and Solutions**:

| Cause | Solution |
|-------|----------|
| Insufficient CPU/memory | Reduce resource requests or add nodes to the cluster |
| No matching node for taints/tolerations | Add appropriate tolerations or remove taints from nodes |
| PVC cannot be provisioned | Verify StorageClass exists and has available capacity |
| PodAntiAffinity rules too strict | Relax anti-affinity rules for non-critical workloads |

### Pods in `CrashLoopBackOff`

**Symptoms**: Pods repeatedly crash and restart.

**Diagnosis**:

```bash
kubectl logs <pod-name> -n thunders-bigdata --previous
kubectl describe pod <pod-name> -n thunders-bigdata
```

**Common Causes and Solutions**:

- **OOMKilled**: Increase memory limits or optimize memory usage
- **Failed health checks**: Check liveness/readiness probe configuration; increase `initialDelaySeconds` if the application needs more startup time
- **Missing configuration**: Verify ConfigMap and Secret volumes are mounted correctly
- **Dependency unavailable**: Ensure init containers are properly waiting for dependencies

### Kafka Connection Failures

**Symptoms**: Application logs show `Connection refused` or `Broker not available` errors.

**Diagnosis**:

```bash
# Check Kafka pod status
kubectl get pods -n thunders-bigdata -l app=thunders-kafka

# Test connectivity from inside the cluster
kubectl run kafka-test --rm -it --image=bitnami/kafka:3.6.1 --restart=Never -- \
  kafka-topics.sh --bootstrap-server thunders-kafka.thunders-bigdata.svc.cluster.local:9092 --list
```

**Solutions**:

- Verify the Kafka service is running: `kubectl get svc -n thunders-bigdata`
- Check that ConfigMap references use the correct service DNS name
- Ensure network policies allow traffic on port 9092

### High Memory Usage on Spark Workers

**Symptoms**: Spark worker pods are OOMKilled or consuming excessive memory.

**Solutions**:

- Adjust `SPARK_EXECUTOR_MEMORY` to leave 20% overhead for the JVM
- Enable `spark.sql.adaptive.enabled` for dynamic shuffle partition adjustment
- Use `spark.memory.fraction=0.6` to reserve more off-heap memory
- Consider using memory-optimized node types (r6i) for Spark worker nodes

### Flink JobManager Unreachable

**Symptoms**: Flink jobs fail to submit; TaskManagers cannot connect to JobManager.

**Solutions**:

```bash
# Check Flink JobManager status
kubectl logs -n thunders-bigdata -l app=thunders-flink,component=jobmanager

# Verify the JobManager service
kubectl get svc -n thunders-bigdata thunders-flink-jobmanager
```

- Ensure `jobmanager.rpc.address` is set to the JobManager service name
- Verify `taskmanager.hostname` is set to the pod IP via the downward API
- Check that the Flink ConfigMap is correctly mounted

### Image Pull Errors

**Symptoms**: `ImagePullBackOff` or `ErrImagePull` errors.

**Solutions**:

```bash
# Check image pull secrets
kubectl get secret -n thunders-bigdata

# Create a Docker registry secret if needed
kubectl create secret docker-registry registry-credentials \
  --namespace thunders-bigdata \
  --docker-server=registry.thunders.io \
  --docker-username=<username> \
  --docker-password=<password>
```

- Ensure image tags exist in the registry
- Verify image pull secrets are referenced in the ServiceAccount
- For private registries, configure `imagePullSecrets` in the pod spec

---

## Next Steps

- [Docker Deployment Guide](docker.md) - Local development with Docker Compose
- [Cloud Setup Guide](cloud_setup.md) - Infrastructure provisioning on AWS, Azure, and GCP
- [System Architecture](../architecture/system_overview.md) - Understanding the platform architecture
