# ML API Reference

The ML API provides endpoints for training machine learning models, making predictions, managing the model registry, accessing the feature store, tracking experiments, and configuring A/B tests. It integrates with MLflow for model lifecycle management and supports both batch and real-time inference.

**Base URL**: `https://api.thunders.example.com/api/v1`

---

## Table of Contents

1. [Model Training API](#model-training-api)
2. [Prediction API](#prediction-api)
3. [Model Registry API](#model-registry-api)
4. [Feature Store API](#feature-store-api)
5. [Experiment Tracking API](#experiment-tracking-api)
6. [Batch vs Real-Time Inference](#batch-vs-real-time-inference)
7. [Model Versioning and A/B Testing](#model-versioning-and-ab-testing)

---

## Model Training API

### POST /api/v1/ml/train

Submit a model training job. The training job runs asynchronously on the Spark cluster and can be monitored via the job status endpoint.

**Request Body**:

```json
{
  "experiment_name": "customer-churn-prediction",
  "run_name": "xgboost-v3-hyperopt",
  "model_name": "customer_churn_model",
  "algorithm": "xgboost",
  "dataset": "customer_features",
  "target_column": "churned",
  "feature_columns": ["tenure_months", "monthly_charges", "total_charges", "contract_type", "payment_method", "support_tickets"],
  "hyperparameters": {
    "max_depth": 8,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "min_child_weight": 5
  },
  "training_config": {
    "test_size": 0.2,
    "validation_size": 0.1,
    "cross_validation_folds": 5,
    "stratify": true,
    "random_seed": 42,
    "early_stopping_rounds": 50,
    "eval_metric": "auc"
  },
  "compute_config": {
    "executor_memory": "8g",
    "executor_cores": 4,
    "num_executors": 4,
    "max_runtime_hours": 2,
    "gpu_enabled": false
  },
  "tags": {
    "team": "data-science",
    "domain": "customer-analytics",
    "framework": "xgboost"
  }
}
```

**Request Schema**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `experiment_name` | string | Yes | MLflow experiment name |
| `run_name` | string | No | Descriptive name for this training run |
| `model_name` | string | Yes | Model name in the registry |
| `algorithm` | string | Yes | Algorithm: `xgboost`, `random_forest`, `logistic_regression`, `lightgbm`, `neural_network`, `custom` |
| `dataset` | string | Yes | Source dataset for training |
| `target_column` | string | Yes | Target/label column |
| `feature_columns` | array | Yes | List of feature column names |
| `hyperparameters` | object | No | Algorithm-specific hyperparameters |
| `training_config` | object | No | Training configuration (splits, CV, early stopping) |
| `compute_config` | object | No | Compute resource configuration |
| `tags` | object | No | Custom tags for organization |

**Supported Algorithms**:

| Algorithm | Type | Key Hyperparameters |
|-----------|------|-------------------|
| `xgboost` | Classification/Regression | `max_depth`, `learning_rate`, `n_estimators`, `subsample` |
| `lightgbm` | Classification/Regression | `num_leaves`, `learning_rate`, `n_estimators`, `feature_fraction` |
| `random_forest` | Classification/Regression | `n_estimators`, `max_depth`, `min_samples_split` |
| `logistic_regression` | Classification | `C`, `penalty`, `solver`, `max_iter` |
| `linear_regression` | Regression | `alpha`, `fit_intercept`, `normalize` |
| `neural_network` | Classification/Regression | `hidden_layers`, `activation`, `optimizer`, `epochs`, `batch_size` |
| `kmeans` | Clustering | `n_clusters`, `init`, `max_iter` |
| `isolation_forest` | Anomaly Detection | `n_estimators`, `contamination`, `max_samples` |
| `prophet` | Time-Series Forecasting | `changepoint_prior_scale`, `seasonality_prior_scale` |
| `custom` | Any | Requires `training_script_uri` in hyperparameters |

**Response** (202 Accepted):

```json
{
  "job_id": "train-a1b2c3d4",
  "experiment_name": "customer-churn-prediction",
  "run_name": "xgboost-v3-hyperopt",
  "model_name": "customer_churn_model",
  "status": "QUEUED",
  "submitted_at": "2024-01-15T10:30:00.000Z",
  "estimated_duration_minutes": 15,
  "links": {
    "status": "/api/v1/ml/train/train-a1b2c3d4",
    "cancel": "/api/v1/ml/train/train-a1b2c3d4/cancel",
    "mlflow_ui": "https://mlflow.thunders.example.com/#/experiments/42/runs/a1b2c3d4"
  }
}
```

### GET /api/v1/ml/train/{job_id}

Get the status of a training job.

**Response** (200 OK - In Progress):

```json
{
  "job_id": "train-a1b2c3d4",
  "status": "RUNNING",
  "progress": {
    "current_epoch": 3,
    "total_epochs": 10,
    "current_fold": 2,
    "total_folds": 5,
    "percentage": 35
  },
  "metrics": {
    "train_auc": 0.8923,
    "val_auc": 0.8756,
    "train_log_loss": 0.3214,
    "val_log_loss": 0.3456
  },
  "started_at": "2024-01-15T10:30:15Z",
  "estimated_completion_at": "2024-01-15T10:45:15Z"
}
```

**Response** (200 OK - Completed):

```json
{
  "job_id": "train-a1b2c3d4",
  "status": "COMPLETED",
  "model_name": "customer_churn_model",
  "model_version": 3,
  "run_id": "a1b2c3d4e5f6g7h8",
  "metrics": {
    "train_auc": 0.9234,
    "val_auc": 0.9123,
    "test_auc": 0.9087,
    "train_f1": 0.8756,
    "val_f1": 0.8634,
    "test_f1": 0.8598,
    "train_accuracy": 0.8912,
    "val_accuracy": 0.8834,
    "test_accuracy": 0.8798
  },
  "feature_importance": {
    "tenure_months": 0.2834,
    "monthly_charges": 0.2156,
    "contract_type": 0.1823,
    "total_charges": 0.1345,
    "support_tickets": 0.0987,
    "payment_method": 0.0855
  },
  "artifacts": {
    "model_binary": "s3://thunders-bigdata-artifacts/models/customer_churn_model/v3/model.xgb",
    "training_log": "s3://thunders-bigdata-artifacts/models/customer_churn_model/v3/training.log",
    "feature_pipeline": "s3://thunders-bigdata-artifacts/models/customer_churn_model/v3/pipeline.pkl",
    "confusion_matrix": "s3://thunders-bigdata-artifacts/models/customer_churn_model/v3/confusion_matrix.png"
  },
  "training_duration_seconds": 842,
  "data_size": {
    "training_rows": 72000,
    "validation_rows": 9000,
    "test_rows": 9000,
    "features": 6
  },
  "completed_at": "2024-01-15T10:44:17Z"
}
```

### Hyperparameter Optimization

Submit a hyperparameter tuning job using Bayesian optimization:

```json
{
  "experiment_name": "customer-churn-prediction",
  "run_name": "xgboost-hyperopt-bayesian",
  "model_name": "customer_churn_model",
  "algorithm": "xgboost",
  "dataset": "customer_features",
  "target_column": "churned",
  "feature_columns": ["tenure_months", "monthly_charges", "total_charges", "contract_type", "payment_method", "support_tickets"],
  "hyperparameter_optimization": {
    "method": "bayesian",
    "objective": "maximize",
    "metric": "val_auc",
    "max_trials": 50,
    "max_concurrent_trials": 4,
    "search_space": {
      "max_depth": {"type": "int", "low": 3, "high": 12},
      "learning_rate": {"type": "float", "low": 0.001, "high": 0.3, "log": true},
      "n_estimators": {"type": "int", "low": 100, "high": 1000},
      "subsample": {"type": "float", "low": 0.5, "high": 1.0},
      "colsample_bytree": {"type": "float", "low": 0.5, "high": 1.0},
      "reg_alpha": {"type": "float", "low": 0.001, "high": 10.0, "log": true},
      "reg_lambda": {"type": "float", "low": 0.001, "high": 10.0, "log": true}
    },
    "early_termination": {
      "type": "median_stopping_rule",
      "min_trials": 10,
      "evaluation_interval": 1
    }
  },
  "training_config": {
    "test_size": 0.2,
    "cross_validation_folds": 5,
    "eval_metric": "auc"
  }
}
```

---

## Prediction API

### POST /api/v1/ml/predict

Make predictions using a registered model. Supports both single-record and batch prediction modes.

**Request Body**:

```json
{
  "model_name": "customer_churn_model",
  "model_version": 3,
  "predictions": [
    {
      "customer_id": "c-001",
      "features": {
        "tenure_months": 24,
        "monthly_charges": 85.50,
        "total_charges": 2052.00,
        "contract_type": "month-to-month",
        "payment_method": "electronic_check",
        "support_tickets": 3
      }
    },
    {
      "customer_id": "c-002",
      "features": {
        "tenure_months": 48,
        "monthly_charges": 55.00,
        "total_charges": 2640.00,
        "contract_type": "two_year",
        "payment_method": "bank_transfer",
        "support_tickets": 0
      }
    }
  ],
  "options": {
    "include_probabilities": true,
    "include_feature_contributions": true,
    "include_prediction_interval": false
  }
}
```

**Response** (200 OK):

```json
{
  "model_name": "customer_churn_model",
  "model_version": 3,
  "predictions": [
    {
      "customer_id": "c-001",
      "prediction": "churn",
      "probability": {
        "churn": 0.7823,
        "retain": 0.2177
      },
      "feature_contributions": {
        "contract_type": 0.2834,
        "support_tickets": 0.2156,
        "tenure_months": -0.1345,
        "monthly_charges": 0.0987,
        "payment_method": 0.0855,
        "total_charges": -0.0423
      }
    },
    {
      "customer_id": "c-002",
      "prediction": "retain",
      "probability": {
        "churn": 0.0812,
        "retain": 0.9188
      },
      "feature_contributions": {
        "contract_type": -0.3456,
        "support_tickets": -0.1234,
        "tenure_months": -0.2345,
        "monthly_charges": 0.0456,
        "payment_method": -0.0567,
        "total_charges": 0.0123
      }
    }
  ],
  "metadata": {
    "prediction_time_ms": 12,
    "model_loaded_at": "2024-01-15T08:00:00Z",
    "request_id": "req-d5e6f7g8"
  }
}
```

### Prediction Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `include_probabilities` | boolean | `false` | Include class probabilities in the response |
| `include_feature_contributions` | boolean | `false` | Include SHAP or permutation feature importance |
| `include_prediction_interval` | boolean | `false` | Include confidence intervals for regression |
| `prediction_interval_level` | float | `0.95` | Confidence level for prediction intervals |
| `explanation_method` | string | `shap` | Feature contribution method: `shap`, `permutation`, `lime` |

### Auto-Version Resolution

When `model_version` is omitted, the API resolves the version based on the model's stage:

```json
{
  "model_name": "customer_churn_model",
  "model_stage": "Production",
  "predictions": [...]
}
```

| Stage | Resolution |
|-------|-----------|
| `Production` | Uses the latest version in Production stage |
| `Staging` | Uses the latest version in Staging stage |
| `Latest` | Uses the highest version number regardless of stage |
| `Archived` | Not available for prediction (returns 400 error) |

---

## Model Registry API

The Model Registry provides CRUD operations for managing model lifecycle, versions, and stage transitions.

### POST /api/v1/ml/models

Register a new model in the registry.

**Request Body**:

```json
{
  "name": "customer_churn_model",
  "description": "XGBoost classifier for predicting customer churn based on usage and billing patterns",
  "tags": {
    "team": "data-science",
    "domain": "customer-analytics",
    "algorithm": "xgboost",
    "task_type": "binary_classification"
  },
  "schema": {
    "input_schema": {
      "type": "object",
      "properties": {
        "tenure_months": {"type": "integer"},
        "monthly_charges": {"type": "number"},
        "total_charges": {"type": "number"},
        "contract_type": {"type": "string"},
        "payment_method": {"type": "string"},
        "support_tickets": {"type": "integer"}
      }
    },
    "output_schema": {
      "type": "object",
      "properties": {
        "prediction": {"type": "string", "enum": ["churn", "retain"]},
        "probability": {"type": "number"}
      }
    }
  }
}
```

**Response** (201 Created):

```json
{
  "name": "customer_churn_model",
  "description": "XGBoost classifier for predicting customer churn based on usage and billing patterns",
  "created_at": "2024-01-15T10:30:00.000Z",
  "updated_at": "2024-01-15T10:30:00.000Z",
  "latest_version": null,
  "tags": {"team": "data-science", "domain": "customer-analytics"},
  "links": {
    "self": "/api/v1/ml/models/customer_churn_model",
    "versions": "/api/v1/ml/models/customer_churn_model/versions"
  }
}
```

### GET /api/v1/ml/models

List all registered models.

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | integer | 1 | Page number |
| `page_size` | integer | 20 | Items per page (max: 100) |
| `filter` | string | - | Filter by name pattern or tag |
| `sort` | string | `name` | Sort by: `name`, `created_at`, `updated_at` |

**Response** (200 OK):

```json
{
  "models": [
    {
      "name": "customer_churn_model",
      "description": "XGBoost classifier for customer churn",
      "latest_version": 3,
      "production_version": 2,
      "staging_version": 3,
      "created_at": "2024-01-01T00:00:00Z",
      "updated_at": "2024-01-15T10:30:00Z",
      "tags": {"team": "data-science"}
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 15,
    "total_pages": 1
  }
}
```

### GET /api/v1/ml/models/{model_name}

Get details of a specific model.

### PATCH /api/v1/ml/models/{model_name}

Update model metadata (description, tags).

### DELETE /api/v1/ml/models/{model_name}

Delete a model and all its versions. Requires `ml:admin` scope.

### GET /api/v1/ml/models/{model_name}/versions

List all versions of a model.

**Response** (200 OK):

```json
{
  "model_name": "customer_churn_model",
  "versions": [
    {
      "version": 3,
      "stage": "Staging",
      "description": "XGBoost v3 with hyperopt tuning",
      "algorithm": "xgboost",
      "metrics": {
        "test_auc": 0.9087,
        "test_f1": 0.8598,
        "test_accuracy": 0.8798
      },
      "run_id": "a1b2c3d4e5f6g7h8",
      "created_at": "2024-01-15T10:44:17Z",
      "created_by": "user@thunders.io",
      "artifact_size_bytes": 524288,
      "links": {
        "self": "/api/v1/ml/models/customer_churn_model/versions/3",
        "promote": "/api/v1/ml/models/customer_churn_model/versions/3/promote",
        "predict": "/api/v1/ml/predict"
      }
    },
    {
      "version": 2,
      "stage": "Production",
      "description": "XGBoost v2 with feature engineering",
      "metrics": {
        "test_auc": 0.8956,
        "test_f1": 0.8412
      },
      "created_at": "2024-01-10T14:20:00Z"
    },
    {
      "version": 1,
      "stage": "Archived",
      "description": "Initial baseline model",
      "metrics": {
        "test_auc": 0.8234,
        "test_f1": 0.7856
      },
      "created_at": "2024-01-05T09:00:00Z"
    }
  ]
}
```

### POST /api/v1/ml/models/{model_name}/versions/{version}/promote

Transition a model version to a new stage.

**Request Body**:

```json
{
  "target_stage": "Production",
  "archive_existing_versions": true
}
```

| Stage | Description |
|-------|-------------|
| `None` | Newly registered, not yet validated |
| `Staging` | Validated and ready for pre-production testing |
| `Production` | Deployed for live predictions |
| `Archived` | Deprecated and no longer in use |

---

## Feature Store API

The Feature Store provides centralized feature definition, computation, and serving for ML models.

### POST /api/v1/ml/features/define

Define a feature group with its schema and computation logic.

**Request Body**:

```json
{
  "feature_group_name": "customer_billing_features",
  "description": "Aggregated billing features for customer analytics",
  "entity_keys": ["customer_id"],
  "features": [
    {
      "name": "avg_monthly_charges_6m",
      "type": "double",
      "description": "Average monthly charges over the last 6 months"
    },
    {
      "name": "charge_trend_6m",
      "type": "double",
      "description": "Linear trend coefficient of charges over 6 months"
    },
    {
      "name": "payment_delay_days_avg",
      "type": "double",
      "description": "Average payment delay in days"
    },
    {
      "name": "payment_method_changes_12m",
      "type": "integer",
      "description": "Number of payment method changes in last 12 months"
    }
  ],
  "source": {
    "type": "sql",
    "query": "SELECT customer_id, AVG(monthly_charges) AS avg_monthly_charges_6m, ... FROM billing WHERE billing_date >= DATE_SUB(CURRENT_DATE, 180) GROUP BY customer_id"
  },
  "schedule": "cron(0 2 * * ? *)",
  "ttl_hours": 24,
  "tags": {
    "domain": "billing",
    "team": "data-engineering"
  }
}
```

**Response** (201 Created):

```json
{
  "feature_group_name": "customer_billing_features",
  "status": "ACTIVE",
  "entity_keys": ["customer_id"],
  "feature_count": 4,
  "last_computed_at": null,
  "schedule": "cron(0 2 * * ? *)",
  "ttl_hours": 24,
  "created_at": "2024-01-15T10:30:00.000Z"
}
```

### POST /api/v1/ml/features/compute

Trigger on-demand feature computation for a feature group.

```json
{
  "feature_group_name": "customer_billing_features",
  "entity_ids": ["c-001", "c-002", "c-003"],
  "feature_names": ["avg_monthly_charges_6m", "charge_trend_6m"]
}
```

### POST /api/v1/ml/features/serve

Retrieve feature vectors for one or more entities, optimized for low-latency online serving.

**Request Body**:

```json
{
  "feature_groups": ["customer_billing_features", "customer_usage_features"],
  "entity_ids": ["c-001", "c-002"],
  "feature_names": ["avg_monthly_charges_6m", "charge_trend_6m", "daily_usage_minutes"]
}
```

**Response** (200 OK):

```json
{
  "features": [
    {
      "entity_id": "c-001",
      "features": {
        "avg_monthly_charges_6m": 85.50,
        "charge_trend_6m": 2.34,
        "daily_usage_minutes": 45.2
      },
      "computed_at": "2024-01-15T02:00:00Z"
    },
    {
      "entity_id": "c-002",
      "features": {
        "avg_monthly_charges_6m": 55.00,
        "charge_trend_6m": -1.12,
        "daily_usage_minutes": 22.8
      },
      "computed_at": "2024-01-15T02:00:00Z"
    }
  ]
}
```

### GET /api/v1/ml/features/groups

List all feature groups.

### GET /api/v1/ml/features/groups/{feature_group_name}

Get details and statistics for a specific feature group.

---

## Experiment Tracking API

The Experiment Tracking API integrates with MLflow to manage experiments, log parameters and metrics, and compare training runs.

### POST /api/v1/ml/experiments

Create a new experiment.

**Request Body**:

```json
{
  "name": "customer-churn-prediction",
  "description": "Experiments for predicting customer churn using various classification algorithms",
  "tags": {
    "domain": "customer-analytics",
    "owner": "data-science"
  },
  "artifact_location": "s3://thunders-bigdata-artifacts/experiments/customer-churn/"
}
```

**Response** (201 Created):

```json
{
  "experiment_id": "42",
  "name": "customer-churn-prediction",
  "description": "Experiments for predicting customer churn using various classification algorithms",
  "artifact_location": "s3://thunders-bigdata-artifacts/experiments/customer-churn/",
  "lifecycle_stage": "active",
  "creation_time": "2024-01-15T10:30:00.000Z",
  "last_update_time": "2024-01-15T10:30:00.000Z",
  "run_count": 0,
  "links": {
    "self": "/api/v1/ml/experiments/42",
    "runs": "/api/v1/ml/experiments/42/runs",
    "compare": "/api/v1/ml/experiments/42/compare",
    "mlflow_ui": "https://mlflow.thunders.example.com/#/experiments/42"
  }
}
```

### GET /api/v1/ml/experiments/{experiment_id}/runs

List all runs for an experiment.

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | string | - | Filter by status: `RUNNING`, `FINISHED`, `FAILED`, `KILLED` |
| `order_by` | string | `start_time DESC` | Sort field |
| `max_results` | integer | 50 | Maximum runs to return |

**Response** (200 OK):

```json
{
  "experiment_id": "42",
  "runs": [
    {
      "run_id": "a1b2c3d4e5f6g7h8",
      "run_name": "xgboost-v3-hyperopt",
      "status": "FINISHED",
      "start_time": "2024-01-15T10:30:15Z",
      "end_time": "2024-01-15T10:44:17Z",
      "duration_seconds": 842,
      "metrics": {
        "train_auc": 0.9234,
        "val_auc": 0.9123,
        "test_auc": 0.9087
      },
      "params": {
        "algorithm": "xgboost",
        "max_depth": "8",
        "learning_rate": "0.05",
        "n_estimators": "500"
      },
      "tags": {
        "model_name": "customer_churn_model",
        "framework": "xgboost"
      },
      "artifact_uris": {
        "model": "s3://thunders-bigdata-artifacts/experiments/customer-churn/a1b2c3d4/model",
        "confusion_matrix": "s3://thunders-bigdata-artifacts/experiments/customer-churn/a1b2c3d4/confusion_matrix.png"
      }
    }
  ],
  "total_count": 12
}
```

### POST /api/v1/ml/experiments/{experiment_id}/compare

Compare metrics across multiple runs side by side.

**Request Body**:

```json
{
  "run_ids": ["a1b2c3d4e5f6g7h8", "i9j0k1l2m3n4o5p6", "q7r8s9t0u1v2w3x4"],
  "metrics": ["test_auc", "test_f1", "test_accuracy"],
  "params": ["max_depth", "learning_rate", "n_estimators"]
}
```

**Response** (200 OK):

```json
{
  "comparison": {
    "metrics": {
      "test_auc": {
        "a1b2c3d4": 0.9087,
        "i9j0k1l2": 0.8956,
        "q7r8s9t0": 0.8723
      },
      "test_f1": {
        "a1b2c3d4": 0.8598,
        "i9j0k1l2": 0.8412,
        "q7r8s9t0": 0.8156
      }
    },
    "params": {
      "max_depth": {
        "a1b2c3d4": "8",
        "i9j0k1l2": "6",
        "q7r8s9t0": "10"
      },
      "learning_rate": {
        "a1b2c3d4": "0.05",
        "i9j0k1l2": "0.1",
        "q7r8s9t0": "0.01"
      }
    },
    "best_run": {
      "by_test_auc": "a1b2c3d4",
      "by_test_f1": "a1b2c3d4"
    }
  }
}
```

### GET /api/v1/ml/experiments/{experiment_id}/runs/{run_id}/metrics/history

Get the time-series history of a metric for a specific run (useful for training curves).

**Response** (200 OK):

```json
{
  "run_id": "a1b2c3d4e5f6g7h8",
  "metric": "train_auc",
  "history": [
    {"step": 1, "value": 0.7823, "timestamp": "2024-01-15T10:31:00Z"},
    {"step": 2, "value": 0.8345, "timestamp": "2024-01-15T10:33:00Z"},
    {"step": 3, "value": 0.8612, "timestamp": "2024-01-15T10:35:00Z"},
    {"step": 4, "value": 0.8878, "timestamp": "2024-01-15T10:37:00Z"},
    {"step": 5, "value": 0.9012, "timestamp": "2024-01-15T10:39:00Z"},
    {"step": 6, "value": 0.9123, "timestamp": "2024-01-15T10:41:00Z"},
    {"step": 7, "value": 0.9189, "timestamp": "2024-01-15T10:43:00Z"},
    {"step": 8, "value": 0.9234, "timestamp": "2024-01-15T10:44:00Z"}
  ]
}
```

---

## Batch vs Real-Time Inference

Thunders-BigData-System supports two inference modes: batch (offline) and real-time (online). Choose the appropriate mode based on your latency requirements and data freshness needs.

### Batch Inference

Batch inference processes large datasets offline on a schedule. It is ideal for scenarios where predictions are not time-critical and can be computed ahead of time.

**POST /api/v1/ml/predict/batch**

**Request Body**:

```json
{
  "model_name": "customer_churn_model",
  "model_version": 3,
  "source": {
    "type": "dataset",
    "dataset": "customer_features",
    "filter": "last_activity_date >= DATE_SUB(CURRENT_DATE, 30)"
  },
  "output": {
    "type": "dataset",
    "dataset": "customer_churn_predictions",
    "mode": "overwrite",
    "partition_by": ["prediction_date"]
  },
  "options": {
    "include_probabilities": true,
    "include_feature_contributions": true,
    "batch_size": 10000
  },
  "schedule": "cron(0 6 * * ? *)"
}
```

**Response** (202 Accepted):

```json
{
  "job_id": "batch-y1z2a3b4",
  "model_name": "customer_churn_model",
  "model_version": 3,
  "status": "QUEUED",
  "estimated_records": 500000,
  "estimated_duration_minutes": 10,
  "schedule": "cron(0 6 * * ? *)",
  "links": {
    "status": "/api/v1/ml/predict/batch/batch-y1z2a3b4"
  }
}
```

### Real-Time Inference

Real-time inference provides sub-100ms latency for individual predictions, served via the REST API or gRPC endpoint.

**REST API** (shown in Prediction API above):

Typical latency: 5-20ms per prediction

**gRPC Endpoint**:

For lowest-latency inference, use the gRPC endpoint:

```python
import grpc
from thunders.ml.v1 import prediction_pb2, prediction_pb2_grpc

channel = grpc.insecure_channel('api.thunders.example.com:443')
stub = prediction_pb2_grpc.PredictionServiceStub(channel)

request = prediction_pb2.PredictRequest(
    model_name="customer_churn_model",
    model_version=3,
    instances=[
        prediction_pb2.Instance(
            features={
                "tenure_months": 24,
                "monthly_charges": 85.50,
                "total_charges": 2052.00,
                "contract_type": "month-to-month",
                "payment_method": "electronic_check",
                "support_tickets": 3
            }
        )
    ],
    options=prediction_pb2.PredictOptions(
        include_probabilities=True,
        include_feature_contributions=False
    )
)

response = stub.Predict(request)
print(f"Prediction: {response.predictions[0].label}")
print(f"Probability: {response.predictions[0].probability}")
```

Typical latency: 1-5ms per prediction

### Comparison

| Aspect | Batch Inference | Real-Time Inference |
|--------|----------------|-------------------|
| Latency | Minutes to hours | 1-20ms |
| Throughput | Millions per hour | Thousands per second |
| Cost | Lower (uses spot instances) | Higher (always-on serving) |
| Data freshness | Scheduled (e.g., daily) | On-demand (latest features) |
| Use cases | Scoring, batch recommendations | Real-time personalization, fraud detection |
| Compute | Spark cluster | Dedicated serving pods |

---

## Model Versioning and A/B Testing

### Model Versioning

Every model in the registry supports multiple versions with stage-based lifecycle management:

```
v1 (Archived) → v2 (Production) → v3 (Staging)
                                   ↓
                              Promote to Production
                                   ↓
                              v2 (Archived), v3 (Production)
```

### A/B Testing

Route a percentage of prediction traffic to different model versions for evaluation.

**POST /api/v1/ml/ab-tests**

**Request Body**:

```json
{
  "name": "churn-model-v2-vs-v3",
  "model_name": "customer_churn_model",
  "variants": [
    {
      "name": "control",
      "model_version": 2,
      "traffic_percentage": 80,
      "description": "Current production model (XGBoost v2)"
    },
    {
      "name": "treatment",
      "model_version": 3,
      "traffic_percentage": 20,
      "description": "Candidate model (XGBoost v3 with hyperopt)"
    }
  ],
  "routing_strategy": "random",
  "success_metric": "prediction_accuracy",
  "minimum_sample_size": 10000,
  "confidence_level": 0.95,
  "minimum_effect_size": 0.01,
  "duration_days": 14,
  "tags": {
    "owner": "data-science",
    "jira": "DS-1234"
  }
}
```

**Response** (201 Created):

```json
{
  "name": "churn-model-v2-vs-v3",
  "status": "ACTIVE",
  "model_name": "customer_churn_model",
  "variants": [
    {"name": "control", "model_version": 2, "traffic_percentage": 80},
    {"name": "treatment", "model_version": 3, "traffic_percentage": 20}
  ],
  "started_at": "2024-01-15T10:30:00.000Z",
  "ends_at": "2024-01-29T10:30:00.000Z",
  "links": {
    "self": "/api/v1/ml/ab-tests/churn-model-v2-vs-v3",
    "results": "/api/v1/ml/ab-tests/churn-model-v2-vs-v3/results",
    "stop": "/api/v1/ml/ab-tests/churn-model-v2-vs-v3/stop"
  }
}
```

### Check A/B Test Results

```
GET /api/v1/ml/ab-tests/{test_name}/results
```

**Response** (200 OK):

```json
{
  "test_name": "churn-model-v2-vs-v3",
  "status": "ACTIVE",
  "results": {
    "control": {
      "model_version": 2,
      "sample_size": 80000,
      "metrics": {
        "prediction_accuracy": 0.8798,
        "avg_prediction_latency_ms": 8.2,
        "auc": 0.8956
      },
      "confidence_interval": {
        "prediction_accuracy": [0.8772, 0.8824]
      }
    },
    "treatment": {
      "model_version": 3,
      "sample_size": 20000,
      "metrics": {
        "prediction_accuracy": 0.9012,
        "avg_prediction_latency_ms": 9.1,
        "auc": 0.9087
      },
      "confidence_interval": {
        "prediction_accuracy": [0.8968, 0.9056]
      }
    },
    "statistical_significance": {
      "p_value": 0.0012,
      "is_significant": true,
      "effect_size": 0.0214,
      "winner": "treatment"
    }
  },
  "recommendation": "Promote treatment (v3) to production. The improvement of +2.14% accuracy is statistically significant (p=0.0012).",
  "updated_at": "2024-01-25T10:30:00Z"
}
```

### Shadow Deployment

For risk-free testing, deploy a model in shadow mode where predictions are logged but not served to users:

```json
{
  "name": "churn-model-v3-shadow",
  "model_name": "customer_churn_model",
  "variants": [
    {
      "name": "production",
      "model_version": 2,
      "traffic_percentage": 100,
      "served": true
    },
    {
      "name": "shadow",
      "model_version": 3,
      "traffic_percentage": 100,
      "served": false
    }
  ],
  "routing_strategy": "mirror"
}
```

In shadow/mirror mode, 100% of traffic goes to the production model for actual responses, while the shadow model receives a mirrored copy of all requests. Its predictions are logged for offline comparison without impacting end users.

---

## See Also

- [Ingestion API Reference](ingestion_api.md) - Data ingestion endpoints
- [Analytics API Reference](analytics_api.md) - Query and analytics API
- [REST API Documentation](../api/rest_api.md) - General REST API overview
- [Model Registry Guide](../../src/python/machine_learning/model_registry.py) - Model registry implementation
