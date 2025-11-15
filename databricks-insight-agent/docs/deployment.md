# Deployment Guide

This guide covers deploying the Databricks Insight Agent in various environments.

## Prerequisites

### System Requirements
- Databricks Workspace with system table access
- Python 3.8 or higher
- OpenAI API access
- SMTP server (for email alerts)

### Permissions
- Databricks workspace admin or equivalent
- Access to system tables
- Secret management permissions
- Job creation permissions

## Databricks Deployment

### 1. Upload Project Files

Upload the project files to Databricks DBFS:

```bash
# Upload source code
databricks fs cp src/ dbfs:/databricks-insight-agent/src/ --recursive

# Upload configuration
databricks fs cp .env.example dbfs:/databricks-insight-agent/.env.example

# Upload deployment files
databricks fs cp databricks/ dbfs:/databricks-insight-agent/databricks/ --recursive
```

### 2. Configure Secrets

Create secrets in Databricks Secret Scope:

```bash
# Create secret scope
databricks secrets create-scope insight-agent

# Store credentials
databricks secrets put --scope insight-agent --key openai_api_key
databricks secrets put --scope insight-agent --key databricks_token
databricks secrets put --scope insight-agent --key smtp_user
databricks secrets put --scope insight-agent --key smtp_pass
```

### 3. Deploy Job

Create a Databricks job using the provided job configuration:

```bash
# Using Databricks CLI
databricks jobs create --json-file databricks/job.json

# Or through the UI:
# 1. Go to Workflows > Jobs
# 2. Click "Create Job"
# 3. Configure using the JSON specification
```

### 4. Deploy Workflow (Advanced)

For complex multi-task workflows:

```bash
databricks jobs create --json-file databricks/workflow.json
```

### 5. Configure Cluster

Ensure the cluster has:
- Compatible Databricks Runtime (14.3+)
- Required libraries installed
- Proper environment variables

## Local Development

### 1. Environment Setup

```bash
# Clone repository
git clone <repository-url>
cd databricks-insight-agent

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy environment file
cp .env.example .env

# Edit configuration
nano .env
```

Required environment variables:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-token
OPENAI_API_KEY=your-openai-api-key
```

### 3. Run Locally

```bash
# Run single analysis
python src/main.py --mode once --hours 24

# Run scheduled analysis
python src/main.py --mode scheduled --interval 6
```

## Docker Deployment (Optional)

### 1. Build Image

```dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY .env.example ./.env

CMD ["python", "src/main.py"]
```

```bash
docker build -t databricks-insight-agent .
```

### 2. Run Container

```bash
docker run -e DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
           -e OPENAI_API_KEY=$OPENAI_API_KEY \
           databricks-insight-agent
```

## Cloud Deployment

### AWS

```yaml
# AWS CloudFormation template excerpt
Resources:
  InsightAgentTask:
    Type: AWS::ECS::TaskDefinition
    Properties:
      ContainerDefinitions:
        - Name: insight-agent
          Image: databricks-insight-agent:latest
          Environment:
            - Name: DATABRICKS_HOST
              Value: !Ref DatabricksHost
            - Name: OPENAI_API_KEY
              Value: !Ref OpenAIKey
```

### Azure

```json
{
  "type": "Microsoft.ContainerInstance/containerGroups",
  "properties": {
    "containers": [{
      "name": "insight-agent",
      "properties": {
        "image": "databricks-insight-agent:latest",
        "environmentVariables": [
          {"name": "DATABRICKS_HOST", "value": "[parameters('databricksHost')]"},
          {"name": "OPENAI_API_KEY", "value": "[parameters('openaiKey')]"}
        ]
      }
    }]
  }
}
```

### GCP

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: insight-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: insight-agent
  template:
    metadata:
      labels:
        app: insight-agent
    spec:
      containers:
      - name: insight-agent
        image: databricks-insight-agent:latest
        env:
        - name: DATABRICKS_HOST
          valueFrom:
            secretKeyRef:
              name: insight-agent-secrets
              key: databricks-host
        - name: OPENAI_API_KEY
          valueFrom:
            secretKeyRef:
              name: insight-agent-secrets
              key: openai-key
```

## Configuration Management

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Databricks access token | Yes |
| `OPENAI_API_KEY` | OpenAI API key | Yes |
| `MLFLOW_TRACKING_URI` | MLflow tracking URI | No |
| `SMTP_SERVER` | SMTP server for alerts | No |
| `ALERT_EMAILS` | Email addresses for alerts | No |

### Advanced Configuration

```python
# Custom configuration
from src.utils.config import config

config.set('ANALYSIS_TIMEOUT', '1800')
config.set('BATCH_SIZE', '500')
config.set('LOG_LEVEL', 'DEBUG')
```

## Monitoring & Maintenance

### Health Checks

```python
from src.main import DatabricksInsightAgent

agent = DatabricksInsightAgent()
health = await agent.health_check()
print(health)
```

### Log Management

Logs are stored in:
- Databricks: Job run logs
- Local: `logs/` directory
- Cloud: Cloud logging services

### Backup & Recovery

- Configuration files
- MLflow experiments
- Generated reports
- System state snapshots

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify Databricks credentials
   - Check network connectivity
   - Validate token permissions

2. **OpenAI API Errors**
   - Check API key validity
   - Verify rate limits
   - Monitor API usage

3. **Memory Issues**
   - Increase cluster size
   - Reduce analysis window
   - Implement data pagination

4. **Permission Errors**
   - Verify system table access
   - Check secret scope permissions
   - Validate job execution permissions

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
python src/main.py --mode once --hours 1
```

### Performance Tuning

- Adjust `BATCH_SIZE` for data processing
- Modify `MAX_WORKERS` for parallel processing
- Configure `ANALYSIS_TIMEOUT` for long-running jobs
- Use appropriate cluster sizes

## Security Best Practices

- Store secrets in secure vaults
- Use least-privilege access
- Enable audit logging
- Regular security updates
- Network isolation
- Data encryption at rest and in transit