# Databricks notebook source
# Databricks Insight Agent - Security Analysis Notebook
# This notebook runs the comprehensive security and network analysis

# COMMAND ----------

# Install required packages (if not already in cluster)
# %pip install databricks-sdk dspy-ai mlflow pandas openai structlog pydantic

# COMMAND ----------

import sys
import os
from pathlib import Path

# Add project files to path (assuming uploaded to DBFS)
project_path = "/dbfs/databricks-insight-agent/src"
if project_path not in sys.path:
    sys.path.append(project_path)

# COMMAND ----------

# Setup logging
from utils.logging import setup_logging
setup_logging(log_level="INFO")

# COMMAND ----------

# Initialize configuration
from utils.config import config

# Enable mock data mode for free edition (no system tables access)
config.set('USE_MOCK_DATA', 'true')

# Override with Databricks secrets/widgets if needed
# config.set('OPENAI_API_KEY', dbutils.secrets.get(scope="insight-agent", key="openai_api_key"))
# config.set('DATABRICKS_TOKEN', dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

# COMMAND ----------

# Import agents
from agents.orchestrator import OrchestratorAgent
from agents.reporting import ReportingAgent

# COMMAND ----------

# Initialize orchestrator
orchestrator = OrchestratorAgent()
reporting = ReportingAgent()

print("Agents initialized successfully")

# COMMAND ----------

# Run analysis
# Get parameters from widgets or defaults
hours_back = dbutils.widgets.get("hours_back") if dbutils.widgets.get("hours_back") else 24
hours_back = int(hours_back)

print(f"Running analysis for last {hours_back} hours...")

# COMMAND ----------

import asyncio

async def run_analysis():
    try:
        # Run full analysis
        results = await orchestrator.run_full_analysis(hours_back)

        # Generate report
        report_result = await reporting.generate_report(results)

        print("Analysis completed successfully!")
        print(f"Overall threat level: {results.get('overall_threat_level')}")
        print(f"Report saved to: {report_result['metadata']['file_path']}")

        # Display key findings
        print("\n=== KEY FINDINGS ===")
        if 'network_analysis' in results and 'key_findings' in results['network_analysis']:
            print("Network Analysis:")
            for finding in results['network_analysis']['key_findings'][:3]:
                print(f"- {finding}")

        if 'security_analysis' in results and 'security_findings' in results['security_analysis']:
            print("\nSecurity Analysis:")
            for finding in results['security_analysis']['security_findings'][:3]:
                print(f"- {finding}")

        return results

    except Exception as e:
        print(f"Analysis failed: {str(e)}")
        raise

# Run the analysis
results = asyncio.run(run_analysis())

# COMMAND ----------

# Display results summary
if results:
    print("\n=== ANALYSIS SUMMARY ===")
    print(f"Data Events Analyzed: {results.get('data_summary', {}).get('audit_events', 0)}")
    print(f"Active Clusters: {results.get('data_summary', {}).get('clusters', 0)}")
    print(f"Query Executions: {results.get('data_summary', {}).get('queries', 0)}")
    print(f"Analysis Duration: {results.get('metadata', {}).get('analysis_duration', 0):.2f} seconds")

    if 'evaluation' in results:
        eval_data = results['evaluation']
        print(f"Overall Score: {eval_data.get('overall_score', 0):.2f}/10")
        print(f"Recommendations: {len(eval_data.get('recommendations', []))}")

# COMMAND ----------

# Export results to table (optional)
# spark.createDataFrame([results]).write.mode("overwrite").saveAsTable("insight_agent.results")

print("Notebook execution completed!")

# COMMAND ----------