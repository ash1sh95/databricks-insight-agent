import os
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query
import structlog
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
import random
import uuid

from src.utils.config import config

logger = structlog.get_logger(__name__)

class DataIngestionAgent:
    """
    Agent responsible for ingesting data from Databricks system tables.
    Focuses on audit logs, cluster metrics, and operational data.
    """

    def __init__(self, host: Optional[str] = None, token: Optional[str] = None):
        self.host = host or os.getenv('DATABRICKS_HOST')
        self.token = token or os.getenv('DATABRICKS_TOKEN')
        self.workspace_client = None
        self.system_tables = {
            'audit': 'system.access.audit',
            'clusters': 'system.compute.clusters',
            'jobs': 'system.jobs.jobs',
            'query_history': 'system.access.query_history',
            'node_types': 'system.compute.node_types'
        }

    async def connect(self) -> bool:
        """Establish connection to Databricks workspace."""
        if config.get_bool('USE_MOCK_DATA'):
            logger.info("Using mock data mode - skipping Databricks connection")
            return True

        try:
            self.workspace_client = WorkspaceClient(
                host=self.host,
                token=self.token
            )
            # Test connection
            self.workspace_client.current_user.me()
            logger.info("Successfully connected to Databricks workspace")
            return True
        except Exception as e:
            logger.error("Failed to connect to Databricks", error=str(e))
            return False

    async def fetch_audit_logs(self, hours_back: int = 24) -> pd.DataFrame:
        """Fetch audit logs from the specified time window."""
        if config.get_bool('USE_MOCK_DATA'):
            return self._generate_mock_audit_logs(hours_back)

        if not self.workspace_client:
            raise ConnectionError("Not connected to Databricks")

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        query = f"""
        SELECT
            event_time,
            user_id,
            service_name,
            action_name,
            request_params,
            response,
            session_id,
            source_ip_address,
            user_agent
        FROM {self.system_tables['audit']}
        WHERE event_time BETWEEN '{start_time.isoformat()}' AND '{end_time.isoformat()}'
        ORDER BY event_time DESC
        """

        try:
            result = self.workspace_client.query.execute(
                warehouse_id=self._get_warehouse_id(),
                query=Query(query_text=query)
            )

            # Convert to DataFrame
            data = []
            for row in result.result.data_array:
                data.append({
                    'event_time': row[0],
                    'user_id': row[1],
                    'service_name': row[2],
                    'action_name': row[3],
                    'request_params': row[4],
                    'response': row[5],
                    'session_id': row[6],
                    'source_ip_address': row[7],
                    'user_agent': row[8]
                })

            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} audit log entries")
            return df

        except Exception as e:
            logger.error("Failed to fetch audit logs", error=str(e))
            return pd.DataFrame()

    async def fetch_cluster_metrics(self) -> pd.DataFrame:
        """Fetch current cluster status and metrics."""
        if config.get_bool('USE_MOCK_DATA'):
            return self._generate_mock_cluster_metrics()

        if not self.workspace_client:
            raise ConnectionError("Not connected to Databricks")

        query = f"""
        SELECT
            cluster_id,
            cluster_name,
            state,
            cluster_source,
            creator_user_name,
            start_time,
            terminated_time,
            last_activity_time
        FROM {self.system_tables['clusters']}
        WHERE state IN ('RUNNING', 'PENDING', 'TERMINATING')
        """

        try:
            result = self.workspace_client.query.execute(
                warehouse_id=self._get_warehouse_id(),
                query=Query(query_text=query)
            )

            data = []
            for row in result.result.data_array:
                data.append({
                    'cluster_id': row[0],
                    'cluster_name': row[1],
                    'state': row[2],
                    'cluster_source': row[3],
                    'creator_user_name': row[4],
                    'start_time': row[5],
                    'terminated_time': row[6],
                    'last_activity_time': row[7]
                })

            df = pd.DataFrame(data)
            logger.info(f"Fetched metrics for {len(df)} clusters")
            return df

        except Exception as e:
            logger.error("Failed to fetch cluster metrics", error=str(e))
            return pd.DataFrame()

    async def fetch_query_history(self, hours_back: int = 24) -> pd.DataFrame:
        """Fetch query execution history."""
        if config.get_bool('USE_MOCK_DATA'):
            return self._generate_mock_query_history(hours_back)

        if not self.workspace_client:
            raise ConnectionError("Not connected to Databricks")

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        query = f"""
        SELECT
            query_id,
            query_text,
            user_id,
            session_id,
            executed_by,
            start_time,
            end_time,
            duration_ms,
            status,
            error_message
        FROM {self.system_tables['query_history']}
        WHERE start_time BETWEEN '{start_time.isoformat()}' AND '{end_time.isoformat()}'
        ORDER BY start_time DESC
        """

        try:
            result = self.workspace_client.query.execute(
                warehouse_id=self._get_warehouse_id(),
                query=Query(query_text=query)
            )

            data = []
            for row in result.result.data_array:
                data.append({
                    'query_id': row[0],
                    'query_text': row[1],
                    'user_id': row[2],
                    'session_id': row[3],
                    'executed_by': row[4],
                    'start_time': row[5],
                    'end_time': row[6],
                    'duration_ms': row[7],
                    'status': row[8],
                    'error_message': row[9]
                })

            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} query history entries")
            return df

        except Exception as e:
            logger.error("Failed to fetch query history", error=str(e))
            return pd.DataFrame()

    def _get_warehouse_id(self) -> str:
        """Get the default SQL warehouse ID."""
        # In production, this should be configured
        return os.getenv('DATABRICKS_WAREHOUSE_ID', 'default_warehouse')

    def _generate_mock_audit_logs(self, hours_back: int = 24) -> pd.DataFrame:
        """Generate mock audit log data for testing."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        services = ['sql', 'clusters', 'jobs', 'dbfs', 'secrets', 'mlflow', 'notebooks']
        actions = ['executeQuery', 'startCluster', 'runJob', 'createSecret', 'getSecret', 'logModel', 'runNotebook']
        users = ['user1@databricks.com', 'user2@databricks.com', 'admin@databricks.com']
        ips = ['192.168.1.1', '10.0.0.1', '203.0.113.1', '127.0.0.1', '172.16.0.1']

        data = []
        num_entries = random.randint(50, 200)

        for _ in range(num_entries):
            event_time = start_time + timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
            data.append({
                'event_time': event_time.isoformat(),
                'user_id': random.choice(users),
                'service_name': random.choice(services),
                'action_name': random.choice(actions),
                'request_params': '{"param1": "value1"}',
                'response': '{"status": "success"}',
                'session_id': str(uuid.uuid4()),
                'source_ip_address': random.choice(ips),
                'user_agent': 'Databricks/1.0'
            })

        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} mock audit log entries")
        return df

    def _generate_mock_cluster_metrics(self) -> pd.DataFrame:
        """Generate mock cluster metrics data for testing."""
        states = ['RUNNING', 'PENDING', 'TERMINATING']
        sources = ['UI', 'JOB', 'API']
        users = ['user1@databricks.com', 'user2@databricks.com', 'admin@databricks.com']
        names = ['cluster-1', 'cluster-2', 'ml-cluster', 'job-cluster-1']

        data = []
        num_clusters = random.randint(2, 5)

        for i in range(num_clusters):
            start_time = datetime.utcnow() - timedelta(hours=random.randint(1, 24))
            data.append({
                'cluster_id': f'cluster-{i+1:03d}',
                'cluster_name': random.choice(names),
                'state': random.choice(states),
                'cluster_source': random.choice(sources),
                'creator_user_name': random.choice(users),
                'start_time': start_time.isoformat(),
                'terminated_time': None,
                'last_activity_time': (start_time + timedelta(minutes=random.randint(0, 60))).isoformat()
            })

        df = pd.DataFrame(data)
        logger.info(f"Generated mock metrics for {len(df)} clusters")
        return df

    def _generate_mock_query_history(self, hours_back: int = 24) -> pd.DataFrame:
        """Generate mock query history data for testing."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        users = ['user1@databricks.com', 'user2@databricks.com', 'admin@databricks.com']
        statuses = ['FINISHED', 'RUNNING', 'FAILED', 'CANCELED']
        queries = [
            'SELECT * FROM table1',
            'INSERT INTO table2 VALUES (1, "test")',
            'UPDATE table3 SET col1 = "value"',
            'DELETE FROM table4 WHERE id = 1'
        ]

        data = []
        num_queries = random.randint(20, 100)

        for _ in range(num_queries):
            start = start_time + timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
            duration = random.randint(100, 10000)
            end = start + timedelta(milliseconds=duration)
            status = random.choice(statuses)

            data.append({
                'query_id': str(uuid.uuid4()),
                'query_text': random.choice(queries),
                'user_id': random.choice(users),
                'session_id': str(uuid.uuid4()),
                'executed_by': random.choice(users),
                'start_time': start.isoformat(),
                'end_time': end.isoformat() if status == 'FINISHED' else None,
                'duration_ms': duration,
                'status': status,
                'error_message': 'Query failed' if status == 'FAILED' else None
            })

        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} mock query history entries")
        return df

    async def collect_all_data(self, hours_back: int = 24) -> Dict[str, pd.DataFrame]:
        """Collect data from all relevant system tables."""
        logger.info("Starting data collection from Databricks system tables")

        data = {
            'audit_logs': await self.fetch_audit_logs(hours_back),
            'cluster_metrics': await self.fetch_cluster_metrics(),
            'query_history': await self.fetch_query_history(hours_back)
        }

        logger.info("Data collection completed")
        return data