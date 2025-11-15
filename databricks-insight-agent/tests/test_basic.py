"""
Basic tests for Databricks Insight Agent.
Note: Full end-to-end testing requires Databricks credentials and OpenAI API access.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

# Import agents
from src.agents.data_ingestion import DataIngestionAgent
from src.agents.network_analysis import NetworkAnalysisAgent
from src.agents.cyber_security import CyberSecurityAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.reporting import ReportingAgent
from src.evaluation.scoring import ScoringSystem

class TestDataIngestionAgent:
    """Test data ingestion functionality."""

    @pytest.fixture
    def agent(self):
        return DataIngestionAgent()

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert agent.host is None
        assert agent.token is None
        assert agent.workspace_client is None

    def test_mock_data_generation(self, agent):
        """Test mock data generation works."""
        # Test that mock data can be generated
        mock_audit = agent._generate_mock_audit_logs(1)
        mock_clusters = agent._generate_mock_cluster_metrics()
        mock_queries = agent._generate_mock_query_history(1)

        assert len(mock_audit) > 0
        assert len(mock_clusters) > 0
        assert len(mock_queries) > 0

        # Check required columns exist
        required_audit_cols = ['event_time', 'user_id', 'service_name', 'action_name']
        required_cluster_cols = ['cluster_id', 'cluster_name', 'state']
        required_query_cols = ['query_id', 'query_text', 'user_id']

        for col in required_audit_cols:
            assert col in mock_audit.columns
        for col in required_cluster_cols:
            assert col in mock_clusters.columns
        for col in required_query_cols:
            assert col in mock_queries.columns

class TestNetworkAnalysisAgent:
    """Test network analysis functionality."""

    @pytest.fixture
    def agent(self):
        with patch('dspy.LM') as mock_lm:
            return NetworkAnalysisAgent()

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert hasattr(agent, 'predictor')
        assert hasattr(agent, 'suspicious_actions')

    @pytest.mark.asyncio
    async def test_empty_analysis(self, agent):
        """Test analysis with empty data."""
        result = await agent.analyze_network_activity(pd.DataFrame())

        assert result['risk_score'] == 1
        assert 'analyzed_events' in result
        assert result['analyzed_events'] == 0

class TestCyberSecurityAgent:
    """Test cybersecurity analysis functionality."""

    @pytest.fixture
    def agent(self):
        with patch('dspy.LM') as mock_lm:
            return CyberSecurityAgent()

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert hasattr(agent, 'predictor')
        assert hasattr(agent, 'suspicious_actions')
        assert 'secrets' in agent.high_risk_services

    def test_is_external_ip(self, agent):
        """Test IP classification."""
        assert agent._is_external_ip('203.0.113.1') == True
        assert agent._is_external_ip('192.168.1.1') == False
        assert agent._is_external_ip('10.0.0.1') == False

class TestReportingAgent:
    """Test reporting functionality."""

    @pytest.fixture
    def agent(self):
        return ReportingAgent()

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert hasattr(agent, 'reports_dir')
        assert hasattr(agent, 'alerts_enabled')

    def test_format_list(self, agent):
        """Test list formatting."""
        items = ['Item 1', 'Item 2', 'Item 3']
        formatted = agent._format_list(items)

        assert '- Item 1' in formatted
        assert '- Item 2' in formatted

class TestScoringSystem:
    """Test evaluation and scoring functionality."""

    @pytest.fixture
    def scoring_system(self):
        with patch('dspy.LM') as mock_lm:
            return ScoringSystem()

    def test_system_initialization(self, scoring_system):
        """Test scoring system initializes correctly."""
        assert hasattr(scoring_system, 'insight_evaluator')
        assert hasattr(scoring_system, 'performance_evaluator')

    @pytest.mark.asyncio
    async def test_empty_evaluation(self, scoring_system):
        """Test evaluation with empty results."""
        result = await scoring_system.evaluate_analysis_results({})

        assert 'overall_score' in result
        assert 'agent_scores' in result
        assert 'recommendations' in result

class TestOrchestratorAgent:
    """Test orchestrator functionality."""

    @pytest.fixture
    def agent(self):
        with patch('mlflow.set_experiment'):
            return OrchestratorAgent()

    def test_agent_initialization(self, agent):
        """Test orchestrator initializes correctly."""
        assert hasattr(agent, 'data_agent')
        assert hasattr(agent, 'network_agent')
        assert hasattr(agent, 'security_agent')
        assert hasattr(agent, 'scoring_system')

# Integration test placeholder
@pytest.mark.integration
class TestIntegration:
    """Integration tests (require actual credentials)."""

    @pytest.mark.skip(reason="Requires Databricks credentials")
    def test_full_workflow(self):
        """Test complete analysis workflow."""
        # This would test the full end-to-end workflow
        # Requires actual Databricks workspace and OpenAI API
        pass

if __name__ == '__main__':
    pytest.main([__file__, '-v'])