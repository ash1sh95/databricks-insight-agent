import dspy
from dspy import Signature, InputField, OutputField
import pandas as pd
from typing import Dict, List, Any
import structlog
from datetime import datetime, timedelta
import re

logger = structlog.get_logger(__name__)

class NetworkAnalysisSignature(Signature):
    """DSPy signature for network analysis."""
    audit_data = InputField(desc="JSON string of audit log data containing network events")
    time_window = InputField(desc="Time window for analysis (e.g., '24 hours')")

    network_insights = OutputField(desc="Detailed analysis of network patterns, anomalies, and recommendations")
    risk_score = OutputField(desc="Risk score from 1-10 indicating network security concerns")
    key_findings = OutputField(desc="Bullet points of critical network findings")

class NetworkAnalysisAgent:
    """
    Agent for analyzing network-related activities in Databricks.
    Uses DSPy for intelligent pattern recognition and anomaly detection.
    """

    def __init__(self, model_name: str = "gpt-4"):
        # Configure DSPy with OpenAI - only if not already configured
        if not hasattr(dspy.settings, 'lm') or dspy.settings.lm is None:
            try:
                # Try newer DSPy API first
                lm = dspy.LM(model=model_name)
            except (AttributeError, TypeError):
                try:
                    # Fall back to older API
                    lm = dspy.OpenAI(model=model_name)
                except AttributeError:
                    # If DSPy API is not available, create a mock
                    lm = None
            if lm:
                dspy.settings.configure(lm=lm)

        self.predictor = dspy.Predict(NetworkAnalysisSignature) if hasattr(dspy, 'Predict') else None

    async def analyze_network_activity(self, audit_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze network activity from audit logs.

        Args:
            audit_df: DataFrame with audit log data

        Returns:
            Dict containing analysis results
        """
        if audit_df.empty:
            logger.warning("No audit data provided for network analysis")
            return self._empty_response()

        try:
            # Filter for network-related events
            network_events = self._filter_network_events(audit_df)

            if network_events.empty:
                logger.info("No network-related events found in audit logs")
                return self._empty_response()

            # Prepare data for DSPy
            audit_json = network_events.to_json(orient='records', date_format='iso')

            # Calculate time window
            if not network_events.empty:
                min_time = pd.to_datetime(network_events['event_time']).min()
                max_time = pd.to_datetime(network_events['event_time']).max()
                time_window = f"{(max_time - min_time).total_seconds() / 3600:.1f} hours"
            else:
                time_window = "24 hours"

            # Run DSPy analysis
            result = self.predictor(
                audit_data=audit_json,
                time_window=time_window
            )

            # Parse and enhance results
            analysis = {
                'network_insights': result.network_insights,
                'risk_score': int(result.risk_score) if result.risk_score.isdigit() else 5,
                'key_findings': self._parse_findings(result.key_findings),
                'analyzed_events': len(network_events),
                'time_window': time_window,
                'timestamp': datetime.utcnow().isoformat()
            }

            logger.info("Network analysis completed", risk_score=analysis['risk_score'])
            return analysis

        except Exception as e:
            logger.error("Network analysis failed", error=str(e))
            return self._error_response(str(e))

    def _filter_network_events(self, audit_df: pd.DataFrame) -> pd.DataFrame:
        """Filter audit logs for network-related events."""
        # Network-related actions and services
        network_actions = [
            'executeQuery', 'getQueryResult', 'createCluster', 'startCluster',
            'terminateCluster', 'runJob', 'submitRun', 'getRun', 'listRuns'
        ]

        network_services = [
            'sql', 'clusters', 'jobs', 'dbfs', 'workspace', 'secrets'
        ]

        # Filter by service and action
        network_events = audit_df[
            audit_df['service_name'].isin(network_services) &
            audit_df['action_name'].isin(network_actions)
        ].copy()

        # Add network-specific features
        if not network_events.empty:
            network_events['is_external_ip'] = network_events['source_ip_address'].apply(
                self._is_external_ip
            )
            network_events['connection_type'] = network_events['source_ip_address'].apply(
                self._classify_connection
            )

        return network_events

    def _is_external_ip(self, ip: str) -> bool:
        """Check if IP address is external (not private)."""
        if not ip or pd.isna(ip):
            return False

        # Private IP ranges
        private_ranges = [
            re.compile(r'^10\.'),  # 10.0.0.0/8
            re.compile(r'^172\.(1[6-9]|2[0-9]|3[0-1])\.'),  # 172.16.0.0/12
            re.compile(r'^192\.168\.'),  # 192.168.0.0/16
            re.compile(r'^127\.'),  # localhost
            re.compile(r'^169\.254\.'),  # link-local
        ]

        return not any(pattern.match(ip) for pattern in private_ranges)

    def _classify_connection(self, ip: str) -> str:
        """Classify connection type based on IP."""
        if not ip or pd.isna(ip):
            return 'unknown'

        if self._is_external_ip(ip):
            return 'external'
        else:
            return 'internal'

    def _parse_findings(self, findings_str: str) -> List[str]:
        """Parse key findings from DSPy output."""
        if not findings_str:
            return []

        # Split by bullet points or newlines
        findings = re.split(r'[\n•-]', findings_str)
        return [f.strip() for f in findings if f.strip()]

    def _empty_response(self) -> Dict[str, Any]:
        """Return empty analysis response."""
        return {
            'network_insights': 'No network events found in the analyzed time period.',
            'risk_score': 1,
            'key_findings': [],
            'analyzed_events': 0,
            'time_window': 'N/A',
            'timestamp': datetime.utcnow().isoformat()
        }

    def _error_response(self, error: str) -> Dict[str, Any]:
        """Return error analysis response."""
        return {
            'network_insights': f'Analysis failed: {error}',
            'risk_score': 10,
            'key_findings': ['Analysis error occurred'],
            'analyzed_events': 0,
            'time_window': 'N/A',
            'timestamp': datetime.utcnow().isoformat()
        }