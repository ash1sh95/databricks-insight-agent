import dspy
from dspy import Signature, InputField, OutputField
import pandas as pd
from typing import Dict, List, Any
import structlog
from datetime import datetime
import re

logger = structlog.get_logger(__name__)

class CyberSecuritySignature(Signature):
    """DSPy signature for cybersecurity analysis."""
    audit_data = InputField(desc="JSON string of audit log data containing security events")
    time_window = InputField(desc="Time window for analysis")

    security_insights = OutputField(desc="Detailed analysis of security threats, vulnerabilities, and recommendations")
    threat_level = OutputField(desc="Threat level: LOW, MEDIUM, HIGH, CRITICAL")
    security_findings = OutputField(desc="Bullet points of critical security findings")
    recommended_actions = OutputField(desc="Immediate actions to mitigate identified risks")

class CyberSecurityAgent:
    """
    Agent for analyzing cybersecurity threats in Databricks environment.
    Uses DSPy for intelligent threat detection and risk assessment.
    """

    def __init__(self, model_name: str = "gpt-4"):
        try:
            # Try newer DSPy API first
            lm = dspy.LM(model=model_name, api_key=dspy.settings.api_key)
        except AttributeError:
            # Fall back to older API
            lm = dspy.OpenAI(model=model_name, api_key=dspy.settings.api_key)
        dspy.settings.configure(lm=lm)

        self.predictor = dspy.Predict(CyberSecuritySignature)

        # Known security patterns
        self.suspicious_actions = [
            'createSecret', 'getSecret', 'listSecrets',
            'createCluster', 'startCluster', 'deleteCluster',
            'executeQuery', 'getQueryResult',
            'createJob', 'runJob', 'deleteJob'
        ]

        self.high_risk_services = [
            'secrets', 'workspace', 'clusters', 'jobs'
        ]

    async def analyze_security_threats(self, audit_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze security threats from audit logs.

        Args:
            audit_df: DataFrame with audit log data

        Returns:
            Dict containing security analysis results
        """
        if audit_df.empty:
            logger.warning("No audit data provided for security analysis")
            return self._empty_response()

        try:
            # Filter for security-related events
            security_events = self._filter_security_events(audit_df)

            if security_events.empty:
                logger.info("No security-related events found in audit logs")
                return self._empty_response()

            # Enrich with security indicators
            enriched_events = self._enrich_security_data(security_events)

            # Prepare data for DSPy
            audit_json = enriched_events.to_json(orient='records', date_format='iso')

            # Calculate time window
            if not enriched_events.empty:
                min_time = pd.to_datetime(enriched_events['event_time']).min()
                max_time = pd.to_datetime(enriched_events['event_time']).max()
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
                'security_insights': result.security_insights,
                'threat_level': self._normalize_threat_level(result.threat_level),
                'security_findings': self._parse_findings(result.security_findings),
                'recommended_actions': self._parse_actions(result.recommended_actions),
                'analyzed_events': len(enriched_events),
                'time_window': time_window,
                'timestamp': datetime.utcnow().isoformat()
            }

            logger.info("Security analysis completed", threat_level=analysis['threat_level'])
            return analysis

        except Exception as e:
            logger.error("Security analysis failed", error=str(e))
            return self._error_response(str(e))

    def _filter_security_events(self, audit_df: pd.DataFrame) -> pd.DataFrame:
        """Filter audit logs for security-related events."""
        # Filter by suspicious actions and high-risk services
        security_events = audit_df[
            (audit_df['action_name'].isin(self.suspicious_actions)) |
            (audit_df['service_name'].isin(self.high_risk_services))
        ].copy()

        return security_events

    def _enrich_security_data(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich security events with additional indicators."""
        if events_df.empty:
            return events_df

        enriched = events_df.copy()

        # Add security indicators
        enriched['is_failed_action'] = enriched['response'].apply(
            lambda x: self._is_failed_response(x)
        )
        enriched['is_external_user'] = enriched['user_id'].apply(
            lambda x: self._is_external_user(x)
        )
        enriched['unusual_timing'] = enriched['event_time'].apply(
            lambda x: self._is_unusual_timing(x)
        )
        enriched['suspicious_ip'] = enriched['source_ip_address'].apply(
            lambda x: self._is_suspicious_ip(x)
        )

        # Calculate risk score per event
        enriched['event_risk_score'] = enriched.apply(
            lambda row: self._calculate_event_risk(row), axis=1
        )

        return enriched

    def _is_failed_response(self, response: str) -> bool:
        """Check if response indicates failure."""
        if not response or pd.isna(response):
            return False

        failure_indicators = ['error', 'failed', 'unauthorized', 'forbidden', 'denied']
        response_lower = str(response).lower()
        return any(indicator in response_lower for indicator in failure_indicators)

    def _is_external_user(self, user_id: str) -> bool:
        """Check if user appears to be external."""
        if not user_id or pd.isna(user_id):
            return False

        # Simple heuristic: check for email domains or patterns
        # In production, this would integrate with user directory
        return '@' in str(user_id) and not any(domain in str(user_id).lower()
                                              for domain in ['company.com', 'internal'])

    def _is_unusual_timing(self, event_time: str) -> bool:
        """Check if event occurred at unusual time."""
        if not event_time or pd.isna(event_time):
            return False

        try:
            dt = pd.to_datetime(event_time)
            hour = dt.hour

            # Business hours: 6 AM - 10 PM
            return hour < 6 or hour > 22
        except:
            return False

    def _is_suspicious_ip(self, ip: str) -> bool:
        """Check if IP address is suspicious."""
        if not ip or pd.isna(ip):
            return False

        # Known suspicious patterns (simplified)
        suspicious_patterns = [
            r'^0\.',  # 0.x.x.x
            r'^255\.',  # 255.x.x.x
            r'^\d{1,2}\.\d{1,2}\.\d{1,2}\.\d{1,2}$',  # Very short IPs
        ]

        return any(re.match(pattern, str(ip)) for pattern in suspicious_patterns)

    def _calculate_event_risk(self, row) -> int:
        """Calculate risk score for individual event."""
        risk = 0

        if row['is_failed_action']:
            risk += 3
        if row['is_external_user']:
            risk += 2
        if row['unusual_timing']:
            risk += 1
        if row['suspicious_ip']:
            risk += 2
        if row['action_name'] in ['createSecret', 'getSecret', 'deleteCluster']:
            risk += 2

        return min(risk, 10)  # Cap at 10

    def _normalize_threat_level(self, threat_str: str) -> str:
        """Normalize threat level string."""
        threat_str = str(threat_str).upper().strip()

        valid_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        if threat_str in valid_levels:
            return threat_str

        # Default mapping
        if 'CRITICAL' in threat_str or 'HIGH' in threat_str:
            return 'HIGH'
        elif 'MEDIUM' in threat_str:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _parse_findings(self, findings_str: str) -> List[str]:
        """Parse security findings."""
        if not findings_str:
            return []

        findings = re.split(r'[\n•-]', findings_str)
        return [f.strip() for f in findings if f.strip()]

    def _parse_actions(self, actions_str: str) -> List[str]:
        """Parse recommended actions."""
        if not actions_str:
            return []

        actions = re.split(r'[\n•-]', actions_str)
        return [a.strip() for a in actions if a.strip()]

    def _empty_response(self) -> Dict[str, Any]:
        """Return empty security analysis response."""
        return {
            'security_insights': 'No security events found in the analyzed time period.',
            'threat_level': 'LOW',
            'security_findings': [],
            'recommended_actions': [],
            'analyzed_events': 0,
            'time_window': 'N/A',
            'timestamp': datetime.utcnow().isoformat()
        }

    def _error_response(self, error: str) -> Dict[str, Any]:
        """Return error security analysis response."""
        return {
            'security_insights': f'Security analysis failed: {error}',
            'threat_level': 'CRITICAL',
            'security_findings': ['Analysis system error'],
            'recommended_actions': ['Contact security team immediately'],
            'analyzed_events': 0,
            'time_window': 'N/A',
            'timestamp': datetime.utcnow().isoformat()
        }