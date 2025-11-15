import asyncio
from typing import Dict, Any, List
import structlog
from datetime import datetime
import mlflow
import mlflow.pyfunc
from concurrent.futures import ThreadPoolExecutor

from .data_ingestion import DataIngestionAgent
from .network_analysis import NetworkAnalysisAgent
from .cyber_security import CyberSecurityAgent
from ..evaluation.scoring import ScoringSystem

logger = structlog.get_logger(__name__)

class OrchestratorAgent:
    """
    Central orchestrator that coordinates all analysis agents.
    Manages the workflow from data ingestion to final reporting.
    """

    def __init__(self, mlflow_experiment: str = "databricks-insight-agent"):
        self.data_agent = DataIngestionAgent()
        self.network_agent = NetworkAnalysisAgent()
        self.security_agent = CyberSecurityAgent()
        self.scoring_system = ScoringSystem()
        self.mlflow_experiment = mlflow_experiment

        # Initialize MLflow
        mlflow.set_experiment(self.mlflow_experiment)

    async def run_full_analysis(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        Run complete analysis workflow.

        Args:
            hours_back: Number of hours of historical data to analyze

        Returns:
            Dict containing all analysis results
        """
        start_time = datetime.utcnow()
        logger.info("Starting full analysis workflow", hours_back=hours_back)

        try:
            with mlflow.start_run(run_name=f"analysis_{start_time.strftime('%Y%m%d_%H%M%S')}"):
                # Log parameters
                mlflow.log_param("hours_back", hours_back)
                mlflow.log_param("analysis_start", start_time.isoformat())

                # Step 1: Data Ingestion
                logger.info("Step 1: Data ingestion")
                data_results = await self._run_data_ingestion(hours_back)
                mlflow.log_metric("data_events_ingested", sum(len(df) for df in data_results.values()))

                # Step 2: Parallel Analysis
                logger.info("Step 2: Running parallel analysis")
                analysis_results = await self._run_parallel_analysis(data_results)

                # Step 3: Aggregate Results
                logger.info("Step 3: Aggregating results")
                final_results = self._aggregate_results(data_results, analysis_results)

                # Step 4: Evaluation and Scoring
                logger.info("Step 4: Evaluation and scoring")
                evaluation_results = await self.scoring_system.evaluate_analysis_results(final_results)
                evaluation_metrics = self.scoring_system.get_performance_metrics(evaluation_results)

                # Log evaluation metrics
                for metric, value in evaluation_metrics.items():
                    mlflow.log_metric(f"eval_{metric}", value)

                # Add evaluation to results
                final_results['evaluation'] = evaluation_results

                # Log completion
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                mlflow.log_metric("analysis_duration_seconds", duration)
                mlflow.log_param("analysis_end", end_time.isoformat())

                final_results['metadata'] = {
                    'analysis_duration': duration,
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'hours_back': hours_back,
                    'quality_metrics': quality_metrics
                }

                logger.info("Analysis workflow completed successfully",
                          duration=duration,
                          threat_level=final_results.get('overall_threat_level'))

                return final_results

        except Exception as e:
            logger.error("Analysis workflow failed", error=str(e))
            mlflow.log_param("error", str(e))
            raise

    async def _run_data_ingestion(self, hours_back: int) -> Dict[str, Any]:
        """Run data ingestion step."""
        # Connect to Databricks
        connected = await self.data_agent.connect()
        if not connected:
            raise ConnectionError("Failed to connect to Databricks")

        # Collect all data
        data = await self.data_agent.collect_all_data(hours_back)
        return data

    async def _run_parallel_analysis(self, data_results: Dict[str, Any]) -> Dict[str, Any]:
        """Run network and security analysis in parallel."""
        audit_df = data_results.get('audit_logs')

        if audit_df is None or audit_df.empty:
            logger.warning("No audit data available for analysis")
            return {
                'network_analysis': {},
                'security_analysis': {}
            }

        # Run both analyses concurrently
        network_task = self.network_agent.analyze_network_activity(audit_df)
        security_task = self.security_agent.analyze_security_threats(audit_df)

        network_result, security_result = await asyncio.gather(
            network_task, security_task, return_exceptions=True
        )

        # Handle exceptions
        results = {}
        if isinstance(network_result, Exception):
            logger.error("Network analysis failed", error=str(network_result))
            results['network_analysis'] = {'error': str(network_result)}
        else:
            results['network_analysis'] = network_result

        if isinstance(security_result, Exception):
            logger.error("Security analysis failed", error=str(security_result))
            results['security_analysis'] = {'error': str(security_result)}
        else:
            results['security_analysis'] = security_result

        return results

    def _aggregate_results(self, data_results: Dict[str, Any],
                          analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate all results into final output."""
        final = {
            'data_summary': {
                'audit_events': len(data_results.get('audit_logs', [])),
                'clusters': len(data_results.get('cluster_metrics', [])),
                'queries': len(data_results.get('query_history', []))
            },
            'network_analysis': analysis_results.get('network_analysis', {}),
            'security_analysis': analysis_results.get('security_analysis', {}),
            'overall_threat_level': self._calculate_overall_threat(
                analysis_results.get('network_analysis', {}),
                analysis_results.get('security_analysis', {})
            ),
            'recommendations': self._generate_recommendations(analysis_results),
            'timestamp': datetime.utcnow().isoformat()
        }

        return final

    def _calculate_overall_threat(self, network: Dict[str, Any],
                                security: Dict[str, Any]) -> str:
        """Calculate overall threat level from individual analyses."""
        threat_levels = {
            'LOW': 1,
            'MEDIUM': 2,
            'HIGH': 3,
            'CRITICAL': 4
        }

        # Get threat levels
        network_risk = network.get('risk_score', 1)
        security_level = security.get('threat_level', 'LOW')

        # Convert security level to numeric
        security_score = threat_levels.get(security_level, 1)

        # Combine scores (weighted average)
        overall_score = (network_risk * 0.3) + (security_score * 0.7)

        # Convert back to level
        if overall_score >= 3.5:
            return 'CRITICAL'
        elif overall_score >= 2.5:
            return 'HIGH'
        elif overall_score >= 1.5:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """Generate high-level recommendations based on analysis."""
        recommendations = []

        network = analysis_results.get('network_analysis', {})
        security = analysis_results.get('security_analysis', {})

        # Network recommendations
        if network.get('risk_score', 1) > 5:
            recommendations.append("Review network access patterns and consider implementing IP whitelisting")

        # Security recommendations
        threat_level = security.get('threat_level', 'LOW')
        if threat_level in ['HIGH', 'CRITICAL']:
            recommendations.append("Immediate security review required - check for unauthorized access attempts")
        elif threat_level == 'MEDIUM':
            recommendations.append("Monitor user activities closely and review access permissions")

        # General recommendations
        recommendations.extend([
            "Regular security audits recommended",
            "Implement automated alerting for high-risk activities",
            "Review and update access policies periodically"
        ])

        return recommendations

    def _assess_analysis_quality(self, results: Dict[str, Any]) -> Dict[str, float]:
        """Assess the quality of the analysis results."""
        metrics = {
            'data_completeness': 0.0,
            'analysis_coverage': 0.0,
            'insight_confidence': 0.0
        }

        # Data completeness
        data_summary = results.get('data_summary', {})
        total_data_points = sum(data_summary.values())
        if total_data_points > 0:
            metrics['data_completeness'] = min(total_data_points / 1000, 1.0)  # Normalize

        # Analysis coverage
        network_done = bool(results.get('network_analysis'))
        security_done = bool(results.get('security_analysis'))
        metrics['analysis_coverage'] = (network_done + security_done) / 2.0

        # Insight confidence (simplified)
        network_risk = results.get('network_analysis', {}).get('risk_score', 5)
        security_level = results.get('security_analysis', {}).get('threat_level', 'MEDIUM')
        confidence_score = 1.0 - (abs(network_risk - 5) / 10)  # Higher confidence when risk is moderate
        metrics['insight_confidence'] = confidence_score

        return metrics

    async def run_scheduled_analysis(self, interval_hours: int = 24):
        """Run analysis on a schedule."""
        logger.info("Starting scheduled analysis", interval_hours=interval_hours)

        while True:
            try:
                await self.run_full_analysis(interval_hours)
                logger.info(f"Scheduled analysis completed, waiting {interval_hours} hours")
            except Exception as e:
                logger.error("Scheduled analysis failed", error=str(e))

            await asyncio.sleep(interval_hours * 3600)