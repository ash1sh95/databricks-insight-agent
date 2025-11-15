import dspy
from dspy import Signature, InputField, OutputField
import re
import json
from typing import Dict, Any, List
import structlog
from datetime import datetime

logger = structlog.get_logger(__name__)

class InsightEvaluationSignature(Signature):
    """DSPy signature for evaluating insight quality."""
    insight_text = InputField(desc="The insight or analysis text to evaluate")
    context = InputField(desc="Context about the data and analysis type")

    quality_score = OutputField(desc="Quality score from 1-10 based on clarity, accuracy, and usefulness")
    strengths = OutputField(desc="Key strengths of the insight")
    weaknesses = OutputField(desc="Areas for improvement")
    confidence_level = OutputField(desc="Confidence in the evaluation: HIGH, MEDIUM, LOW")

class AgentPerformanceSignature(Signature):
    """DSPy signature for evaluating agent performance."""
    agent_output = InputField(desc="Complete output from an analysis agent")
    expected_criteria = InputField(desc="Criteria for successful analysis")

    performance_score = OutputField(desc="Overall performance score from 1-10")
    accuracy_rating = OutputField(desc="Accuracy of the analysis")
    completeness_rating = OutputField(desc="Completeness of the analysis")
    recommendations = OutputField(desc="Suggestions for agent improvement")

class ScoringSystem:
    """
    System for evaluating and scoring agent performance and insight quality.
    Uses DSPy for intelligent evaluation of analysis outputs.
    """

    def __init__(self, model_name: str = "gpt-4"):
        try:
            # Try newer DSPy API first
            lm = dspy.LM(model=model_name, api_key=dspy.settings.api_key)
        except AttributeError:
            # Fall back to older API
            lm = dspy.OpenAI(model=model_name, api_key=dspy.settings.api_key)
        dspy.settings.configure(lm=lm)

        self.insight_evaluator = dspy.Predict(InsightEvaluationSignature)
        self.performance_evaluator = dspy.Predict(AgentPerformanceSignature)

    async def evaluate_analysis_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate the complete analysis results from all agents.

        Args:
            results: Complete analysis results from orchestrator

        Returns:
            Dict containing evaluation scores and feedback
        """
        logger.info("Starting comprehensive evaluation")

        evaluation = {
            'overall_score': 0.0,
            'agent_scores': {},
            'insight_quality': {},
            'recommendations': [],
            'timestamp': datetime.utcnow().isoformat()
        }

        try:
            # Evaluate individual agents
            network_score = await self._evaluate_agent_performance(
                'network_analysis',
                results.get('network_analysis', {}),
                "Network pattern detection, anomaly identification, risk assessment"
            )
            evaluation['agent_scores']['network'] = network_score

            security_score = await self._evaluate_agent_performance(
                'security_analysis',
                results.get('security_analysis', {}),
                "Threat detection, security event analysis, risk mitigation recommendations"
            )
            evaluation['agent_scores']['security'] = security_score

            # Evaluate insight quality
            network_insights = results.get('network_analysis', {}).get('network_insights', '')
            if network_insights:
                network_quality = await self._evaluate_insight_quality(
                    network_insights,
                    "Network traffic analysis and connectivity insights"
                )
                evaluation['insight_quality']['network'] = network_quality

            security_insights = results.get('security_analysis', {}).get('security_insights', '')
            if security_insights:
                security_quality = await self._evaluate_insight_quality(
                    security_insights,
                    "Security threat analysis and compliance insights"
                )
                evaluation['insight_quality']['security'] = security_quality

            # Calculate overall score
            agent_avg = (network_score.get('performance_score', 5) +
                        security_score.get('performance_score', 5)) / 2
            insight_avg = sum(
                q.get('quality_score', 5)
                for q in evaluation['insight_quality'].values()
            ) / max(len(evaluation['insight_quality']), 1)

            evaluation['overall_score'] = (agent_avg + insight_avg) / 2

            # Collect recommendations
            evaluation['recommendations'] = self._aggregate_recommendations(evaluation)

            logger.info("Evaluation completed", overall_score=evaluation['overall_score'])
            return evaluation

        except Exception as e:
            logger.error("Evaluation failed", error=str(e))
            return self._error_evaluation(str(e))

    async def _evaluate_agent_performance(self, agent_name: str,
                                        agent_output: Dict[str, Any],
                                        criteria: str) -> Dict[str, Any]:
        """Evaluate individual agent performance."""
        if not agent_output or 'error' in agent_output:
            return {
                'performance_score': 1,
                'accuracy_rating': 'Unable to evaluate - agent failed',
                'completeness_rating': 'Incomplete - agent error',
                'recommendations': ['Fix agent errors before evaluation']
            }

        try:
            # Prepare input for DSPy
            output_text = json.dumps(agent_output, indent=2)

            result = self.performance_evaluator(
                agent_output=output_text,
                expected_criteria=criteria
            )

            evaluation = {
                'performance_score': int(result.performance_score) if result.performance_score.isdigit() else 5,
                'accuracy_rating': result.accuracy_rating,
                'completeness_rating': result.completeness_rating,
                'recommendations': self._parse_recommendations(result.recommendations)
            }

            return evaluation

        except Exception as e:
            logger.error(f"Agent evaluation failed for {agent_name}", error=str(e))
            return {
                'performance_score': 3,
                'accuracy_rating': 'Evaluation error',
                'completeness_rating': 'Unable to assess',
                'recommendations': ['Review evaluation system']
            }

    async def _evaluate_insight_quality(self, insight_text: str, context: str) -> Dict[str, Any]:
        """Evaluate quality of generated insights."""
        if not insight_text or len(insight_text.strip()) < 10:
            return {
                'quality_score': 1,
                'strengths': 'Insufficient content',
                'weaknesses': 'No meaningful insights generated',
                'confidence_level': 'HIGH'
            }

        try:
            result = self.insight_evaluator(
                insight_text=insight_text,
                context=context
            )

            evaluation = {
                'quality_score': int(result.quality_score) if result.quality_score.isdigit() else 5,
                'strengths': result.strengths,
                'weaknesses': result.weaknesses,
                'confidence_level': result.confidence_level
            }

            return evaluation

        except Exception as e:
            logger.error("Insight evaluation failed", error=str(e))
            return {
                'quality_score': 3,
                'strengths': 'Unable to evaluate',
                'weaknesses': 'Evaluation system error',
                'confidence_level': 'LOW'
            }

    def _parse_recommendations(self, recommendations_str: str) -> List[str]:
        """Parse recommendations from DSPy output."""
        if not recommendations_str:
            return []

        recs = re.split(r'[\n•-]', recommendations_str)
        return [r.strip() for r in recs if r.strip()]

    def _aggregate_recommendations(self, evaluation: Dict[str, Any]) -> List[str]:
        """Aggregate all recommendations into a unique list."""
        all_recs = []

        # Agent recommendations
        for agent_eval in evaluation['agent_scores'].values():
            all_recs.extend(agent_eval.get('recommendations', []))

        # Insight recommendations (from weaknesses)
        for insight_eval in evaluation['insight_quality'].values():
            weaknesses = insight_eval.get('weaknesses', '')
            if weaknesses:
                all_recs.append(f"Address: {weaknesses}")

        # Remove duplicates and empty strings
        unique_recs = list(set(r for r in all_recs if r))

        return unique_recs

    def _error_evaluation(self, error: str) -> Dict[str, Any]:
        """Return error evaluation result."""
        return {
            'overall_score': 0.0,
            'agent_scores': {},
            'insight_quality': {},
            'recommendations': [f'Evaluation system error: {error}'],
            'timestamp': datetime.utcnow().isoformat()
        }

    def get_performance_metrics(self, evaluation: Dict[str, Any]) -> Dict[str, float]:
        """Extract numerical metrics for monitoring."""
        metrics = {
            'overall_score': evaluation.get('overall_score', 0.0),
            'network_agent_score': evaluation.get('agent_scores', {}).get('network', {}).get('performance_score', 0),
            'security_agent_score': evaluation.get('agent_scores', {}).get('security', {}).get('performance_score', 0),
            'network_insight_quality': evaluation.get('insight_quality', {}).get('network', {}).get('quality_score', 0),
            'security_insight_quality': evaluation.get('insight_quality', {}).get('security', {}).get('quality_score', 0),
        }

        return metrics