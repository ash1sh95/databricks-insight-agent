#!/usr/bin/env python3
"""
Main entry point for the Databricks Insight Agent.
Production-grade multi-agent system for enterprise monitoring.
"""

import asyncio
import signal
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logging import setup_logging, get_logger
from utils.config import config
from agents.orchestrator import OrchestratorAgent
from agents.reporting import ReportingAgent

logger = get_logger(__name__)

class DatabricksInsightAgent:
    """Main application class."""

    def __init__(self):
        self.orchestrator = None
        self.reporting = ReportingAgent()
        self.running = False

    async def initialize(self):
        """Initialize the agent system."""
        logger.info("Initializing Databricks Insight Agent")

        try:
            # Validate configuration
            if not config.get('DATABRICKS_HOST') or not config.get('DATABRICKS_TOKEN'):
                raise ValueError("Databricks credentials not configured")

            if not config.get('OPENAI_API_KEY'):
                raise ValueError("OpenAI API key not configured")

            # Initialize orchestrator
            self.orchestrator = OrchestratorAgent(
                mlflow_experiment=config.get('MLFLOW_EXPERIMENT_NAME')
            )

            logger.info("Initialization completed successfully")
            return True

        except Exception as e:
            logger.error("Initialization failed", error=str(e))
            return False

    async def run_analysis(self, hours_back: int = 24):
        """Run a single analysis cycle."""
        if not self.orchestrator:
            logger.error("Agent not initialized")
            return None

        try:
            logger.info("Starting analysis cycle", hours_back=hours_back)

            # Run full analysis
            results = await self.orchestrator.run_full_analysis(hours_back)

            # Generate reports
            report_result = await self.reporting.generate_report(results)

            logger.info("Analysis cycle completed",
                       threat_level=results.get('overall_threat_level'),
                       report_path=report_result['metadata']['file_path'])

            return results

        except Exception as e:
            logger.error("Analysis cycle failed", error=str(e))
            raise

    async def run_scheduled(self, interval_hours: int = 24):
        """Run scheduled analysis cycles."""
        logger.info("Starting scheduled analysis", interval_hours=interval_hours)

        self.running = True

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while self.running:
            try:
                await self.run_analysis(interval_hours)
                logger.info(f"Waiting {interval_hours} hours until next analysis")

                # Wait for next cycle or shutdown
                for _ in range(interval_hours * 3600):  # Check every second
                    if not self.running:
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error("Scheduled analysis cycle failed", error=str(e))
                # Continue running despite errors
                await asyncio.sleep(60)  # Wait before retry

        logger.info("Scheduled analysis stopped")

    async def health_check(self) -> dict:
        """Perform health check of the system."""
        health = {
            'status': 'healthy',
            'checks': {},
            'timestamp': asyncio.get_event_loop().time()
        }

        try:
            # Check Databricks connection
            if self.orchestrator and self.orchestrator.data_agent:
                connected = await self.orchestrator.data_agent.connect()
                health['checks']['databricks_connection'] = 'healthy' if connected else 'unhealthy'
            else:
                health['checks']['databricks_connection'] = 'not_initialized'

            # Check OpenAI
            import openai
            health['checks']['openai_connection'] = 'healthy'

            # Check file system
            reports_dir = Path(config.get('REPORTS_DIR', 'reports'))
            reports_dir.mkdir(exist_ok=True)
            health['checks']['filesystem'] = 'healthy'

        except Exception as e:
            health['status'] = 'unhealthy'
            health['error'] = str(e)

        return health

async def main():
    """Main application entry point."""
    # Setup logging
    setup_logging()

    logger.info("Starting Databricks Insight Agent")

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Databricks Insight Agent')
    parser.add_argument('--mode', choices=['once', 'scheduled'],
                       default='once', help='Run mode')
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours of data to analyze')
    parser.add_argument('--interval', type=int, default=24,
                       help='Interval for scheduled runs (hours)')

    args = parser.parse_args()

    # Initialize agent
    agent = DatabricksInsightAgent()
    if not await agent.initialize():
        sys.exit(1)

    try:
        if args.mode == 'scheduled':
            await agent.run_scheduled(args.interval)
        else:
            await agent.run_analysis(args.hours)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error("Application failed", error=str(e))
        sys.exit(1)

    logger.info("Application completed")

if __name__ == '__main__':
    asyncio.run(main())