#!/usr/bin/env python3
"""
Pipeline monitoring and alerting for Ethiopian Medical Business Data Platform
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests
from dataclasses import dataclass

# Add src directory to path
sys.path.append(str(Path(__file__).parent / 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PipelineAlert:
    """Pipeline alert configuration"""
    alert_type: str
    threshold: float
    comparison: str  # 'gt', 'lt', 'eq'
    message_template: str
    severity: str  # 'info', 'warning', 'error', 'critical'

class PipelineMonitor:
    """Monitor pipeline execution and send alerts"""
    
    def __init__(self):
        """Initialize pipeline monitor"""
        self.dagster_url = "http://localhost:3000"
        self.api_url = f"{self.dagster_url}/graphql"
        self.alerts_file = Path("pipeline_alerts.json")
        self.monitoring_log = Path("logs/pipeline_monitoring.log")
        
        # Create alerts file if it doesn't exist
        self.setup_default_alerts()
        
        # Alert configurations
        self.alerts = self.load_alerts()
    
    def setup_default_alerts(self):
        """Setup default alert configurations"""
        if not self.alerts_file.exists():
            default_alerts = [
                {
                    "alert_type": "pipeline_failure",
                    "threshold": 1.0,
                    "comparison": "gt",
                    "message_template": "Pipeline failed with status: {status}",
                    "severity": "critical"
                },
                {
                    "alert_type": "long_execution_time",
                    "threshold": 3600.0,  # 1 hour
                    "comparison": "gt",
                    "message_template": "Pipeline took {execution_time:.1f} seconds (threshold: {threshold}s)",
                    "severity": "warning"
                },
                {
                    "alert_type": "low_data_volume",
                    "threshold": 100.0,
                    "comparison": "lt",
                    "message_template": "Low data volume: {records_processed} records (threshold: {threshold})",
                    "severity": "warning"
                },
                {
                    "alert_type": "high_failure_rate",
                    "threshold": 0.1,  # 10%
                    "comparison": "gt",
                    "message_template": "High failure rate: {failure_rate:.1%} (threshold: {threshold:.1%})",
                    "severity": "error"
                },
                {
                    "alert_type": "stale_data",
                    "threshold": 48.0,  # 48 hours
                    "comparison": "gt",
                    "message_template": "Data is stale: {hours_since_last_run:.1f} hours old (threshold: {threshold}h)",
                    "severity": "warning"
                }
            ]
            
            with open(self.alerts_file, 'w') as f:
                json.dump(default_alerts, f, indent=2)
            
            logger.info(f"Created default alerts configuration: {self.alerts_file}")
    
    def load_alerts(self) -> List[PipelineAlert]:
        """Load alert configurations"""
        try:
            with open(self.alerts_file, 'r') as f:
                alerts_data = json.load(f)
            
            alerts = []
            for alert_data in alerts_data:
                alerts.append(PipelineAlert(**alert_data))
            
            return alerts
        except Exception as e:
            logger.error(f"Failed to load alerts: {e}")
            return []
    
    def query_dagster_graphql(self, query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """Query Dagster GraphQL API"""
        try:
            response = requests.post(
                self.api_url,
                json={"query": query, "variables": variables or {}},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GraphQL query failed: {response.status_code} - {response.text}")
                return {}
        
        except Exception as e:
            logger.error(f"Failed to query Dagster API: {e}")
            return {}
    
    def get_recent_runs(self, job_name: str = "ethiopian_medical_pipeline", limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent pipeline runs"""
        query = """
        query GetRecentRuns($jobName: String!, $limit: Int!) {
          runsOrError(limit: $limit, filter: {jobName: $jobName}) {
            __typename
            ... on Runs {
              results {
                id
                status
                startTime
                endTime
                runConfigYaml
                logs {
                  timestamp
                  message
                  level
                }
              }
            }
            ... on PythonError {
              message
            }
          }
        }
        """
        
        variables = {"jobName": job_name, "limit": limit}
        result = self.query_dagster_graphql(query, variables)
        
        if result and 'data' in result and 'runsOrError' in result['data']:
            runs_data = result['data']['runsOrError']
            if runs_data.get('__typename') == 'Runs':
                return runs_data.get('results', [])
        
        return []
    
    def get_pipeline_stats(self, job_name: str = "ethiopian_medical_pipeline") -> Dict[str, Any]:
        """Get pipeline execution statistics"""
        runs = self.get_recent_runs(job_name, limit=50)
        
        if not runs:
            return {}
        
        # Calculate statistics
        total_runs = len(runs)
        successful_runs = len([r for r in runs if r.get('status') == 'SUCCESS'])
        failed_runs = len([r for r in runs if r.get('status') == 'FAILURE'])
        
        # Calculate execution times
        execution_times = []
        for run in runs:
            if run.get('startTime') and run.get('endTime'):
                start_time = datetime.fromisoformat(run['startTime'].replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(run['endTime'].replace('Z', '+00:00'))
                execution_time = (end_time - start_time).total_seconds()
                execution_times.append(execution_time)
        
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
        max_execution_time = max(execution_times) if execution_times else 0
        
        # Get last run time
        last_run = runs[0] if runs else None
        hours_since_last_run = 0
        if last_run and last_run.get('startTime'):
            last_run_time = datetime.fromisoformat(last_run['startTime'].replace('Z', '+00:00'))
            hours_since_last_run = (datetime.now(last_run_time.tzinfo) - last_run_time).total_seconds() / 3600
        
        return {
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'success_rate': successful_runs / total_runs if total_runs > 0 else 0,
            'failure_rate': failed_runs / total_runs if total_runs > 0 else 0,
            'avg_execution_time': avg_execution_time,
            'max_execution_time': max_execution_time,
            'last_run_time': last_run.get('startTime') if last_run else None,
            'hours_since_last_run': hours_since_last_run,
            'last_status': last_run.get('status') if last_run else None
        }
    
    def get_data_volume_stats(self) -> Dict[str, Any]:
        """Get data volume statistics from database"""
        try:
            from database import execute_query_to_dataframe
            
            # Get message count
            message_query = """
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT channel_name) as unique_channels,
                MIN(message_date) as earliest_message,
                MAX(message_date) as latest_message
            FROM analytics.fct_messages
            """
            
            message_stats = execute_query_to_dataframe(message_query)
            
            # Get image detection stats
            image_query = """
            SELECT 
                COUNT(*) as total_images,
                AVG(max_confidence) as avg_confidence,
                COUNT(DISTINCT calculated_category) as unique_categories
            FROM analytics.fct_image_detections
            """
            
            image_stats = execute_query_to_dataframe(image_query)
            
            stats = {}
            
            if message_stats:
                stats.update({
                    'total_messages': message_stats[0]['total_messages'],
                    'unique_channels': message_stats[0]['unique_channels'],
                    'earliest_message': str(message_stats[0]['earliest_message']),
                    'latest_message': str(message_stats[0]['latest_message'])
                })
            
            if image_stats:
                stats.update({
                    'total_images': image_stats[0]['total_images'],
                    'avg_confidence': float(image_stats[0]['avg_confidence']) if image_stats[0]['avg_confidence'] else 0,
                    'unique_categories': image_stats[0]['unique_categories']
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get data volume stats: {e}")
            return {}
    
    def check_alerts(self, pipeline_stats: Dict[str, Any], data_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check all alert conditions"""
        triggered_alerts = []
        
        for alert in self.alerts:
            triggered = False
            alert_data = {}
            
            if alert.alert_type == "pipeline_failure":
                if pipeline_stats.get('last_status') == 'FAILURE':
                    triggered = True
                    alert_data['status'] = pipeline_stats['last_status']
            
            elif alert.alert_type == "long_execution_time":
                execution_time = pipeline_stats.get('max_execution_time', 0)
                if alert.comparison == 'gt' and execution_time > alert.threshold:
                    triggered = True
                    alert_data['execution_time'] = execution_time
                    alert_data['threshold'] = alert.threshold
            
            elif alert.alert_type == "low_data_volume":
                total_messages = data_stats.get('total_messages', 0)
                if alert.comparison == 'lt' and total_messages < alert.threshold:
                    triggered = True
                    alert_data['records_processed'] = total_messages
                    alert_data['threshold'] = alert.threshold
            
            elif alert.alert_type == "high_failure_rate":
                failure_rate = pipeline_stats.get('failure_rate', 0)
                if alert.comparison == 'gt' and failure_rate > alert.threshold:
                    triggered = True
                    alert_data['failure_rate'] = failure_rate
                    alert_data['threshold'] = alert.threshold
            
            elif alert.alert_type == "stale_data":
                hours_since_last_run = pipeline_stats.get('hours_since_last_run', 0)
                if alert.comparison == 'gt' and hours_since_last_run > alert.threshold:
                    triggered = True
                    alert_data['hours_since_last_run'] = hours_since_last_run
                    alert_data['threshold'] = alert.threshold
            
            if triggered:
                message = alert.message_template.format(**alert_data)
                triggered_alerts.append({
                    'alert_type': alert.alert_type,
                    'severity': alert.severity,
                    'message': message,
                    'timestamp': datetime.now().isoformat(),
                    'data': alert_data
                })
        
        return triggered_alerts
    
    def send_alert(self, alert: Dict[str, Any]):
        """Send alert (placeholder for actual alerting implementation)"""
        # This is a placeholder - in production, you might send:
        # - Email notifications
        # - Slack messages
        # - PagerDuty alerts
        # - Custom webhook calls
        
        severity_emoji = {
            'info': 'ℹ️',
            'warning': '⚠️',
            'error': '❌',
            'critical': '🚨'
        }
        
        emoji = severity_emoji.get(alert['severity'], '📋')
        
        log_message = f"{emoji} [{alert['severity'].upper()}] {alert['alert_type']}: {alert['message']}"
        
        if alert['severity'] in ['error', 'critical']:
            logger.error(log_message)
        elif alert['severity'] == 'warning':
            logger.warning(log_message)
        else:
            logger.info(log_message)
        
        # Store alert in alerts file
        alerts_log = Path("logs/alerts.log")
        with open(alerts_log, 'a') as f:
            f.write(f"{datetime.now().isoformat()} - {log_message}\n")
    
    def run_monitoring(self):
        """Run complete monitoring cycle"""
        logger.info("Starting pipeline monitoring...")
        
        try:
            # Get pipeline statistics
            pipeline_stats = self.get_pipeline_stats()
            logger.info(f"Pipeline stats: {pipeline_stats}")
            
            # Get data statistics
            data_stats = self.get_data_volume_stats()
            logger.info(f"Data stats: {data_stats}")
            
            # Check alerts
            triggered_alerts = self.check_alerts(pipeline_stats, data_stats)
            
            if triggered_alerts:
                logger.warning(f"Triggered {len(triggered_alerts)} alerts")
                
                for alert in triggered_alerts:
                    self.send_alert(alert)
            else:
                logger.info("No alerts triggered")
            
            # Generate monitoring report
            self.generate_monitoring_report(pipeline_stats, data_stats, triggered_alerts)
            
            logger.info("Monitoring cycle completed")
            
        except Exception as e:
            logger.error(f"Monitoring failed: {e}")
            raise
    
    def generate_monitoring_report(self, pipeline_stats: Dict[str, Any], data_stats: Dict[str, Any], alerts: List[Dict[str, Any]]):
        """Generate monitoring report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_stats': pipeline_stats,
            'data_stats': data_stats,
            'alerts': alerts,
            'alert_count': len(alerts),
            'health_status': 'healthy' if len([a for a in alerts if a['severity'] in ['error', 'critical']]) == 0 else 'unhealthy'
        }
        
        # Save report
        report_file = Path("results/monitoring_report.json")
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Monitoring report saved to {report_file}")
        
        return report

def main():
    """Main monitoring function"""
    monitor = PipelineMonitor()
    
    try:
        monitor.run_monitoring()
        
        # Print summary
        print("\n=== Pipeline Monitoring Summary ===")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("Check logs/pipeline_monitoring.log for detailed information")
        print("Check results/monitoring_report.json for full report")
        
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
