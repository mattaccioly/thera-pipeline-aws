import json
import boto3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
sns_client = boto3.client('sns')
ssm_client = boto3.client('ssm')

# Environment variables
COST_TRACKING_TABLE = os.environ.get('COST_TRACKING_TABLE', 'thera-llm-cost-tracking')
ALERT_TOPIC_ARN = os.environ.get('ALERT_TOPIC_ARN', '')
DAILY_BUDGET_USD = float(os.environ.get('DAILY_BUDGET_USD', '50.0'))

class CostMonitor:
    """Monitors and manages LLM processing costs"""
    
    def __init__(self):
        self.cost_table = dynamodb.Table(COST_TRACKING_TABLE)
        self.daily_budget = DAILY_BUDGET_USD
        self.alert_thresholds = [0.8, 0.9, 1.0]  # 80%, 90%, 100%
    
    def get_daily_cost(self, date: str = None) -> float:
        """Get total cost for a specific date"""
        if not date:
            date = datetime.utcnow().strftime('%Y-%m-%d')
        
        try:
            response = self.cost_table.get_item(Key={'date': date})
            if 'Item' in response:
                return float(response['Item'].get('total_cost_usd', 0.0))
        except Exception as e:
            logger.error(f"Error getting daily cost for {date}: {e}")
        
        return 0.0
    
    def update_daily_cost(self, cost: float, model: str = 'claude-haiku', 
                         request_type: str = 'general') -> None:
        """Update daily cost tracking"""
        date = datetime.utcnow().strftime('%Y-%m-%d')
        
        try:
            # Get current cost
            current_cost = self.get_daily_cost(date)
            new_total = current_cost + cost
            
            # Update cost tracking
            self.cost_table.put_item(Item={
                'date': date,
                'total_cost_usd': Decimal(str(new_total)),
                'last_updated': datetime.utcnow().isoformat(),
                'model_breakdown': {
                    model: {
                        'requests': 1,
                        'cost': Decimal(str(cost))
                    }
                },
                'request_type_breakdown': {
                    request_type: {
                        'requests': 1,
                        'cost': Decimal(str(cost))
                    }
                }
            })
            
            # Check budget thresholds
            self._check_budget_thresholds(new_total)
            
            logger.info(f"Updated daily cost: ${new_total:.4f} (added ${cost:.4f})")
            
        except Exception as e:
            logger.error(f"Error updating daily cost: {e}")
    
    def _check_budget_thresholds(self, current_cost: float) -> None:
        """Check if cost exceeds budget thresholds and send alerts"""
        utilization = current_cost / self.daily_budget
        
        for threshold in self.alert_thresholds:
            if utilization >= threshold:
                self._send_budget_alert(utilization, current_cost)
                break
    
    def _send_budget_alert(self, utilization: float, current_cost: float) -> None:
        """Send budget alert via SNS"""
        if not ALERT_TOPIC_ARN:
            logger.warning("No alert topic ARN configured")
            return
        
        try:
            message = {
                'alert_type': 'budget_threshold',
                'utilization_percent': round(utilization * 100, 1),
                'current_cost': round(current_cost, 2),
                'daily_budget': self.daily_budget,
                'remaining_budget': round(self.daily_budget - current_cost, 2),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Subject=f"LLM Budget Alert: {utilization*100:.1f}% utilized",
                Message=json.dumps(message, indent=2)
            )
            
            logger.info(f"Sent budget alert: {utilization*100:.1f}% utilization")
            
        except Exception as e:
            logger.error(f"Error sending budget alert: {e}")
    
    def get_cost_summary(self, days: int = 7) -> Dict:
        """Get cost summary for the last N days"""
        try:
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days-1)
            
            costs = []
            total_cost = 0.0
            
            for i in range(days):
                date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                daily_cost = self.get_daily_cost(date)
                costs.append({
                    'date': date,
                    'cost': daily_cost
                })
                total_cost += daily_cost
            
            return {
                'period_days': days,
                'total_cost': total_cost,
                'average_daily_cost': total_cost / days,
                'daily_costs': costs,
                'budget_utilization': (total_cost / (self.daily_budget * days)) * 100
            }
            
        except Exception as e:
            logger.error(f"Error getting cost summary: {e}")
            return {}
    
    def emit_cost_metrics(self) -> None:
        """Emit cost metrics to CloudWatch"""
        try:
            current_cost = self.get_daily_cost()
            utilization = (current_cost / self.daily_budget) * 100
            
            cloudwatch.put_metric_data(
                Namespace='TheraPipeline/LLMCost',
                MetricData=[
                    {
                        'MetricName': 'DailyCost',
                        'Value': current_cost,
                        'Unit': 'None',
                        'Timestamp': datetime.utcnow()
                    },
                    {
                        'MetricName': 'BudgetUtilization',
                        'Value': utilization,
                        'Unit': 'Percent',
                        'Timestamp': datetime.utcnow()
                    },
                    {
                        'MetricName': 'RemainingBudget',
                        'Value': self.daily_budget - current_cost,
                        'Unit': 'None',
                        'Timestamp': datetime.utcnow()
                    }
                ]
            )
            
        except Exception as e:
            logger.error(f"Error emitting cost metrics: {e}")

class CacheManager:
    """Manages intelligent caching for LLM results"""
    
    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name)
        self.cache_ttl = 7 * 24 * 60 * 60  # 7 days
        self.cleanup_threshold = 1000  # Cleanup when cache exceeds this size
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        try:
            response = self.table.scan(Select='COUNT')
            total_items = response['Count']
            
            # Get items by age
            now = int(datetime.utcnow().timestamp())
            recent_items = 0
            expired_items = 0
            
            for item in self.table.scan():
                created_at = item.get('created_at', 0)
                expires_at = item.get('expires_at', 0)
                
                if now - created_at < 24 * 60 * 60:  # Last 24 hours
                    recent_items += 1
                
                if now > expires_at:
                    expired_items += 1
            
            return {
                'total_items': total_items,
                'recent_items': recent_items,
                'expired_items': expired_items,
                'hit_rate': self._calculate_hit_rate()
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
    
    def _calculate_hit_rate(self) -> float:
        """Calculate cache hit rate (simplified)"""
        # This would need to be implemented with proper hit/miss tracking
        return 0.0
    
    def cleanup_expired_items(self) -> int:
        """Remove expired cache items"""
        try:
            now = int(datetime.utcnow().timestamp())
            expired_items = []
            
            # Find expired items
            for item in self.table.scan():
                if item.get('expires_at', 0) < now:
                    expired_items.append(item['cache_key'])
            
            # Delete expired items
            deleted_count = 0
            for cache_key in expired_items:
                try:
                    self.table.delete_item(Key={'cache_key': cache_key})
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"Error deleting expired item {cache_key}: {e}")
            
            logger.info(f"Cleaned up {deleted_count} expired cache items")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up expired items: {e}")
            return 0
    
    def optimize_cache(self) -> Dict:
        """Optimize cache by removing least recently used items"""
        try:
            stats = self.get_cache_stats()
            
            if stats.get('total_items', 0) < self.cleanup_threshold:
                return {'action': 'no_cleanup_needed', 'items_removed': 0}
            
            # Remove oldest 20% of items
            items_to_remove = int(stats['total_items'] * 0.2)
            
            # Get oldest items
            oldest_items = []
            for item in self.table.scan():
                oldest_items.append({
                    'cache_key': item['cache_key'],
                    'created_at': item.get('created_at', 0)
                })
            
            # Sort by creation time and remove oldest
            oldest_items.sort(key=lambda x: x['created_at'])
            removed_count = 0
            
            for item in oldest_items[:items_to_remove]:
                try:
                    self.table.delete_item(Key={'cache_key': item['cache_key']})
                    removed_count += 1
                except Exception as e:
                    logger.warning(f"Error removing item {item['cache_key']}: {e}")
            
            logger.info(f"Optimized cache: removed {removed_count} items")
            return {'action': 'optimized', 'items_removed': removed_count}
            
        except Exception as e:
            logger.error(f"Error optimizing cache: {e}")
            return {'action': 'error', 'items_removed': 0}

def lambda_handler(event, context):
    """Main Lambda handler for cost monitoring and cache management"""
    try:
        # Initialize components
        cost_monitor = CostMonitor()
        cache_manager = CacheManager(os.environ.get('CACHE_TABLE', 'thera-advanced-summaries'))
        
        # Get cost summary
        cost_summary = cost_monitor.get_cost_summary(7)
        
        # Emit metrics
        cost_monitor.emit_cost_metrics()
        
        # Get cache stats
        cache_stats = cache_manager.get_cache_stats()
        
        # Cleanup expired items
        expired_removed = cache_manager.cleanup_expired_items()
        
        # Optimize cache if needed
        cache_optimization = cache_manager.optimize_cache()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Cost monitoring and cache management completed',
                'cost_summary': cost_summary,
                'cache_stats': cache_stats,
                'expired_items_removed': expired_removed,
                'cache_optimization': cache_optimization,
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Error in cost monitoring: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    test_event = {}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
