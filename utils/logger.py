"""
CreditGuard Research Logger
RESEARCH GAP 5 & 6: Comprehensive evaluation tracking for thesis analysis
"""
import csv
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from src.core.schemas import ResearchLog, CreditDecision


class ResearchLogger:
    """
    Appends evaluation metrics to research_logs.csv for analysis.
    Each request generates exactly one log row.
    """
    
    def __init__(self, log_path: Path = Path("logs/research_logs.csv")):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()
    
    def _init_csv(self):
        """Create CSV with headers if it doesn't exist."""
        if not self.log_path.exists():
            headers = [
                'timestamp', 'applicant_id', 'routing_strategy',
                'ml_confidence_score', 'ml_prediction',
                'total_tokens', 'prompt_tokens', 'completion_tokens',
                'latency_ms', 'active_key_id', 'key_switches',
                'is_fairness_triggered', 'fairness_decision_changed',
                'final_decision', 'risk_score'
            ]
            
            with open(self.log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
    
    def log_decision(
        self,
        decision: CreditDecision,
        key_switches: int = 0
    ):
        """
        Append a single row to research_logs.csv.
        
        RESEARCH GAP 5: Token density tracking
        RESEARCH GAP 6: Enables single-agent vs multi-agent comparison
        """
        # Calculate token metrics
        total_tokens = decision.total_tokens
        prompt_tokens = sum(
            r.tokens_used for r in decision.llm_results
        ) if decision.llm_results else 0
        completion_tokens = total_tokens - prompt_tokens
        
        # Extract fairness metrics
        fairness_triggered = (
            decision.fairness_check.is_triggered
            if decision.fairness_check else False
        )
        fairness_changed = (
            decision.fairness_check.decision_changed
            if decision.fairness_check else False
        )
        
        # Get ML confidence if available
        ml_confidence = (
            decision.ml_prediction.confidence_score
            if decision.ml_prediction else None
        )
        ml_prob = (
            decision.ml_prediction.default_probability
            if decision.ml_prediction else None
        )
        
        log_entry = ResearchLog(
            timestamp=decision.timestamp,
            applicant_id=decision.applicant_id,
            routing_strategy=decision.routing_strategy_used,
            ml_confidence_score=ml_confidence,
            ml_prediction=ml_prob,
            total_tokens=total_tokens,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            latency_ms=decision.processing_time_ms,
            active_key_id=self._extract_key_id(decision),
            key_switches=key_switches,
            is_fairness_triggered=fairness_triggered,
            fairness_decision_changed=fairness_changed,
            final_decision=decision.decision,
            risk_score=decision.final_risk_score
        )
        
        # Append to CSV
        with open(self.log_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                log_entry.timestamp.isoformat(),
                log_entry.applicant_id,
                log_entry.routing_strategy.value,
                log_entry.ml_confidence_score,
                log_entry.ml_prediction,
                log_entry.total_tokens,
                log_entry.prompt_tokens,
                log_entry.completion_tokens,
                log_entry.latency_ms,
                log_entry.active_key_id,
                log_entry.key_switches,
                log_entry.is_fairness_triggered,
                log_entry.fairness_decision_changed,
                log_entry.final_decision,
                log_entry.risk_score
            ])
    
    def _extract_key_id(self, decision: CreditDecision) -> str:
        """Get the active API key from LLM results."""
        if decision.llm_results:
            # Assume all agents used same key in one request
            return "key_used"  # Would need to be tracked in agent results
        return "ml_only"
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """
        RESEARCH GAP 6: Generate comparative metrics from logs.
        """
        if not self.log_path.exists():
            return {"error": "No logs found"}
        
        with open(self.log_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            return {"error": "Log file is empty"}
        
        total = len(rows)
        
        # Token usage analysis
        total_tokens = sum(int(r['total_tokens']) for r in rows)
        avg_tokens = total_tokens / total
        
        # Latency analysis
        avg_latency = sum(float(r['latency_ms']) for r in rows) / total
        
        # Routing breakdown
        routing_dist = {}
        for r in rows:
            strategy = r['routing_strategy']
            routing_dist[strategy] = routing_dist.get(strategy, 0) + 1
        
        # Fairness metrics
        fairness_triggers = sum(
            1 for r in rows if r['is_fairness_triggered'] == 'True'
        )
        fairness_changes = sum(
            1 for r in rows if r['fairness_decision_changed'] == 'True'
        )
        
        # Key rotation events
        total_switches = sum(int(r['key_switches']) for r in rows)
        
        return {
            "total_evaluations": total,
            "token_usage": {
                "total": total_tokens,
                "average_per_request": avg_tokens,
                "estimated_cost_usd": total_tokens * 0.0000001  # Mock pricing
            },
            "performance": {
                "average_latency_ms": avg_latency
            },
            "routing_distribution": routing_dist,
            "fairness_metrics": {
                "triggers": fairness_triggers,
                "decision_changes": fairness_changes,
                "change_rate": fairness_changes / fairness_triggers if fairness_triggers > 0 else 0
            },
            "resilience": {
                "total_key_switches": total_switches,
                "switch_rate": total_switches / total
            }
        }