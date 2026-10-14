"""
algopilot/agent — the AI options strategist that checks each pattern before a sell.

At 1/3 (a GREEN candle on CE or PE) it collects the market picture and asks the
strategist model whether selling that side would work if candle 2 turns RED. At
2/3 (the moment the sell order would go in) the engine asks for the verdict:
SELL with enough confidence -> the order goes in as usual; SKIP -> no order.

The agent can only BLOCK a sell. It never places, moves or closes orders. If it is
off, over budget, slow or failing, the engine trades the pattern as before.
"""
from .agent import TradeAgent, Verdict

__all__ = ["TradeAgent", "Verdict"]
