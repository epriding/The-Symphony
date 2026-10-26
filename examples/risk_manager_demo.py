"""Offline risk-to-execution example; all books and timing are synthetic.

Run: venv/bin/python -m examples.risk_manager_demo
The shared paper demo covers risk reservations, partial fills, linked legs,
fees and marked portfolio state through the asynchronous paper coordinator.
"""
import asyncio
from .paper_trading_demo import main

if __name__ == '__main__':
    asyncio.run(main())
