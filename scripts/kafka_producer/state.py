"""
State management for order number persistence.
"""

import json
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

STATE_FILE = '.producer_state.json'
DEFAULT_ORDER_NUM = 75122


def load_last_order_num() -> int:
    """
    Load the last order number from state file.
    Returns default if not found.
    """
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                order_num = state.get('last_order_num')
                if order_num:
                    logger.info(f"Resuming from order: SO{order_num}")
                    return order_num
    except Exception as e:
        logger.error(f"Could not load state: {e}")
    
    logger.info(f"Starting with order: SO{DEFAULT_ORDER_NUM}")
    return DEFAULT_ORDER_NUM


def save_last_order_num(order_num: int) -> None:
    """Save the last order number to state file."""
    try:
        state = {
            'last_order_num': order_num,
            'last_updated': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved state: SO{order_num}")
    except Exception as e:
        logger.error(f"Could not save state: {e}")