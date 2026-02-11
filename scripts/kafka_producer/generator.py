"""
Sales data generator.
"""

import random
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class SalesDataGenerator:
    """Generate realistic sales data."""
    
    # Default products and customers
    DEFAULT_PRODUCTS = [
        'BK-R93R-62', 'BK-M82S-44', 'BK-R50B-62', 'TI-M267',
        'TT-R982', 'HL-U509-B', 'LJ-0192-M', 'VE-C304-M',
        'SH-W890-L', 'FE-6654', 'CA-1098', 'CL-9009'
    ]
    
    def __init__(self, starting_order_num: int, csv_path: str = None):
        """Initialize generator."""
        self.current_order_num = starting_order_num
        self.current_order = None
        self.last_order_date = None
        
        # Load patterns from CSV or use defaults
        if csv_path:
            self._load_from_csv(csv_path)
        else:
            self._use_defaults()
        
        logger.info(f"Generator ready, starting from SO{starting_order_num}")
    
    def _load_from_csv(self, csv_path: str) -> None:
        """Load patterns from CSV."""
        try:
            df = pd.read_csv(csv_path)
            self.products = df['sls_prd_key'].unique().tolist()
            self.customers = df['sls_cust_id'].unique().tolist()
            
            # Calculate price ranges
            self.price_ranges = {}
            for product in self.products:
                product_data = df[df['sls_prd_key'] == product]
                self.price_ranges[product] = {
                    'min': float(product_data['sls_price'].min()),
                    'max': float(product_data['sls_price'].max()),
                    'mean': float(product_data['sls_price'].mean())
                }
            
            logger.info(f"Loaded {len(self.products)} products from CSV")
        except Exception as e:
            logger.error(f"Error loading CSV: {e}, using defaults")
            self._use_defaults()
    
    def _use_defaults(self) -> None:
        """Use default patterns."""
        self.products = self.DEFAULT_PRODUCTS
        self.customers = list(range(10000, 30000))
        
        # Default price ranges by product type
        self.price_ranges = {}
        for product in self.products:
            if 'BK-' in product:
                price_range = (699.0, 3578.0)
            elif 'TI-' in product or 'TT-' in product:
                price_range = (4.0, 35.0)
            else:
                price_range = (8.0, 120.0)
            
            self.price_ranges[product] = {
                'min': price_range[0],
                'max': price_range[1],
                'mean': sum(price_range) / 2
            }
    
    def start_new_order(self) -> None:
        """Start a new order."""
        self.current_order_num += 1
        
        # Calculate dates
        if self.last_order_date:
            order_date = self.last_order_date + timedelta(days=random.randint(0, 3))
        else:
            order_date = datetime.now()
        
        self.last_order_date = order_date
        ship_date = order_date + timedelta(days=random.randint(5, 10))
        due_date = ship_date + timedelta(days=5)
        
        # Random number of items (1-5)
        max_items = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]
        
        self.current_order = {
            'order_num': f"SO{self.current_order_num}",
            'customer_id': random.choice(self.customers),
            'order_dt': order_date.strftime('%Y%m%d'),
            'ship_dt': ship_date.strftime('%Y%m%d'),
            'due_dt': due_date.strftime('%Y%m%d'),
            'max_items': max_items,
            'items_generated': 0
        }
    
    def generate_record(self) -> dict:
        """Generate a single sales record."""
        # Start new order if needed
        if (self.current_order is None or 
            self.current_order['items_generated'] >= self.current_order['max_items']):
            self.start_new_order()
        
        # Select product and quantity
        product = random.choice(self.products)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 8, 5, 2])[0]
        
        # Calculate price
        price_info = self.price_ranges[product]
        price = round(random.uniform(
            max(price_info['min'], price_info['mean'] * 0.8),
            min(price_info['max'], price_info['mean'] * 1.2)
        ), 2)
        
        sales = round(price * quantity, 2)
        
        self.current_order['items_generated'] += 1
        
        return {
            'sls_ord_num': self.current_order['order_num'],
            'sls_prd_key': product,
            'sls_cust_id': self.current_order['customer_id'],
            'sls_order_dt': self.current_order['order_dt'],
            'sls_ship_dt': self.current_order['ship_dt'],
            'sls_due_dt': self.current_order['due_dt'],
            'sls_sales': sales,
            'sls_quantity': quantity,
            'sls_price': price,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_current_order_num(self) -> int:
        """Get current order number."""
        return self.current_order_num