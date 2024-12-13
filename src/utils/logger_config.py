import logging
import watchtower
import boto3
from datetime import datetime

def setup_cloudwatch_logging(app_name='maps-server'):
    # Create logger
    logger = logging.getLogger()
    
    # Clear any existing handlers
    logger.handlers = []
    
    logger.setLevel(logging.INFO)
    
    # Create CloudWatch Logs client
    logs = boto3.client('logs', region_name='eu-central-1')
    
    # Create CloudWatch handler
    handler = watchtower.CloudWatchLogHandler(
        log_group=f'{app_name}-logs',
        log_stream_name=datetime.now().strftime('%Y-%m-%d'),
        boto3_client=logs
    )
    
    # Set formatter to handle missing fields gracefully
    class SafeFormatter(logging.Formatter):
        def format(self, record):
            if not hasattr(record, 'frontend_data'):
                record.frontend_data = ''
            if not hasattr(record, 'session_id'):
                record.session_id = ''
            # Add log level color or prefix for better visibility
            if record.levelno == logging.ERROR:
                record.levelname = f'⛔ {record.levelname}'
            elif record.levelno == logging.WARNING:
                record.levelname = f'⚠️ {record.levelname}'
            return super().format(record)
    
    formatter = SafeFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - Frontend Data: %(frontend_data)s - Session: %(session_id)s'
    )
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    # Set SQLAlchemy logging to a higher level (WARNING or ERROR) to reduce noise
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    return logger