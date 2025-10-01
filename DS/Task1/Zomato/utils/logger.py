import logging
import os
from datetime import datetime
from config.settings import LoggingConfig

class DataCleaningLogger:
    """Custom logger for data cleaning pipeline"""
    
    def __init__(self, name: str = "DataCleaning"):
        self.config = LoggingConfig()
        self._setup_logging()
        self.logger = logging.getLogger(name)
    
    def _setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(self.config.LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.config.LOG_LEVEL),
            format=self.config.LOG_FORMAT,
            handlers=[
                logging.FileHandler(self.config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
    
    def log_step_start(self, step_name: str):
        """Log the start of a processing step"""
        self.logger.info(f"🚀 Starting: {step_name}")
    
    def log_step_complete(self, step_name: str, details: str = ""):
        """Log the completion of a processing step"""
        message = f"✅ Completed: {step_name}"
        if details:
            message += f" | {details}"
        self.logger.info(message)
    
    def log_warning(self, message: str):
        """Log a warning message"""
        self.logger.warning(f"⚠️ {message}")
    
    def log_error(self, message: str):
        """Log an error message"""
        self.logger.error(f"❌ {message}")
    
    def log_metrics(self, metrics: dict):
        """Log data quality metrics"""
        self.logger.info("📊 Data Quality Metrics:")
        for key, value in metrics.items():
            self.logger.info(f"   {key}: {value}")