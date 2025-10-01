import os
from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class DataCleaningConfig:
    """Configuration for data cleaning pipeline"""
    
    # Dataset configuration
    DATASET_NAME: str = "himanshupoddar/zomato-bangalore-restaurants"
    LOCAL_DATA_PATH: str = "data/raw"
    CLEANED_DATA_PATH: str = "data/processed"
    
    # Cleaning parameters
    MISSING_THRESHOLD: float = 0.5  # Drop columns with >50% missing values
    OUTLIER_IQR_MULTIPLIER: float = 1.5
    
    # Column mappings - using field with default_factory for mutable objects
    TEXT_COLUMNS: List[str] = field(default_factory=lambda: ['name', 'location', 'rest_type', 'cuisines', 'dish_liked'])
    NUMERICAL_COLUMNS: List[str] = field(default_factory=lambda: ['rate', 'approx_cost(for two people)'])
    
    # Cost categories
    COST_BINS: List[float] = field(default_factory=lambda: [0, 500, 1000, 2000, float('inf')])
    COST_LABELS: List[str] = field(default_factory=lambda: ['Budget', 'Moderate', 'Expensive', 'Premium'])
    
    # Rating categories
    RATING_BINS: List[float] = field(default_factory=lambda: [0, 2, 3, 4, 5])
    RATING_LABELS: List[str] = field(default_factory=lambda: ['Poor', 'Average', 'Good', 'Excellent'])

@dataclass
class LoggingConfig:
    """Configuration for logging"""
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE: str = "logs/data_cleaning.log"