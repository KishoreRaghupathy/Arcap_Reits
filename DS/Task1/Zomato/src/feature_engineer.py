import pandas as pd
from typing import List
from utils.logger import DataCleaningLogger
from config.settings import DataCleaningConfig

class FeatureEngineer:
    """Handles feature engineering operations"""
    
    def __init__(self):
        self.config = DataCleaningConfig()
        self.logger = DataCleaningLogger("FeatureEngineer")
    
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create new features from existing data
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with new features
        """
        self.logger.log_step_start("Feature Engineering")
        
        df_with_features = df.copy()
        
        # Create number of cuisines feature
        if 'cuisines' in df_with_features.columns:
            df_with_features['num_cuisines'] = (
                df_with_features['cuisines']
                .apply(self._count_cuisines)
            )
        
        # Create cost categories
        if 'cost_for_two' in df_with_features.columns:
            df_with_features['cost_category'] = pd.cut(
                df_with_features['cost_for_two'],
                bins=self.config.COST_BINS,
                labels=self.config.COST_LABELS
            )
        
        # Create rating categories
        if 'rate_clean' in df_with_features.columns:
            df_with_features['rating_category'] = pd.cut(
                df_with_features['rate_clean'],
                bins=self.config.RATING_BINS,
                labels=self.config.RATING_LABELS
            )
        
        self.logger.log_step_complete("Feature Engineering", 
                                    "Created num_cuisines, cost_category, rating_category features")
        
        return df_with_features
    
    def _count_cuisines(self, cuisines_str: str) -> int:
        """Count number of cuisines from comma-separated string"""
        if pd.isna(cuisines_str) or cuisines_str in ['Unknown', 'Not Specified']:
            return 0
        
        try:
            return len(str(cuisines_str).split(','))
        except:
            return 0