import pandas as pd
import numpy as np
import re
from typing import Tuple, Dict
from utils.logger import DataCleaningLogger
from config.settings import DataCleaningConfig

class DataCleaner:
    """Handles data cleaning operations"""
    
    def __init__(self):
        self.config = DataCleaningConfig()
        self.logger = DataCleaningLogger("DataCleaner")
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the dataset
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with handled missing values
        """
        self.logger.log_step_start("Missing Values Handling")
        
        df_cleaned = df.copy()
        initial_missing = df_cleaned.isnull().sum().sum()
        
        # Drop columns with too many missing values
        threshold = len(df_cleaned) * self.config.MISSING_THRESHOLD
        columns_to_drop = df_cleaned.columns[df_cleaned.isnull().sum() > threshold].tolist()
        
        if columns_to_drop:
            df_cleaned = df_cleaned.drop(columns=columns_to_drop)
            self.logger.log_warning(f"Dropped columns with >50% missing values: {columns_to_drop}")
        
        # Column-specific imputation strategies
        imputation_strategy = {
            'rate': '0 out of 5',
            'approx_cost(for two people)': df_cleaned['approx_cost(for two people)'].median(),
            'cuisines': 'Unknown',
            'dish_liked': 'Not Specified'
        }
        
        for column, strategy in imputation_strategy.items():
            if column in df_cleaned.columns:
                df_cleaned[column] = df_cleaned[column].fillna(strategy)
        
        final_missing = df_cleaned.isnull().sum().sum()
        self.logger.log_step_complete("Missing Values Handling", 
                                    f"Removed {initial_missing - final_missing} missing values")
        
        return df_cleaned
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate rows from the dataset
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame without duplicates
        """
        self.logger.log_step_start("Duplicate Removal")
        
        initial_rows = len(df)
        df_cleaned = df.drop_duplicates()
        duplicates_removed = initial_rows - len(df_cleaned)
        
        self.logger.log_step_complete("Duplicate Removal", 
                                    f"Removed {duplicates_removed} duplicate rows")
        
        return df_cleaned
    
    def standardize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize text columns (strip, title case, etc.)
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized text
        """
        self.logger.log_step_start("Text Standardization")
        
        df_cleaned = df.copy()
        
        for column in self.config.TEXT_COLUMNS:
            if column in df_cleaned.columns:
                df_cleaned[column] = (
                    df_cleaned[column]
                    .astype(str)
                    .str.strip()
                    .str.title()
                )
        
        self.logger.log_step_complete("Text Standardization", 
                                    f"Standardized {len(self.config.TEXT_COLUMNS)} text columns")
        
        return df_cleaned
    
    def correct_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns to proper data types
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with corrected data types
        """
        self.logger.log_step_start("Data Type Correction")
        
        df_cleaned = df.copy()
        
        # Clean and convert rate column
        if 'rate' in df_cleaned.columns:
            df_cleaned['rate_clean'] = (
                df_cleaned['rate']
                .apply(self._extract_rating)
                .fillna(df_cleaned['rate'].apply(self._extract_rating).median())
            )
        
        # Clean and convert cost column
        if 'approx_cost(for two people)' in df_cleaned.columns:
            df_cleaned['cost_for_two'] = (
                df_cleaned['approx_cost(for two people)']
                .apply(self._clean_cost)
            )
        
        self.logger.log_step_complete("Data Type Correction", 
                                    "Converted rate and cost columns to numeric")
        
        return df_cleaned
    
    def handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle outliers in numerical columns using IQR method
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with handled outliers
        """
        self.logger.log_step_start("Outlier Handling")
        
        df_cleaned = df.copy()
        numerical_cols = ['rate_clean', 'cost_for_two']
        
        outlier_report = {}
        
        for col in numerical_cols:
            if col in df_cleaned.columns:
                Q1 = df_cleaned[col].quantile(0.25)
                Q3 = df_cleaned[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - self.config.OUTLIER_IQR_MULTIPLIER * IQR
                upper_bound = Q3 + self.config.OUTLIER_IQR_MULTIPLIER * IQR
                
                # Count and cap outliers
                outliers_count = ((df_cleaned[col] < lower_bound) | (df_cleaned[col] > upper_bound)).sum()
                df_cleaned[col] = np.clip(df_cleaned[col], lower_bound, upper_bound)
                
                outlier_report[col] = outliers_count
        
        self.logger.log_step_complete("Outlier Handling", 
                                    f"Outliers handled: {outlier_report}")
        
        return df_cleaned
    
    def _extract_rating(self, rate_value) -> float:
        """Extract numeric rating from string format"""
        if pd.isna(rate_value):
            return np.nan
        
        try:
            if isinstance(rate_value, str):
                match = re.search(r'(\d+\.?\d*)', str(rate_value))
                if match:
                    return float(match.group(1))
            return float(rate_value)
        except (ValueError, TypeError):
            return np.nan
    
    def _clean_cost(self, cost_value) -> float:
        """Clean and convert cost to float"""
        if pd.isna(cost_value):
            return np.nan
        
        try:
            if isinstance(cost_value, str):
                return float(cost_value.replace(',', ''))
            return float(cost_value)
        except (ValueError, TypeError):
            return np.nan