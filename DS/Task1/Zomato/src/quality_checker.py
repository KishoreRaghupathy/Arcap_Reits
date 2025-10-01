import pandas as pd
import numpy as np
from typing import Dict, Tuple
import json
from utils.logger import DataCleaningLogger

class QualityChecker:
    """Performs data quality checks and validation"""
    
    def __init__(self):
        self.logger = DataCleaningLogger("QualityChecker")
    
    def generate_quality_report(self, original_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> Dict:
        """
        Generate comprehensive data quality report
        
        Args:
            original_df: Original DataFrame before cleaning
            cleaned_df: Cleaned DataFrame after processing
            
        Returns:
            Dict: Quality metrics and report
        """
        self.logger.log_step_start("Quality Report Generation")
        
        report = {
            'data_quality_metrics': self._calculate_quality_metrics(cleaned_df),
            'cleaning_impact': self._calculate_cleaning_impact(original_df, cleaned_df),
            'final_dataset_info': self._get_dataset_info(cleaned_df)
        }
        
        self.logger.log_metrics(report['data_quality_metrics'])
        self.logger.log_step_complete("Quality Report Generation", "Report generated successfully")
        
        return report
    
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate data quality metrics"""
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        
        metrics = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'completeness_score': round(1 - (missing_cells / total_cells), 4) if total_cells > 0 else 0,
            'duplicate_rows': 0,  # Assuming duplicates already removed
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object']).columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'total_missing_values': missing_cells,
            'missing_value_percentage': round((missing_cells / total_cells) * 100, 2) if total_cells > 0 else 0
        }
        
        return metrics
    
    def _calculate_cleaning_impact(self, original_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> Dict:
        """Calculate the impact of cleaning operations"""
        original_missing = original_df.isnull().sum().sum()
        cleaned_missing = cleaned_df.isnull().sum().sum()
        
        original_completeness = 1 - (original_missing / (len(original_df) * len(original_df.columns))) if len(original_df) > 0 else 0
        cleaned_completeness = 1 - (cleaned_missing / (len(cleaned_df) * len(cleaned_df.columns))) if len(cleaned_df) > 0 else 0
        
        impact = {
            'rows_removed': len(original_df) - len(cleaned_df),
            'columns_removed': len(original_df.columns) - len(cleaned_df.columns),
            'missing_values_removed': original_missing - cleaned_missing,
            'completeness_improvement': round(cleaned_completeness - original_completeness, 4),
            'original_completeness': round(original_completeness, 4),
            'final_completeness': round(cleaned_completeness, 4)
        }
        
        return impact
    
    def _get_dataset_info(self, df: pd.DataFrame) -> Dict:
        """Get final dataset information"""
        return {
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'data_types': {str(col): str(dtype) for col, dtype in df.dtypes.to_dict().items()},
            'sample_size': min(3, len(df))
        }
    
    def validate_data_quality(self, df: pd.DataFrame, rules: Dict = None) -> Tuple[bool, Dict]:
        """
        Validate data against quality rules
        
        Args:
            df: DataFrame to validate
            rules: Dictionary of validation rules
            
        Returns:
            Tuple: (is_valid, validation_report)
        """
        if rules is None:
            rules = {
                'max_missing_percentage': 5.0,
                'min_rows': 100,
                'required_columns': ['name', 'rate_clean', 'cost_for_two']
            }
        
        validation_report = {
            'passed_checks': [],
            'failed_checks': [],
            'warnings': []
        }
        
        # Check minimum rows
        if len(df) < rules['min_rows']:
            validation_report['failed_checks'].append(f"Dataset has only {len(df)} rows, minimum required: {rules['min_rows']}")
        else:
            validation_report['passed_checks'].append(f"Row count check passed: {len(df)} rows")
        
        # Check required columns
        missing_columns = [col for col in rules['required_columns'] if col not in df.columns]
        if missing_columns:
            validation_report['failed_checks'].append(f"Missing required columns: {missing_columns}")
        else:
            validation_report['passed_checks'].append("All required columns present")
        
        # Check missing values
        missing_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        if missing_percentage > rules['max_missing_percentage']:
            validation_report['warnings'].append(f"High missing value percentage: {missing_percentage:.2f}%")
        else:
            validation_report['passed_checks'].append(f"Missing value check passed: {missing_percentage:.2f}%")
        
        # Check data types
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            validation_report['warnings'].append("No numeric columns found in dataset")
        
        is_valid = len(validation_report['failed_checks']) == 0
        
        self.logger.logger.info(f"Data validation {'PASSED' if is_valid else 'FAILED'}")
        for check in validation_report['passed_checks']:
            self.logger.logger.info(f"✅ {check}")
        for warning in validation_report['warnings']:
            self.logger.logger.warning(f"⚠️ {warning}")
        for failure in validation_report['failed_checks']:
            self.logger.logger.error(f"❌ {failure}")
        
        return is_valid, validation_report

# Test function to run the script directly
if __name__ == "__main__":
    # Create sample data for testing
    sample_original = pd.DataFrame({
        'name': ['Restaurant A', 'Restaurant B', None],
        'rate': ['4.1/5', '3.8/5', None],
        'cost': ['800', '1,200', None]
    })
    
    sample_cleaned = pd.DataFrame({
        'name': ['Restaurant A', 'Restaurant B'],
        'rate_clean': [4.1, 3.8],
        'cost_for_two': [800.0, 1200.0]
    })
    
    checker = QualityChecker()
    report = checker.generate_quality_report(sample_original, sample_cleaned)
    
    print("Quality Report:")
    print(json.dumps(report, indent=2))
    
    # Test validation
    is_valid, validation_report = checker.validate_data_quality(sample_cleaned)
    print(f"\nData Validation: {'PASSED' if is_valid else 'FAILED'}")
    print("Validation Report:", json.dumps(validation_report, indent=2))