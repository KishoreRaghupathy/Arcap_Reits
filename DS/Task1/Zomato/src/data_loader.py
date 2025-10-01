import pandas as pd
import kagglehub
import os
from typing import Tuple, Optional
from utils.logger import DataCleaningLogger
from config.settings import DataCleaningConfig

class DataLoader:
    """Handles dataset downloading and loading operations"""
    
    def __init__(self):
        self.config = DataCleaningConfig()
        self.logger = DataCleaningLogger("DataLoader")
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.config.LOCAL_DATA_PATH, exist_ok=True)
        os.makedirs(self.config.CLEANED_DATA_PATH, exist_ok=True)
    
    def download_dataset(self) -> str:
        """
        Download dataset from Kaggle
        
        Returns:
            str: Path to downloaded dataset
        """
        self.logger.log_step_start("Dataset Download")
        
        try:
            path = kagglehub.dataset_download(self.config.DATASET_NAME)
            self.logger.log_step_complete("Dataset Download", f"Dataset downloaded to: {path}")
            return path
        except Exception as e:
            self.logger.log_error(f"Failed to download dataset: {str(e)}")
            # Fallback to local file if download fails
            csv_files = [f for f in os.listdir('.') if f.endswith('.csv') and 'zomato' in f.lower()]
            if csv_files:
                self.logger.log_warning(f"Using local file: {csv_files[0]}")
                return '.'
            raise
    
    def load_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Load data from CSV file
        
        Args:
            file_path: Path to CSV file. If None, tries to find in downloaded path
            
        Returns:
            pd.DataFrame: Loaded dataset
        """
        self.logger.log_step_start("Data Loading")
        
        try:
            if file_path is None:
                # Try to find the CSV file in downloaded path
                downloaded_path = self.download_dataset()
                csv_files = [f for f in os.listdir(downloaded_path) if f.endswith('.csv')]
                if not csv_files:
                    # Look for CSV files in current directory
                    csv_files = [f for f in os.listdir('.') if f.endswith('.csv') and 'zomato' in f.lower()]
                    if not csv_files:
                        raise FileNotFoundError("No CSV files found")
                    file_path = csv_files[0]
                else:
                    file_path = os.path.join(downloaded_path, csv_files[0])
            
            df = pd.read_csv(file_path)
            self.logger.log_step_complete("Data Loading", 
                                        f"Loaded {len(df)} rows and {len(df.columns)} columns")
            return df
        
        except Exception as e:
            self.logger.log_error(f"Failed to load data: {str(e)}")
            raise
    
    def get_data_info(self, df: pd.DataFrame) -> dict:
        """
        Get basic information about the dataset
        
        Args:
            df: Input DataFrame
            
        Returns:
            dict: Data information metrics
        """
        return {
            'original_shape': df.shape,
            'columns': df.columns.tolist(),
            'data_types': df.dtypes.to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        }