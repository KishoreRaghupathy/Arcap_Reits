import pandas as pd
import json
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_loader import DataLoader
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer
from src.quality_checker import QualityChecker
from utils.logger import DataCleaningLogger
from config.settings import DataCleaningConfig

class ZomatoDataCleaningPipeline:
    """Main pipeline for Zomato data cleaning"""
    
    def __init__(self):
        self.config = DataCleaningConfig()
        self.logger = DataCleaningLogger("ZomatoPipeline")
        
        # Initialize components
        self.data_loader = DataLoader()
        self.data_cleaner = DataCleaner()
        self.feature_engineer = FeatureEngineer()
        self.quality_checker = QualityChecker()
        
        self.original_df = None
        self.cleaned_df = None
        self.quality_report = None
    
    def run_pipeline(self, local_file_path: str = None) -> pd.DataFrame:
        """
        Execute the complete data cleaning pipeline
        
        Args:
            local_file_path: Path to local CSV file (optional)
            
        Returns:
            pd.DataFrame: Cleaned and processed DataFrame
        """
        self.logger.logger.info("🎯 Starting Zomato Data Cleaning Pipeline")
        
        try:
            # Step 1: Load Data
            self.original_df = self.data_loader.load_data(local_file_path)
            
            # Step 2: Data Cleaning
            self.logger.logger.info("🧹 Starting Data Cleaning Phase...")
            df_cleaned = self.data_cleaner.handle_missing_values(self.original_df)
            df_cleaned = self.data_cleaner.remove_duplicates(df_cleaned)
            df_cleaned = self.data_cleaner.standardize_text_columns(df_cleaned)
            df_cleaned = self.data_cleaner.correct_data_types(df_cleaned)
            df_cleaned = self.data_cleaner.handle_outliers(df_cleaned)
            
            # Step 3: Feature Engineering
            self.logger.logger.info("⚡ Starting Feature Engineering Phase...")
            self.cleaned_df = self.feature_engineer.create_features(df_cleaned)
            
            # Step 4: Quality Check
            self.logger.logger.info("🔍 Starting Quality Assessment Phase...")
            self.quality_report = self.quality_checker.generate_quality_report(
                self.original_df, self.cleaned_df
            )
            
            # Step 5: Save Results
            self._save_results()
            
            self.logger.logger.info("🎉 Pipeline completed successfully!")
            return self.cleaned_df
            
        except Exception as e:
            self.logger.log_error(f"Pipeline failed: {str(e)}")
            raise
    
    def _save_results(self):
        """Save cleaned data and quality report"""
        # Save cleaned dataset
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{self.config.CLEANED_DATA_PATH}/zomato_cleaned_{timestamp}.csv"
        self.cleaned_df.to_csv(output_file, index=False)
        
        # Save quality report
        report_file = f"{self.config.CLEANED_DATA_PATH}/quality_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(self.quality_report, f, indent=2)
        
        self.logger.log_step_complete("Results Saving", 
                                    f"Data saved to: {output_file}\nReport saved to: {report_file}")
    
    def get_pipeline_summary(self) -> Dict:
        """Get pipeline execution summary"""
        if self.quality_report is None:
            return {"status": "Pipeline not executed"}
        
        return {
            "status": "completed",
            "original_shape": self.original_df.shape,
            "cleaned_shape": self.cleaned_df.shape,
            "quality_metrics": self.quality_report['data_quality_metrics'],
            "cleaning_impact": self.quality_report['cleaning_impact']
        }

def main():
    """Main execution function"""
    try:
        print("🚀 Zomato Data Cleaning Pipeline")
        print("=" * 50)
        
        # Initialize and run pipeline
        pipeline = ZomatoDataCleaningPipeline()
        
        # Check if local file is provided as argument
        local_file = None
        if len(sys.argv) > 1:
            local_file = sys.argv[1]
            print(f"Using local file: {local_file}")
        
        cleaned_data = pipeline.run_pipeline(local_file)
        
        # Print summary
        summary = pipeline.get_pipeline_summary()
        print("\n" + "="*60)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*60)
        for key, value in summary.items():
            if key == 'original_shape':
                print(f"Original Shape: {value}")
            elif key == 'cleaned_shape':
                print(f"Cleaned Shape: {value}")
            elif key == 'quality_metrics':
                print(f"Total Rows: {value.get('total_rows', 'N/A')}")
                print(f"Total Columns: {value.get('total_columns', 'N/A')}")
                print(f"Completeness Score: {value.get('completeness_score', 'N/A')}")
            elif key == 'cleaning_impact':
                print(f"Rows Removed: {value.get('rows_removed', 'N/A')}")
                print(f"Columns Removed: {value.get('columns_removed', 'N/A')}")
                print(f"Missing Values Removed: {value.get('missing_values_removed', 'N/A')}")
        print("="*60)
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())