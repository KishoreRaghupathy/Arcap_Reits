import unittest
import pandas as pd
import numpy as np
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer

class TestDataCleaning(unittest.TestCase):
    
    def setUp(self):
        self.cleaner = DataCleaner()
        self.engineer = FeatureEngineer()
        
        # Sample test data
        self.sample_data = pd.DataFrame({
            'rate': ['4.1/5', '3.8/5', np.nan, 'New'],
            'approx_cost(for two people)': ['800', '1,200', np.nan, '500'],
            'cuisines': ['North Indian, Chinese', 'Italian', np.nan, 'South Indian'],
            'name': [' restaurant A ', 'restaurant b', 'RESTAURANT C', 'd  ']
        })
    
    def test_extract_rating(self):
        self.assertEqual(self.cleaner._extract_rating('4.1/5'), 4.1)
        self.assertEqual(self.cleaner._extract_rating('3.8'), 3.8)
        self.assertTrue(pd.isna(self.cleaner._extract_rating('New')))
    
    def test_clean_cost(self):
        self.assertEqual(self.cleaner._clean_cost('1,200'), 1200.0)
        self.assertEqual(self.cleaner._clean_cost('800'), 800.0)
    
    def test_count_cuisines(self):
        self.assertEqual(self.engineer._count_cuisines('North Indian, Chinese'), 2)
        self.assertEqual(self.engineer._count_cuisines('Italian'), 1)
        self.assertEqual(self.engineer._count_cuisines(np.nan), 0)

if __name__ == '__main__':
    unittest.main()