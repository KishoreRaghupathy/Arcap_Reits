import pandas as pd
import os

def create_sample_data():
    """Create sample data for testing if no dataset is available"""
    sample_data = {
        'name': ['Restaurant A', 'Restaurant B', 'Restaurant C', 'Restaurant D'],
        'rate': ['4.1/5', '3.8/5', '4.3/5', '3.5/5'],
        'approx_cost(for two people)': ['800', '1,200', '600', '1,500'],
        'cuisines': ['North Indian, Chinese', 'Italian', 'South Indian', 'Chinese, Thai'],
        'location': ['BTM Layout', 'Koramangala', 'JP Nagar', 'Indiranagar'],
        'rest_type': ['Casual Dining', 'Fine Dining', 'Quick Bites', 'Casual Dining']
    }
    
    df = pd.DataFrame(sample_data)
    df.to_csv('sample_zomato_data.csv', index=False)
    print("✅ Sample data created: sample_zomato_data.csv")
    return 'sample_zomato_data.csv'

if __name__ == "__main__":
    # Create sample data for testing
    sample_file = create_sample_data()
    print(f"Run the main pipeline with: python main.py {sample_file}")