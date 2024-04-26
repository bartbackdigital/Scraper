import pandas as pd

# Load the original dataset
original_df = pd.read_csv('outputs-2024-04-22.csv')

# Load the updated dataset
updated_df = pd.read_csv('updated-outputs-2024-04-22.csv')

# Convert URLs from both datasets to sets
original_urls = set(original_df['FD URL'])
updated_urls = set(updated_df['FD URL'])

# Find the difference between the original and updated datasets
missing_urls = original_urls - updated_urls

# Output the missing URLs
print("Missing URLs:")
for url in missing_urls:
    print(url)
