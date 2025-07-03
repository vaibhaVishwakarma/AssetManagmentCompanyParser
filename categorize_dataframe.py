
import pandas as pd
from category_lookup import category_lookup

def get_category(investment_type):
    for category, types in category_lookup.items():
        if investment_type in types:
            return category
    return "Unknown"

# Example DataFrame (replace with your actual DataFrame)
data = {
    'type': [
        'Equity Shares',
        'ETF',
        'Bonds',
        'Commercial Paper',
        'REIT',
        'Mutual Funds',
        'Futures',
        'Gold',
        'Some New Type'
    ]
}
df = pd.DataFrame(data)

# Apply the categorization
df['category'] = df['type'].apply(get_category)

print(df)


