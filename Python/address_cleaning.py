import pandas as pd
import re

# Load the dataset
file_path = "raw_addresses.csv"  # Update with your actual file path
df = pd.read_csv(file_path)

# Function to clean individual address components
def clean_address(address):
    if pd.isnull(address):
        return None

    # Convert to lowercase and strip whitespace
    address = address.strip().lower()

    # Standardize common abbreviations
    address = re.sub(r'\bst\b\.?', 'street', address)
    address = re.sub(r'\brd\b\.?', 'road', address)
    address = re.sub(r'\bave\b\.?', 'avenue', address)
    address = re.sub(r'\bdr\b\.?', 'drive', address)
    address = re.sub(r'\bln\b\.?', 'lane', address)
    address = re.sub(r'\bpkwy\b\.?', 'parkway', address)
    address = re.sub(r'\bblvd\b\.?', 'boulevard', address)

    # Remove multiple spaces
    address = re.sub(r'\s+', ' ', address)

    # Capitalize the first letter of each word
    address = address.title()

    return address

# Apply the cleaning function to the address column
if 'address' in df.columns:
    df['cleaned_address'] = df['address'].apply(clean_address)
else:
    print("No 'address' column found in the dataset. Please check the file.")

# Standardize city names
def clean_city(city):
    if pd.isnull(city):
        return None
    return city.strip().title()

if 'city' in df.columns:
    df['cleaned_city'] = df['city'].apply(clean_city)

# Standardize state abbreviations
state_mapping = {
    'ga': 'Georgia',
    'fl': 'Florida',
    'al': 'Alabama',
    'tn': 'Tennessee',
    # Add other states as needed
}

def clean_state(state):
    if pd.isnull(state):
        return None
    state = state.strip().lower()
    return state_mapping.get(state, state.title())

if 'state' in df.columns:
    df['cleaned_state'] = df['state'].apply(clean_state)

# Standardize ZIP codes
def clean_zip(zip_code):
    if pd.isnull(zip_code):
        return None
    zip_code = str(zip_code).strip()
    if len(zip_code) == 5 and zip_code.isdigit():
        return zip_code
    elif len(zip_code) > 5 and '-' in zip_code:
        return zip_code.split('-')[0]
    return None

if 'zip' in df.columns:
    df['cleaned_zip'] = df['zip'].apply(clean_zip)

# Save the cleaned dataset
output_file = "cleaned_addresses.csv"
df.to_csv(output_file, index=False)

print(f"Address cleaning completed. Cleaned file saved as '{output_file}'.")
