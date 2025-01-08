# ERP Data Migration Project: Oracle NetSuite  

## Project Overview  
This project showcases my experience in designing and implementing a data migration pipeline for transferring data from a legacy ERP system to Oracle NetSuite. The goal was to ensure data consistency, quality, and readiness for seamless integration into the new system.  

## Key Components  
1. **Data Cleaning and Preparation (SQL)**  
   - Cleaned and standardized customer data (`customer_data_cleaning.sql`).  
   - Prepared inventory items for migration (`inventory_items_preparation.sql`).  
   - Processed and validated contacts data (`contacts_data_cleaning.sql`).  
   - Prepared Bill of Materials (BOMs) data (`BOMs_preparation.sql`).  

2. **Address Cleaning (Python)**  
   - Developed a Python script (`address_cleaning.py`) to clean and standardize address data for consistency and accuracy.  

## Technologies Used  
- **SQL**: For data extraction, cleaning, and preparation.  
- **Python**: For advanced cleaning tasks such as address validation and standardization.  

## Files Included  
### SQL Files  
- `customer_data_cleaning.sql`: Removes duplicates, standardizes formatting, and resolves inconsistencies in customer records.  
- `inventory_items_preparation.sql`: Cleans and prepares inventory data for migration.  
- `contacts_data_cleaning.sql`: Cleans and validates contact details.  
- `BOMs_preparation.sql`: Prepares Bill of Materials (BOMs) data for integration.  

### Python Script  
- `address_cleaning.py`: Uses libraries such as Pandas to clean, standardize, and validate addresses.  

## How to Use  
1. Run the SQL scripts in order to clean and prepare data from the legacy ERP system.  
2. Execute the Python script to clean and validate address data.  

## Outcomes  
- Ensured data quality and consistency across critical ERP components (customers, inventory, BOMs, contacts).  
- Streamlined the migration process and reduced errors in data integration into Oracle NetSuite.  
