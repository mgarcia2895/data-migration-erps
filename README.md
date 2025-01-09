# ERP Data Migration Project: Oracle NetSuite  

## Project Overview  
This project showcases my experience in designing and implementing a data migration pipeline for transferring data from a legacy ERP system to Oracle NetSuite. The goal was to ensure data consistency, quality, and readiness for seamless integration into the new system.  

## Key Components  
1. **Data Cleaning and Preparation (SQL)**  
   - Cleaned and standardized customer data (`Customers_Full.sql`).  
   - Prepared Purchase inventory items for migration (`Purchase_Items_Full.sql`).
   - Prepared Assembly inventory items for migration (`Assembly_Items_Full.sql`).    
   - Processed and validated contacts data (`Contacts_Full.sql`).  
   - Prepared Bill of Materials (BOMs) data (`Bill_of_Materials_Full.sql`).
   - Prepared Bill of Materials (BOMs) Routings data ('Manufacturing_Routing_Full.sql).
   - Prepared Vendor data ('Vendors_Full.sql') 

2. **Address Cleaning (Python)**  
   - Developed a Python script (`address_cleaning.py`) to clean and standardize address data for consistency and accuracy.  

## Technologies Used  
- **SQL**: For data extraction, cleaning, and preparation.  
- **Python**: For advanced cleaning tasks such as address validation and standardization.  

## Files Included  
### SQL Files  
- `Customers_Full.sql`: Removes duplicates, standardizes formatting, and resolves inconsistencies in customer records.  
- `Purchase_Items_Full.sql`: Cleans and prepares purchased inventory data for migration.
- `Assembly_Items_Full.sql`: Cleans and prepares assembly inventory data for migration.    
- `Contacts_Full.sql`: Cleans and validates contact details.  
- `Bill_of_Materials_Full.sql`: Prepares Bill of Materials (BOMs) data for integration.
- `Manufacturing_Routing_Full.sql`: Prepares Bill of Materials (BOMs) Routings data for integration.
- `Vendors_Full.sql`: Cleans and validates Vendor details.  
 
 

### Python Script  
- `address_cleaning.py`: Uses libraries such as Pandas to clean, standardize, and validate addresses.  

## How to Use  
1. Run the SQL scripts in order to clean and prepare data from the legacy ERP system.  
2. Execute the Python script to clean and validate address data.  

## Outcomes  
- Ensured data quality and consistency across critical ERP components (customers, inventory, BOMs, contacts).  
- Streamlined the migration process and reduced errors in data integration into Oracle NetSuite.  
