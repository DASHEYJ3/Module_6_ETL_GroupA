import pyodbc

# Database connection configuration
SERVER = 'localhost'  # Your SQL Server instance
DATABASE = 'customer_warehouse'  # We'll create this database

# Connection string for initial setup (master database)
master_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE=master;Trusted_Connection=yes;'

warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

print("=== DATABASE SETUP ===")
try:
    # Connect to master database to create our warehouse (with autocommit for CREATE DATABASE)
    master_conn = pyodbc.connect(master_connection_string, autocommit=True)
    master_cursor = master_conn.cursor()
    
    # Create database if it doesn't exist
    try:
        master_cursor.execute(f"CREATE DATABASE {DATABASE}")
        print(f"✅ Created database: {DATABASE}")
    except pyodbc.Error as e:
        if "already exists" in str(e):
            print(f"ℹ️  Database {DATABASE} already exists")
        else:
            print(f"⚠️  Database creation issue: {e}")
    
    master_conn.close()
    print("Database setup complete")
    
except Exception as e:
    print(f"❌ Database setup failed: {e}")
    print("Please check your SQL Server connection")