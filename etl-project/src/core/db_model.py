# TODO: Convert this to some kind of model (like dbt) rather than creating tables from scratch

from db_setup import warehouse_connection_string
import pyodbc

def create_warehouse_tables():
    """Create optimized data warehouse tables"""
    
    # Table creation scripts
    create_tables_sql = """
    -- Drop existing tables if they exist
    IF OBJECT_ID('dbo.customer_enriched', 'U') IS NOT NULL DROP TABLE dbo.customer_enriched;
    IF OBJECT_ID('dbo.enrichment_audit', 'U') IS NOT NULL DROP TABLE dbo.enrichment_audit;
    
    -- Main customer data table
    CREATE TABLE customer_enriched (
        customer_id INT PRIMARY KEY,
        first_name NVARCHAR(50) NOT NULL,
        last_name NVARCHAR(50) NOT NULL,
        email NVARCHAR(100) NOT NULL,
        phone NVARCHAR(20),
        postcode NVARCHAR(10),
        
        -- Geographic enrichment
        region NVARCHAR(50),
        country NVARCHAR(50),
        district NVARCHAR(50),
        longitude DECIMAL(10,7),
        latitude DECIMAL(10,7),
        geo_enriched BIT DEFAULT 0,
        
        -- Business enrichment
        company NVARCHAR(100),
        company_size NVARCHAR(50),
        industry NVARCHAR(50),
        annual_revenue NVARCHAR(50),
        is_business BIT DEFAULT 0,
        
        -- Risk assessment
        calculated_risk NVARCHAR(20),
        risk_score_numeric INT,
        risk_factors NVARCHAR(500),
        
        -- Account status
        status NVARCHAR(20),
        
        -- ETL metadata
        processed_date DATETIME2 DEFAULT GETDATE(),
        data_source NVARCHAR(50),
        enrichment_status NVARCHAR(50),
        
        -- Audit fields
        created_date DATETIME2 DEFAULT GETDATE(),
        modified_date DATETIME2 DEFAULT GETDATE()
    );
    
    -- Audit table for tracking all loading operations
    CREATE TABLE enrichment_audit (
        audit_id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        operation_type NVARCHAR(20), -- INSERT, UPDATE, DELETE
        records_processed INT,
        records_successful INT,
        records_failed INT,
        processing_start DATETIME2,
        processing_end DATETIME2,
        duration_seconds AS DATEDIFF(SECOND, processing_start, processing_end),
        error_message NVARCHAR(1000),
        pipeline_version NVARCHAR(20)
    );
    
    -- Create indexes for better query performance
    CREATE INDEX IX_customer_enriched_region ON customer_enriched(region);
    CREATE INDEX IX_customer_enriched_risk ON customer_enriched(calculated_risk);
    CREATE INDEX IX_customer_enriched_business ON customer_enriched(is_business);
    CREATE INDEX IX_customer_enriched_status ON customer_enriched(status);
    """
    
    try:
        conn = pyodbc.connect(warehouse_connection_string)
        cursor = conn.cursor()
        
        # Execute table creation
        cursor.execute(create_tables_sql)
        conn.commit()
        
        print("✅ Data warehouse tables created successfully")
        print("   - customer_enriched (main data table)")
        print("   - enrichment_audit (processing audit trail)")
        print("   - Performance indexes created")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return False

# Create the tables
print("=== CREATING DATA WAREHOUSE TABLES ===")
table_creation_success = create_warehouse_tables()