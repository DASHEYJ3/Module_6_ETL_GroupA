import pyodbc

# Define connection parameters
SERVER = 'your_server_name'
DATABASE = 'customer_warehouse'
warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

def create_warehouse_tables_and_views():
    """Create optimized data warehouse tables and views"""
    
    # Table creation scripts
    create_tables_sql = """
    IF OBJECT_ID('dbo.customer_enriched', 'U') IS NOT NULL DROP TABLE dbo.customer_enriched;
    IF OBJECT_ID('dbo.enrichment_audit', 'U') IS NOT NULL DROP TABLE dbo.enrichment_audit;

    CREATE TABLE customer_enriched (
        customer_id INT PRIMARY KEY,
        first_name NVARCHAR(50) NOT NULL,
        last_name NVARCHAR(50) NOT NULL,
        email NVARCHAR(100) NOT NULL,
        phone NVARCHAR(20),
        postcode NVARCHAR(10),
        region NVARCHAR(50),
        country NVARCHAR(50),
        district NVARCHAR(50),
        longitude DECIMAL(10,7),
        latitude DECIMAL(10,7),
        geo_enriched BIT DEFAULT 0,
        company NVARCHAR(100),
        company_size NVARCHAR(50),
        industry NVARCHAR(50),
        annual_revenue NVARCHAR(50),
        is_business BIT DEFAULT 0,
        calculated_risk NVARCHAR(20),
        risk_score_numeric INT,
        risk_factors NVARCHAR(500),
        status NVARCHAR(20),
        processed_date DATETIME2 DEFAULT GETDATE(),
        data_source NVARCHAR(50),
        enrichment_status NVARCHAR(50),
        created_date DATETIME2 DEFAULT GETDATE(),
        modified_date DATETIME2 DEFAULT GETDATE()
    );

    CREATE TABLE enrichment_audit (
        audit_id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        operation_type NVARCHAR(20),
        records_processed INT,
        records_successful INT,
        records_failed INT,
        processing_start DATETIME2,
        processing_end DATETIME2,
        duration_seconds AS DATEDIFF(SECOND, processing_start, processing_end),
        error_message NVARCHAR(1000),
        pipeline_version NVARCHAR(20)
    );

    CREATE INDEX IX_customer_enriched_region ON customer_enriched(region);
    CREATE INDEX IX_customer_enriched_risk ON customer_enriched(calculated_risk);
    CREATE INDEX IX_customer_enriched_business ON customer_enriched(is_business);
    CREATE INDEX IX_customer_enriched_status ON customer_enriched(status);
    """

    # View creation scripts
    create_views_sql = """
    IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'risk')
    BEGIN
        EXEC('CREATE VIEW risk AS
        SELECT DISTINCT
              risk_score_numeric,
              calculated_risk,
              risk_factors
        FROM dbo.customer_enriched')
    END;

    IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'address_info')
    BEGIN
        EXEC('CREATE VIEW address_info AS
        SELECT DISTINCT
              postcode,
              region,
              country,
              district,
              longitude,
              latitude,
              geo_enriched
        FROM dbo.customer_enriched')
    END;

    IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'customer_normalised')
    BEGIN
        EXEC('CREATE VIEW customer_normalised AS
        SELECT
              customer_id,
              first_name,
              last_name,
              email,
              phone,
              postcode,
              company,
              company_size,
              industry,
              annual_revenue,
              is_business,
              risk_score_numeric,
              status,
              processed_date,
              data_source,
              enrichment_status,
              created_date,
              modified_date
        FROM dbo.customer_enriched')
    END;
    """

    try:
        conn = pyodbc.connect(warehouse_connection_string)
        cursor = conn.cursor()

        # Execute table creation
        cursor.execute(create_tables_sql)
        conn.commit()

        # Execute view creation
        cursor.execute(create_views_sql)
        conn.commit()

        print("✅ Data warehouse tables and views created successfully")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Table or view creation failed: {e}")
        return False

# Run the function
print("=== CREATING DATA WAREHOUSE TABLES AND VIEWS ===")
creation_success = create_warehouse_tables_and_views()