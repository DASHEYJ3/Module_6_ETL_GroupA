import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine
import urllib

from ..core.db_setup import warehouse_connection_string

# Set up SQLAlchemy engine for pandas compatibility
params = urllib.parse.quote_plus(warehouse_connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


@pytest.mark.parametrize("table", [
    "customer_enriched",
    "orders", # for example
    "products", # for example
])
def test_table_has_records(engine, table):
    """Validate record counts for important tables"""
    count_query = f"SELECT COUNT(*) FROM {table}"
    result = pd.read_sql(count_query, engine).iat[0, 0]
    if isinstance(result, (int, float, np.integer, np.floating)):
        total_records = int(result)
    else:
        raise AssertionError(f"Unexpected count result type: {type(result)} -> {result}")
    assert total_records > 0, f"No records found in {table}"

@pytest.fixture(scope="module")
def completeness(engine):
    """Fetch completeness metrics once per test run"""
    query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 1 ELSE 0 END) as complete_names,
            SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as complete_emails,
            SUM(CASE WHEN geo_enriched = 1 THEN 1 ELSE 0 END) as geo_enriched_count,
            SUM(CASE WHEN is_business = 1 THEN 1 ELSE 0 END) as business_customers
        FROM customer_enriched
    """
    df = pd.read_sql(query, engine)
    return df.iloc[0].to_dict()

@pytest.mark.parametrize("field, min_ratio", [
    ("complete_names",   0.90),   # expect at least 90% names
    ("complete_emails",  0.95),   # expect at least 95% emails
    ("geo_enriched_count", 0.50), # at least half geo enriched
    ("business_customers", 0.50), # at least half business
])
def test_completeness(completeness, field, min_ratio):
    total = int(completeness["total_records"])
    value = int(completeness[field])
    ratio = value / total if total > 0 else 0
    assert ratio >= min_ratio, (
        f"{field} too low: {value}/{total} ({ratio:.1%}), "
        f"expected ≥ {min_ratio:.0%}"
    )

# OLD CODE

def validate_loaded_data():
        
        # 3. Risk distribution analysis
        risk_query = """
        SELECT 
            calculated_risk,
            COUNT(*) as customer_count,
            CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
        FROM customer_enriched 
        GROUP BY calculated_risk
        ORDER BY customer_count DESC
        """
        
        risk_df = pd.read_sql(risk_query, engine)
        print(f"\n✅ Risk Distribution:")
        for _, row in risk_df.iterrows():
            print(f"   {row['calculated_risk']} Risk: {row['customer_count']} customers ({row['percentage']}%)")
        
        # 4. Geographic distribution
        geo_query = """
        SELECT TOP 5
            region,
            COUNT(*) as customer_count
        FROM customer_enriched 
        WHERE region IS NOT NULL AND region != 'Unknown'
        GROUP BY region
        ORDER BY customer_count DESC
        """
        
        geo_df = pd.read_sql(geo_query, engine)
        print(f"\n✅ Top Regions by Customer Count:")
        for _, row in geo_df.iterrows():
            print(f"   {row['region']}: {row['customer_count']} customers")
        
        # 5. Audit trail verification
        audit_query = """
        SELECT 
            batch_id,
            operation_type,
            records_processed,
            records_successful,
            records_failed,
            duration_seconds,
            processing_start
        FROM enrichment_audit 
        ORDER BY processing_start DESC
        """
        
        audit_df = pd.read_sql(audit_query, engine)
        print(f"\n✅ Recent Processing Batches:")
        for _, row in audit_df.iterrows():
            success_rate = (row['records_successful'] / row['records_processed'] * 100) if row['records_processed'] > 0 else 0
            print(f"   Batch: {str(row['batch_id'])[:8]}... | {row['records_processed']} records | {success_rate:.1f}% success | {row['duration_seconds']}s")
        
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

# Run validation
validation_success = validate_loaded_data()