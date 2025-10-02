from datetime import datetime
import pandas as pd
import pyodbc
from typing import Tuple, Dict
import uuid

from .core.db_setup import warehouse_connection_string

class DatabaseLoader:
    """Production database loading with comprehensive error handling"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.batch_id = str(uuid.uuid4())
        
    def load_enriched_customers(self, df_customers: pd.DataFrame) -> Dict:
        """
        Load enriched customer data with comprehensive error handling
        Returns: Loading statistics and results
        """
        start_time = datetime.now()
        results = {
            'batch_id': self.batch_id,
            'total_records': len(df_customers),
            'successful_inserts': 0,
            'successful_updates': 0,
            'failed_records': 0,
            'errors': [],
            'processing_time': 0
        }
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            print(f"🔄 Starting batch load: {self.batch_id}")
            print(f"📊 Processing {len(df_customers)} customer records")
            
            # Process each customer with upsert logic
            for index, customer in df_customers.iterrows():
                try:
                    # Check if customer already exists
                    check_sql = "SELECT COUNT(*) FROM customer_enriched WHERE customer_id = ?"
                    cursor.execute(check_sql, customer['customer_id'])
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        # UPDATE existing record
                        update_sql = """
                        UPDATE customer_enriched SET
                            first_name = ?, last_name = ?, email = ?, phone = ?, postcode = ?,
                            region = ?, country = ?, district = ?, longitude = ?, latitude = ?, geo_enriched = ?,
                            company = ?, company_size = ?, industry = ?, annual_revenue = ?, is_business = ?,
                            calculated_risk = ?, risk_score_numeric = ?, risk_factors = ?,
                            status = ?, processed_date = ?, data_source = ?, enrichment_status = ?,
                            modified_date = GETDATE()
                        WHERE customer_id = ?
                        """
                        
                        cursor.execute(update_sql, (
                            customer['first_name'], customer['last_name'], customer['email'],
                            customer['phone'], customer['postcode'],
                            customer['region'], customer['country'], customer['district'],
                            customer['longitude'], customer['latitude'], customer['geo_enriched'],
                            customer['company'], customer['company_size'], customer['industry'],
                            customer['annual_revenue'], customer['is_business'],
                            customer['calculated_risk'], customer['risk_score_numeric'], customer['risk_factors'],
                            customer['status'], customer['processed_date'], customer['data_source'],
                            customer['enrichment_status'], customer['customer_id']
                        ))
                        
                        results['successful_updates'] += 1
                        print(f"  ✏️  Updated customer {customer['customer_id']}")
                        
                    else:
                        # INSERT new record
                        insert_sql = """
                        INSERT INTO customer_enriched (
                            customer_id, first_name, last_name, email, phone, postcode,
                            region, country, district, longitude, latitude, geo_enriched,
                            company, company_size, industry, annual_revenue, is_business,
                            calculated_risk, risk_score_numeric, risk_factors,
                            status, processed_date, data_source, enrichment_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        cursor.execute(insert_sql, (
                            customer['customer_id'], customer['first_name'], customer['last_name'],
                            customer['email'], customer['phone'], customer['postcode'],
                            customer['region'], customer['country'], customer['district'],
                            customer['longitude'], customer['latitude'], customer['geo_enriched'],
                            customer['company'], customer['company_size'], customer['industry'],
                            customer['annual_revenue'], customer['is_business'],
                            customer['calculated_risk'], customer['risk_score_numeric'], customer['risk_factors'],
                            customer['status'], customer['processed_date'], customer['data_source'],
                            customer['enrichment_status']
                        ))
                        
                        results['successful_inserts'] += 1
                        print(f"  ➕ Inserted customer {customer['customer_id']}")
                
                except Exception as record_error:
                    error_msg = f"Customer {customer['customer_id']}: {str(record_error)}"
                    results['errors'].append(error_msg)
                    results['failed_records'] += 1
                    print(f"  ❌ Failed: {error_msg}")
                    
            # Commit all changes
            conn.commit()
            
            # Record processing time
            end_time = datetime.now()
            results['processing_time'] = (end_time - start_time).total_seconds()
            
            # Log audit information
            self._log_audit_record(cursor, start_time, end_time, results)
            conn.commit()
            
            conn.close()
            
            print(f"\n✅ Batch load completed successfully")
            
        except Exception as e:
            results['errors'].append(f"Critical loading error: {str(e)}")
            print(f"❌ Critical loading error: {e}")
            
        return results
    
    def _log_audit_record(self, cursor, start_time, end_time, results):
        """Log detailed audit information"""
        audit_sql = """
        INSERT INTO enrichment_audit (
            batch_id, operation_type, records_processed, records_successful, 
            records_failed, processing_start, processing_end, error_message, pipeline_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        error_summary = '; '.join(results['errors'][:5]) if results['errors'] else None
        successful_records = results['successful_inserts'] + results['successful_updates']
        
        cursor.execute(audit_sql, (
            self.batch_id,
            'UPSERT',
            results['total_records'],
            successful_records,
            results['failed_records'],
            start_time,
            end_time,
            error_summary,
            'ETL_Pipeline_v1.0'
        ))

# Execute the loading process
print("=== LOADING ENRICHED DATA TO DATABASE ===")

if table_creation_success:
    loader = DatabaseLoader(warehouse_connection_string)
    loading_results = loader.load_enriched_customers(df_enriched)
    
    print("\n=== LOADING RESULTS ===")
    print(f"Batch ID: {loading_results['batch_id']}")
    print(f"Total records: {loading_results['total_records']}")
    print(f"Successful inserts: {loading_results['successful_inserts']}")
    print(f"Successful updates: {loading_results['successful_updates']}")
    print(f"Failed records: {loading_results['failed_records']}")
    print(f"Processing time: {loading_results['processing_time']:.2f} seconds")
    
    if loading_results['errors']:
        print("\nErrors encountered:")
        for error in loading_results['errors']:
            print(f"  ⚠️  {error}")
else:
    print("❌ Skipping data loading due to table creation failure")