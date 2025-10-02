# Disaster Recovery & Backup Procedures

## 1. Definitions

### Recovery Time Objective (RTO)
RTO is the maximum acceptable time to restore service after a disruption.  
**For this project:**  
**RTO = 2 hours** (from incident detection to full restoration of data and service).

### Recovery Point Objective (RPO)
RPO is the maximum acceptable amount of data loss measured in time.  
**For this project:**  
**RPO = 15 minutes** (no more than 15 minutes of data can be lost).

---

## 2. Backup & Recovery Procedures

### 2.1. Database & Data Backups

- **Frequency:** Every 15 minutes (automated incremental backup).
- **Scope:** All customer data tables, configuration files, and enrichment scripts.
- **Storage:**
  - **Primary:** Secure cloud storage (Azure Blob).
  - **Secondary:** Encrypted local backup on a separate server.
- **Retention:**
  - Daily backups kept for 30 days.
  - Weekly backups kept for 6 months.

#### Python Example: Automated Backup Script


import shutil
import datetime

def backup_database(db_path, backup_dir):
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f"{backup_dir}/customer_db_backup_{timestamp}.sqlite"
    shutil.copy2(db_path, backup_file)
    print(f"Backup created: {backup_file}")

#Schedule this script to run every 15 minutes using a scheduler.

# 2.2. Configuration Backups

Store all configuration files (YAML, JSON, .env) in version control (e.g., Git).
Nightly export of config files to backup storage.


# 3. Failover & Manual Override Procedures
# 3.1. Failover Scenarios

Primary DB Failure:

Switch to the latest backup on the secondary server.
Update application config to point to the failover DB.


Cloud Storage Outage:

Use local encrypted backup.
Notify IT and escalate to cloud provider support.



# 3.2. Manual Override

Manual Restore:

Identify latest valid backup.
Restore database using documented restore script.
Validate data integrity (run automated tests).

Manual Data Entry:

If automated restore fails, allow manual CSV import via admin tool.

#4. Disaster Recovery Testing Schedule

Test TypeFrequencyResponsibleNotesBackup Restore TestMonthlyData EngineeringRestore from backup, validate integrityFailover SimulationQuarterlyIT/DevOpsSimulate DB/cloud outage, test failoverConfig Restore TestMonthlyData EngineeringRestore config from backup| Full DR Drill         | Annually   | All Stakeholders | End-to-end recovery, including manual process |

# 5. Documentation & Audit

All backup and restore actions must be logged (timestamp, operator, outcome).
Store logs in a secure, tamper-evident location.
Review and update this document after each DR test or incident.
