#!/usr/bin/env python3
"""
PostgreSQL loader for CircleCI Usage API data.

This module provides functionality to load CircleCI usage data from CSV files
into a PostgreSQL database with proper schema and indexing.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional, Dict, Any, List
from datetime import datetime
import argparse

# Add parent directory to path for direct execution
if __name__ == '__main__':
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CircleCIPostgresLoader:
    """PostgreSQL loader for CircleCI usage data."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the PostgreSQL loader.
        
        Args:
            connection_params: Database connection parameters including:
                - host: Database host
                - port: Database port (default: 5432)
                - database: Database name
                - user: Username
                - password: Password
        """
        self.connection_params = connection_params
        self.connection = None
        
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL database")
    
    def create_schema(self) -> bool:
        """Create the database schema for CircleCI usage data."""
        schema_sql = """
        -- Create the main usage data table
        CREATE TABLE IF NOT EXISTS circleci_usage (
            id SERIAL PRIMARY KEY,
            organization_id VARCHAR(255),
            organization_name VARCHAR(255),
            organization_created_date TIMESTAMP,
            project_id VARCHAR(255),
            project_name VARCHAR(255),
            project_created_date TIMESTAMP,
            last_build_finished_at TIMESTAMP,
            vcs_name VARCHAR(100),
            vcs_url TEXT,
            vcs_branch VARCHAR(255),
            pipeline_id VARCHAR(255),
            pipeline_created_at TIMESTAMP,
            pipeline_number NUMERIC,
            is_unregistered_user BOOLEAN,
            pipeline_trigger_source VARCHAR(100),
            pipeline_trigger_user_id VARCHAR(255),
            workflow_id VARCHAR(255),
            workflow_name VARCHAR(255),
            workflow_first_job_queued_at TIMESTAMP,
            workflow_first_job_started_at TIMESTAMP,
            workflow_stopped_at TIMESTAMP,
            is_workflow_successful BOOLEAN,
            job_name VARCHAR(255),
            job_run_number NUMERIC,
            job_id VARCHAR(255),
            job_run_date TIMESTAMP,
            job_run_queued_at TIMESTAMP,
            job_run_started_at TIMESTAMP,
            job_run_stopped_at TIMESTAMP,
            job_build_status VARCHAR(50),
            resource_class VARCHAR(100),
            operating_system VARCHAR(100),
            executor VARCHAR(100),
            parallelism INTEGER,
            job_run_seconds NUMERIC,
            median_cpu_utilization_pct DECIMAL(5,2),
            max_cpu_utilization_pct DECIMAL(5,2),
            median_ram_utilization_pct DECIMAL(5,2),
            max_ram_utilization_pct DECIMAL(5,2),
            compute_credits DECIMAL(10,2),
            dlc_credits DECIMAL(10,2),
            user_credits DECIMAL(10,2),
            storage_credits DECIMAL(10,2),
            network_credits DECIMAL(10,2),
            lease_credits DECIMAL(10,2),
            lease_overage_credits DECIMAL(10,2),
            ipranges_credits DECIMAL(10,2),
            total_credits DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Unique constraint on job_id for upsert support
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_circleci_usage_job_id'
            ) THEN
                ALTER TABLE circleci_usage ADD CONSTRAINT uq_circleci_usage_job_id UNIQUE (job_id);
            END IF;
        END $$;

        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_organization_id ON circleci_usage(organization_id);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_project_id ON circleci_usage(project_id);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_pipeline_id ON circleci_usage(pipeline_id);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_workflow_id ON circleci_usage(workflow_id);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_job_name ON circleci_usage(job_name);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_job_build_status ON circleci_usage(job_build_status);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_resource_class ON circleci_usage(resource_class);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_executor ON circleci_usage(executor);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_pipeline_created_at ON circleci_usage(pipeline_created_at);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_job_run_started_at ON circleci_usage(job_run_started_at);
        CREATE INDEX IF NOT EXISTS idx_circleci_usage_total_credits ON circleci_usage(total_credits);
        
        -- Create a view for job performance analysis
        CREATE OR REPLACE VIEW job_performance AS
        SELECT 
            job_name,
            resource_class,
            executor,
            COUNT(*) as job_count,
            AVG(job_run_seconds) as avg_duration_seconds,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY job_run_seconds) as median_duration_seconds,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY job_run_seconds) as p95_duration_seconds,
            AVG(median_cpu_utilization_pct) as avg_cpu_utilization,
            AVG(median_ram_utilization_pct) as avg_ram_utilization,
            SUM(total_credits) as total_credits_used,
            AVG(total_credits) as avg_credits_per_job,
            SUM(CASE WHEN job_build_status = 'success' THEN 1 ELSE 0 END) as successful_jobs,
            SUM(CASE WHEN job_build_status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
            ROUND(
                SUM(CASE WHEN job_build_status = 'success' THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2
            ) as success_rate_pct
        FROM circleci_usage
        GROUP BY job_name, resource_class, executor
        ORDER BY total_credits_used DESC;
        
        -- Create a view for cost analysis
        CREATE OR REPLACE VIEW cost_analysis AS
        SELECT 
            organization_name,
            project_name,
            DATE_TRUNC('day', pipeline_created_at) as usage_date,
            resource_class,
            executor,
            COUNT(*) as job_count,
            SUM(total_credits) as total_credits,
            AVG(total_credits) as avg_credits_per_job,
            SUM(compute_credits) as total_compute_credits,
            SUM(dlc_credits) as total_dlc_credits,
            SUM(user_credits) as total_user_credits,
            SUM(storage_credits) as total_storage_credits,
            SUM(network_credits) as total_network_credits,
            SUM(lease_credits) as total_lease_credits
        FROM circleci_usage
        WHERE pipeline_created_at IS NOT NULL
        GROUP BY organization_name, project_name, DATE_TRUNC('day', pipeline_created_at), 
                 resource_class, executor
        ORDER BY usage_date DESC, total_credits DESC;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(schema_sql)
                self.connection.commit()
                logger.info("Database schema created successfully")
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to create schema: {e}")
            self.connection.rollback()
            return False
    
    def load_csv_data(self, csv_file_path: str, batch_size: int = 1000) -> bool:
        """
        Load data from CSV file into PostgreSQL database.
        
        Args:
            csv_file_path: Path to the CSV file
            batch_size: Number of records to insert per batch
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Loading data from {csv_file_path}")
            
            # Read CSV file in chunks to handle large files
            chunk_iter = pd.read_csv(csv_file_path, chunksize=batch_size, na_values=['\\N'])
            
            total_records = 0
            for chunk_num, chunk in enumerate(chunk_iter):
                logger.info(f"Processing chunk {chunk_num + 1}")
                
                # Clean and prepare data
                cleaned_chunk = self._clean_dataframe(chunk)
                
                # Insert data
                if self._insert_batch(cleaned_chunk):
                    total_records += len(cleaned_chunk)
                    logger.info(f"Inserted {len(cleaned_chunk)} records (total: {total_records})")
                else:
                    logger.error(f"Failed to insert chunk {chunk_num + 1}")
                    return False
            
            logger.info(f"Successfully loaded {total_records} records from {csv_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load CSV data: {e}")
            return False
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare DataFrame for database insertion."""
        # Create a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Convert column names to lowercase and replace spaces with underscores
        cleaned_df.columns = [col.lower().replace(' ', '_') for col in cleaned_df.columns]
        
        # Handle datetime columns
        datetime_columns = [
            'organization_created_date', 'project_created_date', 'last_build_finished_at',
            'pipeline_created_at', 'workflow_first_job_queued_at', 'workflow_first_job_started_at',
            'workflow_stopped_at', 'job_run_date', 'job_run_queued_at', 'job_run_started_at',
            'job_run_stopped_at'
        ]
        
        for col in datetime_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
        
        # Convert boolean columns
        boolean_columns = ['is_unregistered_user', 'is_workflow_successful']
        for col in boolean_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].map({'true': True, 'false': False, 'True': True, 'False': False})
        
        # Convert numeric columns
        numeric_columns = [
            'pipeline_number', 'parallelism', 'job_run_number', 'job_run_seconds',
            'median_cpu_utilization_pct', 'max_cpu_utilization_pct',
            'median_ram_utilization_pct', 'max_ram_utilization_pct',
            'compute_credits', 'dlc_credits', 'user_credits', 'storage_credits',
            'network_credits', 'lease_credits', 'lease_overage_credits',
            'ipranges_credits', 'total_credits'
        ]
        
        for col in numeric_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
                
                # Debug: Check for extremely large values in all numeric columns
                max_val = cleaned_df[col].max()
                min_val = cleaned_df[col].min()
                if pd.notna(max_val) and (max_val > 9223372036854775807 or min_val < -9223372036854775808):
                    logger.warning(f"Column {col} has values outside BIGINT range: min={min_val}, max={max_val}")
                    # Show some examples of problematic values
                    extreme_values = cleaned_df[cleaned_df[col] > 9223372036854775807][col].head(5)
                    if not extreme_values.empty:
                        logger.warning(f"Examples of large values in {col}: {extreme_values.tolist()}")
        
        # Convert parallelism to integer (it should be a small integer)
        if 'parallelism' in cleaned_df.columns:
            cleaned_df['parallelism'] = cleaned_df['parallelism'].astype('Int64')  # Nullable integer type
        
        # Replace NaN and NaT values with None for proper NULL handling in PostgreSQL
        # Do this after all conversions to catch any remaining NaN values
        cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)
        
        # Specifically handle NaT values in datetime columns
        for col in datetime_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].replace({pd.NaT: None})
        
        # Convert any remaining numpy NaN values to None (in case they weren't caught)
        cleaned_df = cleaned_df.replace({pd.NA: None, float('nan'): None})
        
        return cleaned_df
    
    def truncate(self) -> bool:
        """Truncate the circleci_usage table for a clean reload."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE circleci_usage")
                self.connection.commit()
                logger.info("Truncated circleci_usage table")
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to truncate table: {e}")
            self.connection.rollback()
            return False

    def load_directory(self, directory: str, batch_size: int = 1000) -> bool:
        """Load all CSV files from a directory."""
        import glob
        csv_files = sorted(glob.glob(os.path.join(directory, "*.csv")))
        if not csv_files:
            logger.warning(f"No CSV files found in {directory}")
            return True

        total = 0
        for csv_file in csv_files:
            logger.info(f"Loading {os.path.basename(csv_file)}")
            if not self.load_csv_data(csv_file, batch_size):
                return False
            with self.connection.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM circleci_usage")
                total = cur.fetchone()[0]
            logger.info(f"  Running total: {total} records")

        logger.info(f"Directory load complete: {total} records in database")
        return True

    def _insert_batch(self, df: pd.DataFrame) -> bool:
        """Insert a batch of records, skipping duplicates on job_id conflict."""
        if df.empty:
            return True
        
        # Define the column mapping
        column_mapping = {
            'organization_id': 'organization_id',
            'organization_name': 'organization_name',
            'organization_created_date': 'organization_created_date',
            'project_id': 'project_id',
            'project_name': 'project_name',
            'project_created_date': 'project_created_date',
            'last_build_finished_at': 'last_build_finished_at',
            'vcs_name': 'vcs_name',
            'vcs_url': 'vcs_url',
            'vcs_branch': 'vcs_branch',
            'pipeline_id': 'pipeline_id',
            'pipeline_created_at': 'pipeline_created_at',
            'pipeline_number': 'pipeline_number',
            'is_unregistered_user': 'is_unregistered_user',
            'pipeline_trigger_source': 'pipeline_trigger_source',
            'pipeline_trigger_user_id': 'pipeline_trigger_user_id',
            'workflow_id': 'workflow_id',
            'workflow_name': 'workflow_name',
            'workflow_first_job_queued_at': 'workflow_first_job_queued_at',
            'workflow_first_job_started_at': 'workflow_first_job_started_at',
            'workflow_stopped_at': 'workflow_stopped_at',
            'is_workflow_successful': 'is_workflow_successful',
            'job_name': 'job_name',
            'job_run_number': 'job_run_number',
            'job_id': 'job_id',
            'job_run_date': 'job_run_date',
            'job_run_queued_at': 'job_run_queued_at',
            'job_run_started_at': 'job_run_started_at',
            'job_run_stopped_at': 'job_run_stopped_at',
            'job_build_status': 'job_build_status',
            'resource_class': 'resource_class',
            'operating_system': 'operating_system',
            'executor': 'executor',
            'parallelism': 'parallelism',
            'job_run_seconds': 'job_run_seconds',
            'median_cpu_utilization_pct': 'median_cpu_utilization_pct',
            'max_cpu_utilization_pct': 'max_cpu_utilization_pct',
            'median_ram_utilization_pct': 'median_ram_utilization_pct',
            'max_ram_utilization_pct': 'max_ram_utilization_pct',
            'compute_credits': 'compute_credits',
            'dlc_credits': 'dlc_credits',
            'user_credits': 'user_credits',
            'storage_credits': 'storage_credits',
            'network_credits': 'network_credits',
            'lease_credits': 'lease_credits',
            'lease_overage_credits': 'lease_overage_credits',
            'ipranges_credits': 'ipranges_credits',
            'total_credits': 'total_credits'
        }
        
        # Select only the columns that exist in the dataframe
        available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        
        if not available_columns:
            logger.warning("No matching columns found in dataframe")
            return True
        
        # Prepare data for insertion
        columns = list(available_columns.values())
        values = []
        
        for _, row in df.iterrows():
            # Convert any NaN/NaT values to None when creating tuples
            value_tuple = tuple(
                None if pd.isna(row.get(col)) else row.get(col, None) 
                for col in available_columns.keys()
            )
            values.append(value_tuple)
        
        # UPSERT: skip duplicates based on job_id unique constraint
        insert_sql = f"""
            INSERT INTO circleci_usage ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (job_id) DO NOTHING
        """
        
        try:
            with self.connection.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_sql,
                    values,
                    template=None,
                    page_size=1000
                )
                self.connection.commit()
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to insert batch: {e}")
            
            # Debug: Try to identify problematic values
            logger.info("Attempting to identify problematic values...")
            for i, value_tuple in enumerate(values[:5]):  # Check first 5 rows
                logger.info(f"Row {i} values: {value_tuple}")
            
            self.connection.rollback()
            return False
    
    def get_data_summary(self) -> Optional[Dict[str, Any]]:
        """Get summary statistics about the loaded data."""
        summary_queries = {
            'total_records': "SELECT COUNT(*) FROM circleci_usage",
            'date_range': """
                SELECT 
                    MIN(pipeline_created_at) as earliest_pipeline,
                    MAX(pipeline_created_at) as latest_pipeline,
                    MIN(job_run_started_at) as earliest_job,
                    MAX(job_run_started_at) as latest_job
                FROM circleci_usage
            """,
            'organizations': "SELECT COUNT(DISTINCT organization_id) FROM circleci_usage",
            'projects': "SELECT COUNT(DISTINCT project_id) FROM circleci_usage",
            'total_credits': "SELECT SUM(total_credits) FROM circleci_usage",
            'job_status_breakdown': """
                SELECT job_build_status, COUNT(*) as count
                FROM circleci_usage
                GROUP BY job_build_status
                ORDER BY count DESC
            """,
            'resource_class_breakdown': """
                SELECT resource_class, COUNT(*) as count, SUM(total_credits) as total_credits
                FROM circleci_usage
                GROUP BY resource_class
                ORDER BY total_credits DESC
            """
        }
        
        try:
            summary = {}
            with self.connection.cursor() as cursor:
                for key, query in summary_queries.items():
                    cursor.execute(query)
                    if key in ['total_records', 'organizations', 'projects', 'total_credits']:
                        result = cursor.fetchone()
                        summary[key] = result[0] if result else 0
                    else:
                        result = cursor.fetchall()
                        summary[key] = result
            return summary
        except psycopg2.Error as e:
            logger.error(f"Failed to get data summary: {e}")
            return None


def add_parser(subparsers):
    """Add load-to-postgres command parser."""
    parser = subparsers.add_parser(
        'load-to-postgres',
        help='Load CircleCI usage data into PostgreSQL database'
    )
    parser.add_argument(
        'csv_file',
        help='Path to the CSV file to load'
    )
    parser.add_argument(
        '--host',
        default='localhost',
        help='PostgreSQL host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--database',
        required=True,
        help='PostgreSQL database name'
    )
    parser.add_argument(
        '--user',
        required=True,
        help='PostgreSQL username'
    )
    parser.add_argument(
        '--password',
        help='PostgreSQL password (or set PGPASSWORD env var)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for loading (default: 1000)'
    )
    parser.add_argument(
        '--create-schema',
        action='store_true',
        help='Create database schema before loading'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show data summary after loading'
    )
    return parser


def handle(args):
    """Execute the load-to-postgres command."""
    # Get password from argument or environment variable
    password = args.password or os.getenv('PGPASSWORD')
    if not password:
        logger.error("PostgreSQL password required. Use --password or set PGPASSWORD env var")
        return 1
    
    # Set up connection parameters
    connection_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': password
    }
    
    # Initialize loader
    loader = CircleCIPostgresLoader(connection_params)
    
    try:
        # Connect to database
        if not loader.connect():
            return 1
        
        # Create schema if requested
        if args.create_schema:
            if not loader.create_schema():
                return 1
        
        # Load data
        if not loader.load_csv_data(args.csv_file, args.batch_size):
            return 1
        
        # Show summary if requested
        if args.summary:
            summary = loader.get_data_summary()
            if summary:
                print("\n=== Data Summary ===")
                print(f"Total records: {summary.get('total_records', 0):,}")
                print(f"Organizations: {summary.get('organizations', 0)}")
                print(f"Projects: {summary.get('projects', 0)}")
                print(f"Total credits: {summary.get('total_credits', 0):,.2f}")
                
                if 'date_range' in summary and summary['date_range']:
                    date_range = summary['date_range'][0]
                    print(f"Pipeline date range: {date_range[0]} to {date_range[1]}")
                    print(f"Job date range: {date_range[2]} to {date_range[3]}")
                
                print("\nJob Status Breakdown:")
                for status, count in summary.get('job_status_breakdown', []):
                    print(f"  {status}: {count:,}")
                
                print("\nResource Class Breakdown:")
                for resource_class, count, credits in summary.get('resource_class_breakdown', []):
                    print(f"  {resource_class}: {count:,} jobs, {credits:,.2f} credits")
        
        logger.info("Data loading completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    finally:
        loader.disconnect()


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description='Load CircleCI usage data into PostgreSQL')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--csv-file',
        help='Path to a single CSV file to load'
    )
    group.add_argument(
        '--directory',
        help='Path to a directory of CSV files to load (all *.csv files)'
    )
    parser.add_argument(
        '--host',
        default=os.getenv('PGHOST', 'localhost'),
        help='PostgreSQL host (default: $PGHOST or localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('PGPORT', '5432')),
        help='PostgreSQL port (default: $PGPORT or 5432)'
    )
    parser.add_argument(
        '--database',
        default=os.getenv('PGDATABASE', 'circleci_usage'),
        help='PostgreSQL database name (default: $PGDATABASE or circleci_usage)'
    )
    parser.add_argument(
        '--user',
        default=os.getenv('PGUSER', 'postgres'),
        help='PostgreSQL username (default: $PGUSER or postgres)'
    )
    parser.add_argument(
        '--password',
        help='PostgreSQL password (or set PGPASSWORD env var)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for loading (default: 1000)'
    )
    parser.add_argument(
        '--create-schema',
        action='store_true',
        help='Create database schema before loading'
    )
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate table before loading (clean reload)'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show data summary after loading'
    )
    args = parser.parse_args()

    password = args.password or os.getenv('PGPASSWORD')
    if not password:
        logger.error("PostgreSQL password required. Use --password or set PGPASSWORD env var")
        sys.exit(1)

    connection_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': password,
    }

    loader = CircleCIPostgresLoader(connection_params)
    try:
        if not loader.connect():
            sys.exit(1)

        if args.create_schema:
            if not loader.create_schema():
                sys.exit(1)

        if args.truncate:
            if not loader.truncate():
                sys.exit(1)

        if args.directory:
            if not loader.load_directory(args.directory, args.batch_size):
                sys.exit(1)
        else:
            if not loader.load_csv_data(args.csv_file, args.batch_size):
                sys.exit(1)

        if args.summary:
            summary = loader.get_data_summary()
            if summary:
                print(f"\n=== Data Summary ===")
                print(f"Total records: {summary.get('total_records', 0):,}")
                print(f"Organizations: {summary.get('organizations', 0)}")
                print(f"Projects: {summary.get('projects', 0)}")
                print(f"Total credits: {summary.get('total_credits', 0):,.2f}")
                if 'date_range' in summary and summary['date_range']:
                    dr = summary['date_range'][0]
                    print(f"Pipeline date range: {dr[0]} to {dr[1]}")
                print(f"\nJob Status Breakdown:")
                for status, count in summary.get('job_status_breakdown', []):
                    print(f"  {status}: {count:,}")

        logger.info("Done")
        sys.exit(0)
    finally:
        loader.disconnect()


if __name__ == '__main__':
    main()
