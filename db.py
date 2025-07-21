import os
import struct
import pyodbc
import time
from auth import get_token

def _find_driver():
    """Pick the latest installed ODBC Driver for SQL Server."""
    drivers = [d for d in pyodbc.drivers()]
    print(f"Available ODBC drivers: {drivers}")
    
    # Prefer ODBC Driver 18, then 17, then any SQL Server driver
    for version in ("18", "17"):
        for name in drivers:
            if f"ODBC Driver {version} for SQL Server" in name:
                print(f"Selected driver: {name}")
                return name
    
    for name in drivers:
        if "SQL Server" in name:
            print(f"Selected driver: {name}")
            return name
            
    raise RuntimeError(
        f"No suitable ODBC Driver for SQL Server found. Installed drivers: {drivers}"
    )

def get_connection():
    """Get database connection to Fabric SQL endpoint."""
    
    # Read environment variables at runtime (not at import)
    SERVER = os.getenv("FABRIC_SQL_SERVER")
    DATABASE = os.getenv("FABRIC_DATABASE")
    
    if not SERVER or not DATABASE:
        raise RuntimeError(
            f"Missing connection parameters:\n"
            f"FABRIC_SQL_SERVER: {SERVER}\n"
            f"FABRIC_DATABASE: {DATABASE}\n"
            f"Please configure these in the GUI or environment variables."
        )
    
    print(f"Connecting to Fabric SQL...")
    print(f"Server: {SERVER}")
    print(f"Database: {DATABASE}")
    
    # Get fresh token
    token = get_token()
    
    # Fabric SQL requires specific token encoding (different from regular Azure SQL)
    from itertools import chain, repeat
    token_as_bytes = bytes(token, "UTF-8")  # Convert token to UTF-8 bytes
    encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))  # Encode for Windows
    token_struct = struct.pack("<i", len(encoded_bytes)) + encoded_bytes  # Package the token
    
    # Get driver
    driver = _find_driver()
    
    # Try different connection string configurations for Fabric
    configs = [
        {
            "name": "Fabric SQL Optimized",
            "conn_str": (
                f"Driver={{{driver}}};"
                f"Server={SERVER},1433;"
                f"Database={DATABASE};"
                "Encrypt=Yes;"
                "TrustServerCertificate=No;"
            ),
            "use_token": True
        },
        {
            "name": "Fabric SQL with TCP Prefix", 
            "conn_str": (
                f"Driver={{{driver}}};"
                f"Server=tcp:{SERVER},1433;"
                f"Database={DATABASE};"
                "Encrypt=Yes;"
                "TrustServerCertificate=Yes;"
            ),
            "use_token": True
        },
        {
            "name": "Fabric SQL Simple",
            "conn_str": (
                f"Driver={{{driver}}};"
                f"Server={SERVER};"
                f"Database={DATABASE};"
                "Encrypt=yes;"
            ),
            "use_token": True
        }
    ]
    
    for config in configs:
        try:
            print(f"\nTrying: {config['name']}")
            print(f"Connection string: {config['conn_str']}")
            
            # All Fabric SQL connections use token in attrs_before with the special encoding
            connection = pyodbc.connect(
                config['conn_str'],
                attrs_before={1256: token_struct}  # SQL_COPT_SS_ACCESS_TOKEN = 1256
            )
            
            # Test the connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] == 1:
                print(f"✓ Connection successful with: {config['name']}")
                return connection
            else:
                print(f"✗ Connection test failed with: {config['name']}")
                connection.close()
                
        except pyodbc.Error as e:
            print(f"Connection method failed: {config['name']} - {str(e)}")
            continue
        except Exception as e:
            print(f"Unexpected error with {config['name']}: {str(e)}")
            continue
    
    # If all methods fail, provide detailed error
    raise RuntimeError(
        f"Failed to connect to Fabric SQL endpoint.\n"
        f"Server: {SERVER}\n"
        f"Database: {DATABASE}\n"
        f"Please verify:\n"
        f"1. The Fabric SQL endpoint is active and accessible\n"
        f"2. Your Azure AD account has proper permissions\n"
        f"3. The database name is correct\n"
        f"4. Network connectivity to the endpoint\n"
        f"5. Try accessing the endpoint through Fabric portal first"
    )

def run_query(sql: str):
    """Execute a SQL query and return column names and rows."""
    
    connection = None
    try:
        print(f"\nExecuting query: {sql}")
        
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        
        # Handle queries that don't return results
        if cursor.description is None:
            rows_affected = cursor.rowcount
            connection.commit()
            connection.close()
            print(f"✓ Query executed. {rows_affected} rows affected.")
            return ["rows_affected"], [(rows_affected,)]
        
        # Handle SELECT queries
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        connection.close()
        
        print(f"✓ Query completed. Returned {len(rows)} rows, {len(columns)} columns.")
        return columns, rows
        
    except Exception as e:
        if connection:
            try:
                connection.close()
            except:
                pass
        
        error_msg = f"Query execution failed:\nSQL: {sql}\nError: {str(e)}"
        print(f"✗ {error_msg}")
        raise RuntimeError(error_msg) from e

def test_connection():
    """Test the database connection and return status."""
    try:
        print("\n=== Testing Connection ===")
        
        # Show current environment variables for debugging
        SERVER = os.getenv("FABRIC_SQL_SERVER")
        DATABASE = os.getenv("FABRIC_DATABASE")
        print(f"Current SERVER from env: {SERVER}")
        print(f"Current DATABASE from env: {DATABASE}")
        
        columns, rows = run_query("SELECT @@VERSION as db_version, GETDATE() as current_datetime")
        return {
            "status": "success",
            "version": rows[0][0] if rows else "Unknown",
            "current_time": rows[0][1] if rows else "Unknown",
            "server": SERVER,
            "database": DATABASE
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "server": os.getenv("FABRIC_SQL_SERVER"),
            "database": os.getenv("FABRIC_DATABASE")
        }

def get_table_schema():
    """Get schema information for all tables to help with query generation."""
    try:
        # Try Fabric-compatible schema query first
        schema_query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.ORDINAL_POSITION,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        AND t.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        columns, rows = run_query(schema_query)
        
        # Organize schema by table
        schema = {}
        for row in rows:
            schema_name = row[0]
            table_name = row[1]
            full_table_name = f"{schema_name}.{table_name}"
            
            if full_table_name not in schema:
                schema[full_table_name] = []
            
            column_info = {
                'column_name': row[2],
                'data_type': row[3],
                'is_nullable': row[4],
                'ordinal_position': row[5],
                'max_length': row[6],
                'numeric_precision': row[7],
                'numeric_scale': row[8],
                'column_default': row[9]
            }
            
            schema[full_table_name].append(column_info)
        
        # Try to get primary key information separately (Fabric Data Warehouse compatible)
        try:
            pk_query = """
            SELECT 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA 
                AND tc.TABLE_NAME = ku.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """
            
            pk_columns, pk_rows = run_query(pk_query)
            
            # Mark primary key columns
            pk_info = {}
            for pk_row in pk_rows:
                pk_schema = pk_row[0]
                pk_table = pk_row[1]
                pk_column = pk_row[2]
                pk_full_name = f"{pk_schema}.{pk_table}"
                
                if pk_full_name not in pk_info:
                    pk_info[pk_full_name] = set()
                pk_info[pk_full_name].add(pk_column)
            
            # Update schema with primary key information
            for table_name, columns in schema.items():
                table_pks = pk_info.get(table_name, set())
                for col in columns:
                    col['is_primary_key'] = col['column_name'] in table_pks
                    
        except Exception as pk_e:
            print(f"Could not retrieve primary key information: {pk_e}")
            # Set all primary key flags to False if we can't get the info
            for table_name, columns in schema.items():
                for col in columns:
                    col['is_primary_key'] = False
        
        return schema
        
    except Exception as e:
        print(f"Could not retrieve schema using Fabric query: {e}")
        
        # Fallback to basic schema query without constraints
        try:
            basic_query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
            
            columns, rows = run_query(basic_query)
            
            # Organize basic schema
            schema = {}
            for row in rows:
                schema_name = row[0]
                table_name = row[1]
                full_table_name = f"{schema_name}.{table_name}"
                
                if full_table_name not in schema:
                    schema[full_table_name] = []
                
                schema[full_table_name].append({
                    'column_name': row[2],
                    'data_type': row[3],
                    'is_nullable': row[4],
                    'is_primary_key': False,  # Can't determine without constraints
                    'ordinal_position': None,
                    'max_length': None,
                    'numeric_precision': None,
                    'numeric_scale': None,
                    'column_default': None
                })
            
            print("Using basic schema discovery (no constraint information available)")
            return schema
            
        except Exception as basic_e:
            print(f"Basic schema query also failed: {basic_e}")
            return None

# Utility function to check environment
def check_environment():
    """Check if all required environment variables are set."""
    required_vars = ["FABRIC_SQL_SERVER", "FABRIC_DATABASE", "AZURE_CLIENT_ID", "AZURE_TENANT_ID"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise RuntimeError(f"Missing required environment variables: {missing_vars}")
    
    print("✓ All required environment variables are set")
    print(f"Server: {os.getenv('FABRIC_SQL_SERVER')}")
    print(f"Database: {os.getenv('FABRIC_DATABASE')}")
    print(f"Client ID: {os.getenv('AZURE_CLIENT_ID')}")
    print(f"Tenant ID: {os.getenv('AZURE_TENANT_ID')}")

def get_current_connection_info():
    """Get current connection information for debugging."""
    return {
        "server": os.getenv("FABRIC_SQL_SERVER"),
        "database": os.getenv("FABRIC_DATABASE"),
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "tenant_id": os.getenv("AZURE_TENANT_ID")
    }