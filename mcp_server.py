#!/usr/bin/env python3
"""
MCP Server for Fabric SQL Assistant with Dynamic Configuration
"""

import sys
import os

# Force UTF-8 encoding for Windows compatibility
if os.name == 'nt':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='strict', line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='strict', line_buffering=True)

import asyncio
import json
from typing import Any, Sequence, Dict, Optional
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

# Create the MCP server
server = Server("fabric-sql-assistant")

# Store dynamic configuration
current_config: Dict[str, Any] = {
    "server": None,
    "database": None,
    "schema_cache": None,
    "last_schema_update": None
}

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools for the MCP client."""
    return [
        types.Tool(
            name="configure_database",
            description="Configure the Fabric SQL database connection",
            inputSchema={
                "type": "object",
                "properties": {
                    "server": {
                        "type": "string",
                        "description": "Fabric SQL server address (e.g., your-server.datawarehouse.fabric.microsoft.com)"
                    },
                    "database": {
                        "type": "string", 
                        "description": "Database name to connect to"
                    }
                },
                "required": ["server", "database"]
            }
        ),
        types.Tool(
            name="discover_schema",
            description="Automatically discover database schema including all tables and columns",
            inputSchema={
                "type": "object",
                "properties": {
                    "refresh": {
                        "type": "boolean",
                        "description": "Force refresh of cached schema (default: false)",
                        "default": False
                    }
                },
                "required": []
            }
        ),
        types.Tool(
            name="ask_database",
            description="Ask natural language questions about the Fabric SQL database",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about the data"
                    },
                    "include_raw_data": {
                        "type": "boolean",
                        "description": "Whether to include raw data results (default: false)",
                        "default": False
                    },
                    "use_auto_schema": {
                        "type": "boolean",
                        "description": "Use automatically discovered schema (default: true)",
                        "default": True
                    }
                },
                "required": ["question"]
            }
        ),
        types.Tool(
            name="get_current_config",
            description="Get current database configuration",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        types.Tool(
            name="execute_sql_query",
            description="Execute a specific SQL query directly",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "limit_rows": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 100)",
                        "default": 100
                    }
                },
                "required": ["sql"]
            }
        ),
        types.Tool(
            name="get_table_details",
            description="Get detailed information about a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to inspect"
                    },
                    "include_sample_data": {
                        "type": "boolean",
                        "description": "Include sample data from the table (default: true)",
                        "default": True
                    }
                },
                "required": ["table_name"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    """Handle tool calls from the MCP client."""
    if arguments is None:
        arguments = {}

    try:
        if name == "configure_database":
            return await handle_configure_database(arguments)
        elif name == "discover_schema":
            return await handle_discover_schema(arguments)
        elif name == "ask_database":
            return await handle_ask_database(arguments)
        elif name == "get_current_config":
            return await handle_get_current_config(arguments)
        elif name == "execute_sql_query":
            return await handle_execute_sql(arguments)
        elif name == "get_table_details":
            return await handle_table_details(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

    except Exception as e:
        error_msg = f"Error in {name}: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]

async def handle_configure_database(arguments: dict) -> list[types.TextContent]:
    """Configure database connection dynamically."""
    server = arguments.get("server", "")
    database = arguments.get("database", "")
    
    if not server or not database:
        return [types.TextContent(type="text", text="❌ Both server and database parameters are required.")]
    
    # Update environment variables
    os.environ["FABRIC_SQL_SERVER"] = server
    os.environ["FABRIC_DATABASE"] = database
    
    # Update configuration
    current_config["server"] = server
    current_config["database"] = database
    current_config["schema_cache"] = None  # Reset schema cache
    
    # Test connection
    try:
        from db import test_connection
        result = test_connection()
        
        if result["status"] == "success":
            response = f"""✅ **Database Configured Successfully!**

**Server:** {server}
**Database:** {database}
**Status:** Connected
**Version:** {result.get('version', 'Unknown')}

You can now:
1. Run `discover_schema` to automatically discover all tables and columns
2. Ask questions about your data using `ask_database`
3. Execute SQL queries directly using `execute_sql_query`"""
        else:
            response = f"""❌ **Database Configuration Failed**

**Server:** {server}
**Database:** {database}
**Error:** {result.get('error', 'Unknown error')}

Please check:
- Server address is correct
- Database name exists
- You have proper permissions
- Network connectivity"""
            
        return [types.TextContent(type="text", text=response)]
        
    except Exception as e:
        return [types.TextContent(type="text", text=f"❌ Configuration failed: {str(e)}")]

async def handle_discover_schema(arguments: dict) -> list[types.TextContent]:
    """Automatically discover database schema."""
    refresh = arguments.get("refresh", False)
    
    if not current_config["server"] or not current_config["database"]:
        return [types.TextContent(type="text", text="❌ Please configure database first using `configure_database` tool.")]
    
    # Check cache
    if current_config["schema_cache"] and not refresh:
        return [types.TextContent(type="text", text=format_schema_response(current_config["schema_cache"]))]
    
    try:
        from db import run_query
        
        # Query for all tables and columns with more details
        schema_query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_TYPE,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            CASE 
                WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PK'
                WHEN fk.COLUMN_NAME IS NOT NULL THEN 'FK'
                ELSE ''
            END as KEY_TYPE,
            fk.REFERENCED_TABLE_NAME,
            fk.REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        LEFT JOIN (
            SELECT 
                ku.TABLE_SCHEMA,
                ku.TABLE_NAME,
                ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
        LEFT JOIN (
            SELECT 
                ku.TABLE_SCHEMA,
                ku.TABLE_NAME,
                ku.COLUMN_NAME,
                ku.REFERENCED_TABLE_NAME,
                ku.REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        ) fk ON c.TABLE_SCHEMA = fk.TABLE_SCHEMA AND c.TABLE_NAME = fk.TABLE_NAME AND c.COLUMN_NAME = fk.COLUMN_NAME
        WHERE t.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        cols, rows = run_query(schema_query)
        
        # Organize schema data
        schema_data = {}
        relationships = []
        
        for row in rows:
            schema_name = row[0]
            table_name = row[1]
            table_type = row[2]
            column_name = row[3]
            data_type = row[4]
            max_length = row[5]
            precision = row[6]
            scale = row[7]
            is_nullable = row[8]
            default_value = row[9]
            ordinal = row[10]
            key_type = row[11]
            ref_table = row[12]
            ref_column = row[13]
            
            full_table_name = f"{schema_name}.{table_name}"
            
            if full_table_name not in schema_data:
                schema_data[full_table_name] = {
                    "schema": schema_name,
                    "table_name": table_name,
                    "table_type": table_type,
                    "columns": []
                }
            
            # Build column info
            col_info = {
                "name": column_name,
                "data_type": data_type,
                "position": ordinal,
                "is_nullable": is_nullable == "YES",
                "key_type": key_type
            }
            
            # Add type details
            if max_length:
                col_info["max_length"] = max_length
            if precision:
                col_info["precision"] = precision
            if scale:
                col_info["scale"] = scale
            if default_value:
                col_info["default"] = default_value
                
            schema_data[full_table_name]["columns"].append(col_info)
            
            # Track relationships
            if key_type == "FK" and ref_table:
                relationships.append({
                    "from_table": full_table_name,
                    "from_column": column_name,
                    "to_table": f"{schema_name}.{ref_table}",
                    "to_column": ref_column
                })
        
        # Cache the schema
        current_config["schema_cache"] = {
            "tables": schema_data,
            "relationships": relationships,
            "discovered_at": asyncio.get_event_loop().time()
        }
        
        return [types.TextContent(type="text", text=format_schema_response(current_config["schema_cache"]))]
        
    except Exception as e:
        return [types.TextContent(type="text", text=f"❌ Schema discovery failed: {str(e)}")]

async def handle_table_details(arguments: dict) -> list[types.TextContent]:
    """Get detailed information about a specific table."""
    table_name = arguments.get("table_name", "")
    include_sample = arguments.get("include_sample_data", True)
    
    if not table_name:
        return [types.TextContent(type="text", text="❌ Please provide a table name.")]
    
    try:
        from db import run_query
        
        # Get row count
        count_sql = f"SELECT COUNT(*) as row_count FROM {table_name}"
        cols, rows = run_query(count_sql)
        row_count = rows[0][0] if rows else 0
        
        response_parts = [f"## Table: {table_name}", f"**Total Rows:** {row_count:,}"]
        
        # Get column details from schema cache if available
        if current_config["schema_cache"]:
            for full_name, table_info in current_config["schema_cache"]["tables"].items():
                if table_name in full_name:
                    response_parts.append("\n**Columns:**")
                    response_parts.append("| Column | Type | Nullable | Key |")
                    response_parts.append("|--------|------|----------|-----|")
                    for col in table_info["columns"]:
                        nullable = "Yes" if col["is_nullable"] else "No"
                        response_parts.append(f"| {col['name']} | {col['data_type']} | {nullable} | {col['key_type']} |")
                    break
        
        # Get sample data
        if include_sample and row_count > 0:
            sample_sql = f"SELECT TOP 5 * FROM {table_name}"
            cols, rows = run_query(sample_sql)
            
            response_parts.append("\n**Sample Data:**")
            response_parts.append("| " + " | ".join(cols) + " |")
            response_parts.append("|" + "---|" * len(cols))
            for row in rows:
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                response_parts.append("| " + " | ".join(formatted_row) + " |")
        
        return [types.TextContent(type="text", text="\n".join(response_parts))]
        
    except Exception as e:
        return [types.TextContent(type="text", text=f"❌ Error getting table details: {str(e)}")]

async def handle_ask_database(arguments: dict) -> list[types.TextContent]:
    """Handle natural language database questions with auto-discovered schema."""
    question = arguments.get("question", "")
    include_raw_data = arguments.get("include_raw_data", False)
    use_auto_schema = arguments.get("use_auto_schema", True)
    
    if not question:
        return [types.TextContent(type="text", text="❌ Please provide a question.")]
    
    if not current_config["server"] or not current_config["database"]:
        return [types.TextContent(type="text", text="❌ Please configure database first using `configure_database` tool.")]
    
    try:
        # Use auto-discovered schema if available
        if use_auto_schema and current_config["schema_cache"]:
            from llm_dynamic import generate_sql_with_dynamic_schema
            sql = generate_sql_with_dynamic_schema(question, current_config["schema_cache"])
        else:
            from llm import generate_sql
            sql = generate_sql(question)
        
        # Execute query
        from db import run_query
        cols, rows = run_query(sql)
        
        # Generate summary
        from llm import summarize_result
        answer = summarize_result(question, cols, rows, sql)
        
        # Build response
        response_parts = [f"## {answer}", "", "**SQL Query:**", f"```sql\n{sql}\n```", f"**Results:** {len(rows)} rows"]
        
        if include_raw_data and rows:
            response_parts.append("\n**Data:**")
            response_parts.append("| " + " | ".join(cols) + " |")
            response_parts.append("|" + "---|" * len(cols))
            for row in rows[:10]:
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                response_parts.append("| " + " | ".join(formatted_row) + " |")
            if len(rows) > 10:
                response_parts.append(f"... and {len(rows) - 10} more rows")
        
        return [types.TextContent(type="text", text="\n".join(response_parts))]
        
    except Exception as e:
        return [types.TextContent(type="text", text=f"❌ Error: {str(e)}")]

async def handle_get_current_config(arguments: dict) -> list[types.TextContent]:
    """Get current database configuration."""
    if not current_config["server"]:
        return [types.TextContent(type="text", text="❌ No database configured. Use `configure_database` to set up connection.")]
    
    response = f"""**Current Configuration:**

**Server:** {current_config['server']}
**Database:** {current_config['database']}
**Schema Discovered:** {'Yes' if current_config['schema_cache'] else 'No'}
"""
    
    if current_config['schema_cache']:
        table_count = len(current_config['schema_cache']['tables'])
        response += f"**Tables Found:** {table_count}\n"
        response += f"**Relationships Found:** {len(current_config['schema_cache']['relationships'])}"
    
    return [types.TextContent(type="text", text=response)]

async def handle_execute_sql(arguments: dict) -> list[types.TextContent]:
    """Execute SQL query."""
    sql = arguments.get("sql", "")
    limit_rows = arguments.get("limit_rows", 100)
    
    if not sql:
        return [types.TextContent(type="text", text="❌ Please provide a SQL query.")]
    
    if not current_config["server"]:
        return [types.TextContent(type="text", text="❌ Please configure database first using `configure_database` tool.")]
    
    try:
        from db import run_query
        
        # Add limit if needed
        if sql.strip().upper().startswith("SELECT") and "LIMIT" not in sql.upper() and "TOP" not in sql.upper():
            sql = f"SELECT TOP {limit_rows} * FROM ({sql}) AS limited_query"
        
        cols, rows = run_query(sql)
        
        response_parts = [f"**Results:** {len(rows)} rows", ""]
        
        if rows:
            response_parts.append("| " + " | ".join(cols) + " |")
            response_parts.append("|" + "---|" * len(cols))
            for row in rows[:20]:
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                response_parts.append("| " + " | ".join(formatted_row) + " |")
            if len(rows) > 20:
                response_parts.append(f"... and {len(rows) - 20} more rows")
        else:
            response_parts.append("No results returned.")
        
        return [types.TextContent(type="text", text="\n".join(response_parts))]
        
    except Exception as e:
        return [types.TextContent(type="text", text=f"❌ SQL execution failed: {str(e)}")]

def format_schema_response(schema_cache: dict) -> str:
    """Format schema discovery results."""
    tables = schema_cache["tables"]
    relationships = schema_cache["relationships"]
    
    response_parts = [f"## Database Schema Discovery Complete!", f"**Found {len(tables)} tables**", ""]
    
    # Group tables by schema
    schemas = {}
    for full_name, table_info in tables.items():
        schema = table_info["schema"]
        if schema not in schemas:
            schemas[schema] = []
        schemas[schema].append(table_info)
    
    # Display tables by schema
    for schema, tables_in_schema in schemas.items():
        response_parts.append(f"### Schema: {schema}")
        response_parts.append("")
        
        for table in sorted(tables_in_schema, key=lambda x: x["table_name"]):
            pk_cols = [col["name"] for col in table["columns"] if col["key_type"] == "PK"]
            fk_cols = [col["name"] for col in table["columns"] if col["key_type"] == "FK"]
            
            response_parts.append(f"**{table['table_name']}** ({len(table['columns'])} columns)")
            if pk_cols:
                response_parts.append(f"  - Primary Key: {', '.join(pk_cols)}")
            if fk_cols:
                response_parts.append(f"  - Foreign Keys: {', '.join(fk_cols)}")
            response_parts.append("")
    
    # Display relationships
    if relationships:
        response_parts.append("### Relationships")
        response_parts.append("")
        for rel in relationships[:10]:  # Show first 10
            response_parts.append(f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        if len(relationships) > 10:
            response_parts.append(f"... and {len(relationships) - 10} more relationships")
    
    response_parts.append("\n**Next Steps:**")
    response_parts.append("1. Use `ask_database` to query your data with natural language")
    response_parts.append("2. Use `get_table_details` to inspect specific tables")
    response_parts.append("3. The AI will automatically use this schema for better SQL generation")
    
    return "\n".join(response_parts)

async def main():
    """Main function to run the MCP server."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="fabric-sql-assistant",
                server_version="2.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())