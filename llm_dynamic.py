import os
from openai import OpenAI
from typing import Dict, Any

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def build_dynamic_schema_prompt(schema_cache: Dict[str, Any]) -> str:
    """Build schema description from auto-discovered schema."""
    
    prompt_parts = ["DATABASE SCHEMA:", ""]
    
    # Add table information
    for full_name, table_info in schema_cache["tables"].items():
        schema_name = table_info["schema"]
        table_name = table_info["table_name"]
        
        prompt_parts.append(f"TABLE: {full_name}")
        prompt_parts.append("COLUMNS:")
        
        for col in table_info["columns"]:
            col_desc = f"  - {col['name']} ({col['data_type']}"
            
            # Add type details
            if col.get("max_length"):
                col_desc += f", max_length={col['max_length']}"
            elif col.get("precision"):
                col_desc += f", precision={col['precision']}"
                if col.get("scale"):
                    col_desc += f", scale={col['scale']}"
            
            col_desc += ")"
            
            # Add constraints
            if col["key_type"] == "PK":
                col_desc += " [PRIMARY KEY]"
            elif col["key_type"] == "FK":
                col_desc += " [FOREIGN KEY]"
            
            if not col["is_nullable"]:
                col_desc += " [NOT NULL]"
                
            prompt_parts.append(col_desc)
        
        prompt_parts.append("")
    
    # Add relationships
    if schema_cache["relationships"]:
        prompt_parts.append("RELATIONSHIPS:")
        for rel in schema_cache["relationships"]:
            prompt_parts.append(f"  - {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        prompt_parts.append("")
    
    return "\n".join(prompt_parts)

def generate_sql_with_dynamic_schema(question: str, schema_cache: Dict[str, Any]) -> str:
    """Generate SQL using dynamically discovered schema."""
    
    # Build schema description
    schema_description = build_dynamic_schema_prompt(schema_cache)
    
    # Analyze question to determine relevant tables
    relevant_tables = analyze_question_for_tables(question, schema_cache)
    
    prompt = f"""You are an expert SQL generator. Generate SQL queries based on the discovered database schema.

{schema_description}

IMPORTANT RULES:
1. Use the exact table and column names from the schema above
2. Always use proper table prefixes (schema.table_name)
3. Use appropriate JOINs based on the relationships shown
4. Include proper GROUP BY clauses for aggregations
5. Add meaningful column aliases
6. Consider data types when writing conditions
7. Use TOP or LIMIT for potentially large result sets

RELEVANT TABLES FOR THIS QUERY: {', '.join(relevant_tables)}

Question: "{question}"

Generate only the SQL query, nothing else.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert SQL generator. Generate clean, efficient SQL queries based on the provided schema. Return only the SQL query without any explanation or markdown formatting."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0,
            max_tokens=500,
        )
        
        sql = resp.choices[0].message.content.strip()
        
        # Clean up the SQL
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()
        
        return sql
        
    except Exception as e:
        print(f"Error generating SQL: {e}")
        raise

def analyze_question_for_tables(question: str, schema_cache: Dict[str, Any]) -> list:
    """Analyze question to determine which tables might be relevant."""
    
    question_lower = question.lower()
    relevant_tables = []
    
    # Check each table name against the question
    for full_name, table_info in schema_cache["tables"].items():
        table_name = table_info["table_name"].lower()
        
        # Check if table name or common variations appear in question
        if table_name in question_lower:
            relevant_tables.append(full_name)
            continue
            
        # Check for singular/plural variations
        if table_name.endswith('s') and table_name[:-1] in question_lower:
            relevant_tables.append(full_name)
            continue
        
        if table_name + 's' in question_lower:
            relevant_tables.append(full_name)
            continue
            
        # Check column names
        for col in table_info["columns"]:
            col_name = col["name"].lower()
            if col_name in question_lower and len(col_name) > 3:  # Avoid short matches
                relevant_tables.append(full_name)
                break
    
    # If no tables found, include main fact tables
    if not relevant_tables:
        for full_name, table_info in schema_cache["tables"].items():
            table_name = table_info["table_name"].lower()
            # Common fact table names
            if any(fact in table_name for fact in ['sales', 'order', 'transaction', 'fact']):
                relevant_tables.append(full_name)
    
    return list(set(relevant_tables))  # Remove duplicates

def validate_sql_with_schema(sql: str, schema_cache: Dict[str, Any]) -> list:
    """Validate SQL against discovered schema."""
    
    warnings = []
    sql_lower = sql.lower()
    
    # Extract table names from SQL
    # This is a simple check - could be enhanced with proper SQL parsing
    all_tables = set()
    for full_name in schema_cache["tables"]:
        all_tables.add(full_name.lower())
        # Also add just the table name without schema
        parts = full_name.split(".")
        if len(parts) > 1:
            all_tables.add(parts[-1].lower())
    
    # Check if SQL references non-existent tables
    words = sql_lower.split()
    for i, word in enumerate(words):
        if i > 0 and words[i-1] in ['from', 'join']:
            # Clean the table reference
            table_ref = word.strip('(),')
            if table_ref and table_ref not in all_tables and '.' not in table_ref:
                warnings.append(f"Table '{table_ref}' not found in schema")
    
    # Check for common issues
    if 'group by' not in sql_lower and any(agg in sql_lower for agg in ['sum(', 'count(', 'avg(', 'max(', 'min(']):
        warnings.append("Query contains aggregation but might be missing GROUP BY clause")
    
    return warnings