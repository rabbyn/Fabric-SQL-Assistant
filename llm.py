import os
from openai import OpenAI
from prompt import build_sql_prompt, build_context_aware_prompt

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Store recent queries for context
recent_queries = []

def generate_sql(question: str, use_context: bool = True) -> str:
    """Generate SQL query with enhanced prompting and context awareness."""
    
    # Choose prompt based on whether to use context
    if use_context and recent_queries:
        prompt = build_context_aware_prompt(question, recent_queries)
    else:
        prompt = build_sql_prompt(question)
    
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an expert SQL generator for a retail analytics database. Generate only valid SQL queries that follow the schema provided. Always use proper JOINs and table relationships."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            temperature=0,
            max_tokens=300,  # Increased for complex queries
        )
        
        sql = resp.choices[0].message.content.strip()
        
        # Clean up the SQL (remove markdown formatting if present)
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()
        
        # Store the query for context
        recent_queries.append((question, sql))
        if len(recent_queries) > 10:  # Keep only last 10 queries
            recent_queries.pop(0)
        
        return sql
        
    except Exception as e:
        print(f"Error generating SQL: {e}")
        # Fallback to basic query if advanced generation fails
        return f"SELECT * FROM Sales LIMIT 5; -- Error: {str(e)}"

def generate_sql_with_validation(question: str) -> tuple:
    """Generate SQL and perform basic validation."""
    sql = generate_sql(question)
    
    # Basic validation checks
    validation_errors = []
    
    # Check for required elements in complex queries
    question_lower = question.lower()
    sql_lower = sql.lower()
    
    if any(word in question_lower for word in ['total', 'sum', 'count', 'average', 'each', 'by']):
        if 'group by' not in sql_lower and 'count' not in sql_lower:
            validation_errors.append("Query might need GROUP BY clause for aggregation")
    
    if 'reseller' in question_lower and 'company' in question_lower:
        if 'resellercompany' not in sql_lower:
            validation_errors.append("Should use ResellerCompany for company-level analysis")
    
    if 'sales' in question_lower and 'join' not in sql_lower and 'from sales' not in sql_lower:
        validation_errors.append("Complex sales queries usually require JOINs")
    
    return sql, validation_errors

def summarize_result(question: str, columns, rows, sql: str = None):
    """Enhanced result summarization with query context."""
    
    # Build a concise table representation
    if not rows:
        return "No results found for your query."
    
    # For large result sets, show sample + summary
    max_rows_to_show = 10
    table_preview = []
    
    # Add headers
    table_preview.append(" | ".join(str(col) for col in columns))
    table_preview.append("-" * len(table_preview[0]))
    
    # Add sample rows
    sample_rows = rows[:max_rows_to_show]
    for row in sample_rows:
        table_preview.append(" | ".join(str(val) if val is not None else "NULL" for val in row))
    
    if len(rows) > max_rows_to_show:
        table_preview.append(f"... and {len(rows) - max_rows_to_show} more rows")
    
    result_str = "\n".join(table_preview)
    
    # Create summary context
    summary_context = f"""
Question: {question}
SQL Query: {sql if sql else 'Not provided'}
Results: {len(rows)} rows, {len(columns)} columns
Columns: {', '.join(columns)}

Data Preview:
{result_str}

Provide a clear, concise summary that directly answers the user's question.
If this is numerical data, highlight key insights and trends.
If this is a list, mention the top results.
Keep the response conversational and informative.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a data analyst providing clear, insightful summaries of query results. Focus on answering the user's specific question with the most important findings."
                },
                {
                    "role": "user",
                    "content": summary_context
                }
            ],
            temperature=0.1,
            max_tokens=200,
        )
        
        return resp.choices[0].message.content.strip()
        
    except Exception as e:
        # Fallback summary
        if len(rows) == 1 and len(columns) == 1:
            return f"The answer is: {rows[0][0]}"
        else:
            return f"Found {len(rows)} results. Top result: {dict(zip(columns, rows[0])) if rows else 'No data'}"

def clear_query_context():
    """Clear the query context - useful for starting fresh conversations."""
    global recent_queries
    recent_queries = []
    print("Query context cleared")

def get_query_suggestions(question: str) -> list:
    """Get related query suggestions based on the current question."""
    suggestions = []
    
    question_lower = question.lower()
    
    if 'sales' in question_lower:
        suggestions.extend([
            "What are the monthly sales trends?",
            "Which products generate the most revenue?",
            "Who are the top performing salespeople?"
        ])
    
    if 'reseller' in question_lower or 'company' in question_lower:
        suggestions.extend([
            "Which reseller companies have the highest sales?",
            "How many resellers does each company have?",
            "What is the average sale per reseller?"
        ])
    
    if 'product' in question_lower:
        suggestions.extend([
            "Which product categories sell the most?",
            "What are the most expensive products?",
            "Which suppliers provide the best-selling products?"
        ])
    
    return suggestions[:3]  # Return top 3 suggestions