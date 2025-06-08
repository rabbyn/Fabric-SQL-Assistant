# Enhanced schema with detailed table information and relationships
DETAILED_SCHEMA = """
DATABASE SCHEMA - CURATED_EDW:

TABLES AND RELATIONSHIPS:

1. CUSTOMERS Table:
   - CustomerID (Primary Key) - Unique customer identifier
   - FullName, Address, City, Country, State, ZipCode - Customer details
   - Age, Generation - Demographics
   - BrandAffinity, InterestAffinity1, InterestAffinity2 - Preferences
   - ResellerID - Links to Resellers table

2. SALES Table (Main fact table):
   - InvoiceID, InvoiceLineID - Invoice identifiers
   - ResellerID - Links to Resellers table
   - SalespersonPersonID - Links to People table
   - StockItemID - Links to Products table
   - InvoiceDate - Sale date
   - Quantity, UnitPrice, TaxRate, TaxAmount, LineProfit, ExtendedPrice
   - [Sales Amount] - Total sales value (use brackets for column names with spaces)

3. RESELLERS Table:
   - ResellerID (Primary Key) - Unique reseller identifier
   - ResellerName - Individual reseller name
   - ResellerCompany - Company name (multiple resellers can belong to same company)
   - PostalCityID - Links to Geography table
   - Address and contact information

4. PRODUCTS Table:
   - StockItemID (Primary Key) - Product identifier
   - StockItemName - Product name
   - SupplierID - Links to Supplier table
   - Size, UnitPrice, RecommendedRetailPrice
   - StockGroupID, StockGroupName - Product categories
   - IsChillerStock, TaxRate, TypicalWeightPerUnit

5. GEOGRAPHY Table:
   - CityID (Primary Key) - City identifier
   - CityName, StateProvinceCode, StateProvinceName, CountryName
   - SalesTerritory, Region, Continent
   - LatestRecordedPopulation

6. PEOPLE Table:
   - PersonID (Primary Key) - Person identifier
   - FullName, PreferredName, EmailAddress
   - IsSalesperson - Boolean flag

7. DATE Table:
   - Date (Primary Key) - Date value
   - Year, Month, Quarter, MonthName, MonthNameShort
   - StartOfMonth

8. SUPPLIER Table:
   - SupplierID (Primary Key) - Supplier identifier
   - SupplierName, SupplierCategoryName
   - WebsiteURL, SupplierType

9. PURCHASEORDER Table:
   - PurchaseOrderID, PurchaseOrderLineID
   - OrderDate, ExpectedDeliveryDate
   - ContactPersonID, StockItemID
   - OrderedOuters, ExpectedUnitPricePerOuter

10. PRODUCTDETAILS Table:
    - StockItemID - Links to Products
    - CountryOfManufacture, Tag

KEY RELATIONSHIPS:
- Sales.ResellerID → Resellers.ResellerID
- Sales.SalespersonPersonID → People.PersonID  
- Sales.StockItemID → Products.StockItemID
- Products.SupplierID → Supplier.SupplierID
- Resellers.PostalCityID → Geography.CityID
- Customers.ResellerID → Resellers.ResellerID

IMPORTANT NOTES:
- Use [Sales Amount] for the total sales value column (brackets required)
- ResellerCompany groups multiple resellers (use for company-level aggregations)
- InvoiceDate in Sales table for time-based analysis
- Join Date table using Sales.InvoiceDate = Date.Date for time dimensions
"""

ADVANCED_EXAMPLES = """
EXAMPLE QUERIES:

Q: What is the total sales for each reseller company?
A: SELECT r.ResellerCompany, SUM(s.[Sales Amount]) as TotalSales
   FROM Sales s
   JOIN Resellers r ON s.ResellerID = r.ResellerID
   GROUP BY r.ResellerCompany
   ORDER BY TotalSales DESC;

Q: Which products generate the most revenue?
A: SELECT p.StockItemName, SUM(s.[Sales Amount]) as Revenue
   FROM Sales s
   JOIN Products p ON s.StockItemID = p.StockItemID
   GROUP BY p.StockItemName
   ORDER BY Revenue DESC;

Q: What are the monthly sales trends?
A: SELECT d.Year, d.MonthName, SUM(s.[Sales Amount]) as MonthlySales
   FROM Sales s
   JOIN Date d ON s.InvoiceDate = d.Date
   GROUP BY d.Year, d.Month, d.MonthName
   ORDER BY d.Year, d.Month;

Q: Which sales territories perform best?
A: SELECT g.SalesTerritory, SUM(s.[Sales Amount]) as TotalSales
   FROM Sales s
   JOIN Resellers r ON s.ResellerID = r.ResellerID
   JOIN Geography g ON r.PostalCityID = g.CityID
   GROUP BY g.SalesTerritory
   ORDER BY TotalSales DESC;

Q: Top performing salespeople?
A: SELECT p.FullName, SUM(s.[Sales Amount]) as TotalSales
   FROM Sales s
   JOIN People p ON s.SalespersonPersonID = p.PersonID
   WHERE p.IsSalesperson = 1
   GROUP BY p.FullName
   ORDER BY TotalSales DESC;
"""

def build_sql_prompt(user_question: str) -> str:
    return f"""
You are an expert SQL generator for a retail analytics database.

{DETAILED_SCHEMA}

{ADVANCED_EXAMPLES}

QUERY GENERATION RULES:
1. Always use proper table joins based on the relationships shown above
2. Use [Sales Amount] (with brackets) for sales value calculations
3. Include appropriate GROUP BY clauses for aggregations
4. Add ORDER BY clauses to sort results meaningfully
5. Use descriptive column aliases (e.g., TotalSales, Revenue)
6. Consider using date functions for time-based queries
7. Remember that multiple resellers can belong to the same ResellerCompany

Now generate a SQL query for this question:
"{user_question}"

Return only the SQL query, nothing else.
"""

def build_context_aware_prompt(user_question: str, recent_queries: list = None) -> str:
    """Build a prompt that considers recent query context."""
    context = ""
    if recent_queries:
        context = f"\nRECENT QUERIES FOR CONTEXT:\n"
        for i, (q, sql) in enumerate(recent_queries[-3:], 1):  # Last 3 queries
            context += f"{i}. Q: {q}\n   SQL: {sql}\n"
    
    return f"""
You are an expert SQL generator for a retail analytics database.

{DETAILED_SCHEMA}

{ADVANCED_EXAMPLES}
{context}

QUERY GENERATION RULES:
1. Always use proper table joins based on the relationships shown above
2. Use [Sales Amount] (with brackets) for sales value calculations
3. Include appropriate GROUP BY clauses for aggregations
4. Add ORDER BY clauses to sort results meaningfully
5. Use descriptive column aliases (e.g., TotalSales, Revenue)
6. Consider using date functions for time-based queries
7. Remember that multiple resellers can belong to the same ResellerCompany
8. If user asks follow-up questions, build upon previous context

Current question: "{user_question}"

Return only the SQL query, nothing else.
"""