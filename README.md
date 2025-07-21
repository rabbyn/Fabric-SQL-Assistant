# Fabric SQL MCP Server Setup Guide for Claude Desktop

## 🎯 Overview
This guide will help you set up a Model Context Protocol (MCP) server that allows Claude Desktop to interact with Microsoft Fabric Data Warehouse and SQL databases using natural language.

**✨ New in this version:**
- **Fabric Data Warehouse Support**: Optimized schema discovery for Fabric SQL endpoints
- **Enhanced Compatibility**: Works with various Fabric configurations and constraint limitations
- **Robust Fallbacks**: Graceful handling when advanced metadata isn't available
- **Improved Error Handling**: Better diagnostics for connection and schema issues

## 📋 Prerequisites

### Required Software
- **Python 3.8+** installed on your system
- **Claude Desktop** application
- **Microsoft Fabric** workspace with SQL endpoint
- **Azure Active Directory** app registration
- **OpenAI API** account

### Required Credentials
- Azure Client ID and Tenant ID
- OpenAI API key
- Fabric SQL server address and database name

## 🚀 Step-by-Step Setup

### Step 1: Download the Project Files
1. Clone or download the repository
2. Ensure you have these Python files in your project directory:
   - `mcp_server.py`
   - `db.py`
   - `auth.py`
   - `llm.py`
   - `llm_dynamic.py`
   - `prompt.py`

### Step 2: Set Up Python Environment
```bash
# Navigate to your project directory
cd /path/to/your/fabric-sql-mcp

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install required packages
pip install mcp pyodbc msal python-dotenv openai
```

### Step 3: Create Environment File
Create a `.env` file in your project directory:

```bash
# Azure AD - Get these from Azure Portal > App Registrations
AZURE_CLIENT_ID=your_azure_client_id_here
AZURE_TENANT_ID=your_azure_tenant_id_here

# OpenAI - Get from https://platform.openai.com/api-keys
OPENAI_API_KEY=your_openai_api_key_here

# Fabric SQL - Get from your Fabric workspace
FABRIC_SQL_SERVER=your-server.datawarehouse.fabric.microsoft.com
FABRIC_DATABASE=your_database_name_here
```

### Step 4: Get Your Azure Credentials

#### Create Azure App Registration:
1. Go to **Azure Portal** > **Azure Active Directory** > **App Registrations**
2. Click **New Registration**
3. Name: "Fabric SQL MCP"
4. Account types: "Accounts in this organizational directory only"
5. Redirect URI: Leave blank for now
6. Click **Register**

#### Configure Authentication:
1. In your new app, go to **Authentication**
2. Click **Add a platform** > **Mobile and desktop applications**
3. Check **https://login.microsoftonline.com/common/oauth2/nativeclient**
4. Click **Configure**

#### Grant Permissions:
1. Go to **API Permissions**
2. Click **Add a permission**
3. Select **Azure Service Management**
4. Check **user_impersonation**
5. Click **Grant admin consent**

#### Get Your IDs:
- **Application (client) ID** - Copy this as your `AZURE_CLIENT_ID`
- **Directory (tenant) ID** - Copy this as your `AZURE_TENANT_ID`

### Step 5: Get Fabric SQL Details
1. Go to your **Microsoft Fabric workspace**
2. Navigate to your **SQL Database** or **Data Warehouse**
3. Click on **Settings** or **Connection strings**
4. Copy the **SQL connection endpoint** (looks like: `your-server.datawarehouse.fabric.microsoft.com`)
5. Note your **database name**

### Step 6: Configure Claude Desktop

#### Find Claude Desktop Config:
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

#### Create/Edit Configuration:
```json
{
  "mcpServers": {
    "fabric-sql-assistant": {
      "command": "C:/path/to/your/project/.venv/Scripts/python.exe",
      "args": [
        "C:/path/to/your/project/mcp_server.py"
      ],
      "env": {
        "FABRIC_SQL_SERVER": "your-server.datawarehouse.fabric.microsoft.com",
        "FABRIC_DATABASE": "your_database_name",
        "AZURE_CLIENT_ID": "your_azure_client_id",
        "AZURE_TENANT_ID": "your_azure_tenant_id",
        "OPENAI_API_KEY": "your_openai_api_key"
      }
    }
  }
}
```

**Important Notes:**
- Use **full absolute paths** for both `command` and `args`
- On Windows, use forward slashes `/` or escape backslashes `\\`
- On macOS/Linux, the command would be `/path/to/your/project/.venv/bin/python`

### Step 7: Test the Setup

#### Test MCP Server Standalone:
```bash
# Activate your virtual environment
.venv\Scripts\activate  # Windows
# or
source .venv/bin/activate  # macOS/Linux

# Run the server
python mcp_server.py
```

You should see the server start without errors.

#### Test in Claude Desktop:
1. **Restart Claude Desktop** completely
2. Open a new conversation
3. Look for the 🔧 (tools) icon in the interface
4. Try asking: "What tables are available in my database?"

## 🔧 Available Commands in Claude

Once set up, you can use these commands:

### Database Configuration
```
Configure my database connection:
Server: your-server.datawarehouse.fabric.microsoft.com
Database: your_database_name
```

### Schema Discovery
```
Discover my database schema
```

### Natural Language Queries
```
What are the total sales by product category?
Which customers have made the most purchases?
Show me monthly revenue trends for this year
```

### Direct SQL Execution
```
Execute this SQL query: SELECT TOP 10 * FROM Sales
```

### Table Inspection
```
Show me details about the Sales table
```

## 🐛 Troubleshooting

### Common Issues

#### 1. "Tool not found" in Claude Desktop
- **Solution:** Restart Claude Desktop completely after config changes
- Check that file paths in config are absolute and correct
- Verify Python virtual environment path

#### 2. Authentication Failures
- **Solution:** Ensure Azure app has correct permissions
- Check that you've granted admin consent for API permissions
- Verify Client ID and Tenant ID are correct

#### 3. Database Connection Issues
- **Solution:** Test connection to Fabric SQL from another tool first
- Ensure your account has access to the Fabric workspace
- Check that the SQL endpoint is active

#### 4. Python Import Errors
- **Solution:** Ensure all required packages are installed in the virtual environment
- Check that all Python files are in the same directory

#### 5. OpenAI API Errors
- **Solution:** Verify your OpenAI API key is valid and has credits
- Check that the key has access to the GPT-4 models

### Debug Steps

#### 1. Test Python Environment:
```bash
.venv\Scripts\activate
python -c "import mcp, pyodbc, msal, openai; print('All imports successful')"
```

#### 2. Test Database Connection:
```bash
python -c "from db import test_connection; print(test_connection())"
```

#### 3. Check Claude Desktop Logs:
- **Windows:** `%APPDATA%\Claude\logs\`
- **macOS:** `~/Library/Logs/Claude/`

## 🔒 Security Best Practices

1. **Never commit** `.env` or `claude_desktop_config.json` to version control
2. **Use principle of least privilege** for Azure app permissions
3. **Rotate credentials** regularly
4. **Monitor usage** of your OpenAI API key
5. **Use environment variables** rather than hardcoding secrets

## 🆘 Getting Help

If you encounter issues:

1. **Check Claude Desktop logs** for error messages
2. **Test each component separately** (auth, database, MCP server)
3. **Verify all credentials** are correct and active
4. **Ensure all dependencies** are installed in the virtual environment

## 📚 Additional Resources

- [Microsoft Fabric Documentation](https://docs.microsoft.com/en-us/fabric/)
- [Azure Active Directory App Registration](https://docs.microsoft.com/en-us/azure/active-directory/develop/)
- [OpenAI API Documentation](https://platform.openai.com/docs)
- [Model Context Protocol Specification](https://modelcontextprotocol.io/)

---

**Happy querying! 🎉**
