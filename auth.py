from dotenv import load_dotenv
load_dotenv() 

import os
import msal


CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT = os.getenv("AZURE_TENANT_ID")

# Correct scope for Fabric SQL - try both to see which works
#SCOPE = ["https://database.windows.net/.default"]  # Primary scope for SQL
# Alternative scope if the above doesn't work:
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]


# Choose MSAL app type based on presence of client secret
if CLIENT_SECRET and CLIENT_SECRET.strip():
    _app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT}"
    )
else:
    _app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT}"
    )


def get_token():
    """
    Get Azure AD token for Fabric SQL authentication.
    Uses client credentials if client secret is set, otherwise falls back to interactive authentication.
    """
    if CLIENT_SECRET and CLIENT_SECRET.strip():
        print("Getting Azure AD token using client credentials...")
        try:
            result = _app.acquire_token_for_client(scopes=SCOPE)
            if result and "access_token" in result:
                print("✓ Got token using client credentials")
                return result["access_token"]
            else:
                print(f"Client credentials authentication failed: {result}")
                raise RuntimeError(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")
        except Exception as e:
            print(f"Client credentials authentication failed: {e}")
            raise RuntimeError(f"Authentication failed: {e}")
    else:
        print("Getting Azure AD token interactively...")
        # Try silent first
        accounts = _app.get_accounts()
        if accounts:
            print(f"Found {len(accounts)} cached account(s), trying silent auth...")
            result = _app.acquire_token_silent(SCOPE, account=accounts[0])
            if result and "access_token" in result:
                print("✓ Got token silently")
                return result["access_token"]
            else:
                print("Silent auth failed, will try interactive")
        # Fallback to interactive if no cached token
        print("Starting interactive authentication...")
        try:
            result = _app.acquire_token_interactive(scopes=SCOPE)
            if result and "access_token" in result:
                print("✓ Got token interactively")
                return result["access_token"]
            else:
                print(f"Authentication failed: {result}")
                raise RuntimeError(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")
        except Exception as e:
            print(f"Interactive authentication failed: {e}")
            raise RuntimeError(f"Authentication failed: {e}")

def clear_token_cache():
    """Clear cached tokens - useful for troubleshooting."""
    accounts = _app.get_accounts()
    for account in accounts:
        _app.remove_account(account)
    print("Token cache cleared")