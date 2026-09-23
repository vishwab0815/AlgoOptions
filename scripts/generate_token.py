#!/usr/bin/env python3
"""
Helper script to generate DhanHQ access token.
Choose between OAuth flow or PIN+TOTP flow.
"""

import sys

def oauth_flow():
    """Generate access token using OAuth flow."""
    print("\n" + "=" * 60)
    print("🔐 DhanHQ OAuth Flow - Access Token Generator")
    print("=" * 60)
    
    try:
        from dhanhq import DhanLogin
    except ImportError:
        print("❌ ERROR: dhanhq package not installed")
        print("   Run: pip install dhanhq")
        return
    
    # Get credentials
    print("\n📝 Please provide the following details:")
    client_id = input("Client ID: ").strip()
    app_id = input("APP ID: ").strip()
    app_secret = input("APP SECRET: ").strip()
    
    if not client_id or not app_id or not app_secret:
        print("❌ All fields are required!")
        return
    
    try:
        print("\n🔄 Generating login session...")
        dhan_login = DhanLogin(client_id)
        
        # Generate consent and open browser
        consent_id = dhan_login.generate_login_session(app_id, app_secret)
        
        print(f"✅ Consent ID generated: {consent_id}")
        print("\n📱 Your browser should open automatically for login.")
        print("   After login, you'll be redirected to a URL with a TOKEN_ID parameter.")
        print()
        
        # Get token ID from user
        token_id = input("Enter TOKEN_ID from redirect URL: ").strip()
        
        if not token_id:
            print("❌ TOKEN_ID is required!")
            return
        
        print("\n🔄 Generating access token...")
        access_token = dhan_login.consume_token_id(token_id, app_id, app_secret)
        
        print("\n" + "=" * 60)
        print("✅ Access Token Generated Successfully!")
        print("=" * 60)
        print(f"\nAccess Token: {access_token}")
        print("\n💾 Add this to your .env file:")
        print(f"DHAN_CLIENT_ID={client_id}")
        print(f"DHAN_ACCESS_TOKEN={access_token}")
        print()
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
        print("\n💡 Troubleshooting:")
        print("   - Verify your APP_ID and APP_SECRET are correct")
        print("   - Ensure you copied the complete TOKEN_ID from redirect URL")
        print("   - Check your internet connection")

def pin_totp_flow():
    """Generate access token using PIN + TOTP flow."""
    print("\n" + "=" * 60)
    print("🔐 DhanHQ PIN + TOTP Flow - Access Token Generator")
    print("=" * 60)
    
    try:
        from dhanhq import DhanLogin
    except ImportError:
        print("❌ ERROR: dhanhq package not installed")
        print("   Run: pip install dhanhq")
        return
    
    # Get credentials
    print("\n📝 Please provide the following details:")
    client_id = input("Client ID: ").strip()
    pin = input("Trading PIN: ").strip()
    totp = input("TOTP Code (from authenticator app): ").strip()
    
    if not client_id or not pin or not totp:
        print("❌ All fields are required!")
        return
    
    try:
        print("\n🔄 Generating access token...")
        dhan_login = DhanLogin(client_id)
        
        access_token_data = dhan_login.generate_token(pin, totp)
        
        if access_token_data and 'data' in access_token_data:
            access_token = access_token_data['data'].get('access_token', '')
            
            print("\n" + "=" * 60)
            print("✅ Access Token Generated Successfully!")
            print("=" * 60)
            print(f"\nAccess Token: {access_token}")
            print("\n💾 Add this to your .env file:")
            print(f"DHAN_CLIENT_ID={client_id}")
            print(f"DHAN_ACCESS_TOKEN={access_token}")
            print()
            
            # Show additional info
            if 'data' in access_token_data:
                data = access_token_data['data']
                print("ℹ️  Additional Information:")
                print(f"   User ID: {data.get('user_id', 'N/A')}")
                print(f"   Valid Until: {data.get('valid_till', 'N/A')}")
                print()
        else:
            print(f"\n⚠️  Unexpected response: {access_token_data}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
        print("\n💡 Troubleshooting:")
        print("   - Verify your Client ID is correct")
        print("   - Check if your trading PIN is correct")
        print("   - Ensure TOTP code is current (they expire quickly)")
        print("   - Make sure 2FA is enabled on your Dhan account")

def renew_token_flow():
    """Renew existing access token."""
    print("\n" + "=" * 60)
    print("🔄 DhanHQ Token Renewal")
    print("=" * 60)
    
    try:
        from dhanhq import DhanLogin
    except ImportError:
        print("❌ ERROR: dhanhq package not installed")
        print("   Run: pip install dhanhq")
        return
    
    print("\n📝 Please provide the following details:")
    client_id = input("Client ID: ").strip()
    old_token = input("Current Access Token: ").strip()
    
    if not client_id or not old_token:
        print("❌ All fields are required!")
        return
    
    try:
        print("\n🔄 Renewing access token...")
        dhan_login = DhanLogin(client_id)
        
        new_token = dhan_login.renew_token(old_token)
        
        print("\n" + "=" * 60)
        print("✅ Token Renewed Successfully!")
        print("=" * 60)
        print(f"\nNew Access Token: {new_token}")
        print("\n💾 Update your .env file with the new token:")
        print(f"DHAN_ACCESS_TOKEN={new_token}")
        print()
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
        print("\n💡 Troubleshooting:")
        print("   - Check if your old token is still valid")
        print("   - Verify your Client ID is correct")
        print("   - If renewal fails, generate a new token instead")

def main():
    """Main menu."""
    print("\n" + "=" * 60)
    print("🚀 DhanHQ Access Token Generator")
    print("=" * 60)
    print("\nChoose a method to generate/renew your access token:\n")
    print("1. OAuth Flow (Recommended for production)")
    print("2. PIN + TOTP Flow (Quick setup)")
    print("3. Renew Existing Token")
    print("4. Exit")
    print()
    
    choice = input("Enter your choice (1-4): ").strip()
    
    if choice == "1":
        oauth_flow()
    elif choice == "2":
        pin_totp_flow()
    elif choice == "3":
        renew_token_flow()
    elif choice == "4":
        print("\n👋 Goodbye!")
        sys.exit(0)
    else:
        print("\n❌ Invalid choice!")
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
