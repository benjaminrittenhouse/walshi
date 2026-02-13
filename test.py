import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding

# Load environment variables
load_dotenv()

def create_signature(private_key_pem, timestamp, method, path):
    """
    Create RSA-PSS signature for Kalshi API authentication.
    
    Based on: https://docs.kalshi.com/getting_started/quick_start_authenticated_requests
    """
    # Load the private key from PEM string
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode('utf-8'),
        password=None,
        backend=default_backend()
    )
    
    # Create the message: timestamp + method + path (without query params)
    # Example: "1703123456789GET/trade-api/v2/portfolio/balance"
    path_without_query = path.split('?')[0]
    message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
    
    print(f"Signing message: {timestamp}{method}{path_without_query}")
    
    # Sign the message using RSA-PSS
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    
    # Return base64-encoded signature
    return base64.b64encode(signature).decode('utf-8')


def get_account_balance():
    """
    Get your Kalshi demo account balance.
    """
    # Get credentials from environment
    api_key_id = os.getenv('KALSHI_API_KEY_DEMO')
    private_key_pem = os.getenv('KALSHI_API_SECRET_DEMO')
    private_key_file = os.getenv('KALSHI_API_SECRET_DEMO_FILE')
    
    # Try to load from file if specified
    if private_key_file and os.path.exists(private_key_file):
        print(f"Loading private key from file: {private_key_file}")
        with open(private_key_file, 'r') as f:
            private_key_pem = f.read()
    
    if not api_key_id or not private_key_pem:
        print("❌ Error: KALSHI_API_KEY_DEMO and KALSHI_API_SECRET_DEMO (or KALSHI_API_SECRET_DEMO_FILE) must be set")
        print("\nOption 1 - Use file:")
        print("KALSHI_API_KEY_DEMO=your-key-id")
        print("KALSHI_API_SECRET_DEMO_FILE=./kalshi_demo_key.pem")
        print("\nOption 2 - Use inline (with proper newlines):")
        print("KALSHI_API_KEY_DEMO=your-key-id")
        print('KALSHI_API_SECRET_DEMO="-----BEGIN RSA PRIVATE KEY-----\\nMII...\\n-----END RSA PRIVATE KEY-----"')
        return None
    
    print("="*60)
    print("KALSHI DEMO API - GET ACCOUNT BALANCE TEST")
    print("="*60)
    print(f"API Key ID: {api_key_id[:20]}...")
    print(f"Private Key loaded: {len(private_key_pem)} characters")
    
    # Set up the request
    base_url = "https://demo-api.kalshi.co"
    path = "/trade-api/v2/portfolio/balance"
    method = "GET"
    timestamp = str(int(datetime.now().timestamp() * 1000))
    
    print(f"\nRequest details:")
    print(f"  URL: {base_url}{path}")
    print(f"  Method: {method}")
    print(f"  Timestamp: {timestamp}")
    
    try:
        # Create the signature
        print(f"\nCreating signature...")
        signature = create_signature(private_key_pem, timestamp, method, path)
        print(f"Signature created: {signature[:50]}...")
        
        # Make the request
        headers = {
            'KALSHI-ACCESS-KEY': api_key_id,
            'KALSHI-ACCESS-SIGNATURE': signature,
            'KALSHI-ACCESS-TIMESTAMP': timestamp
        }
        
        print(f"\nMaking request...")
        response = requests.get(base_url + path, headers=headers, timeout=10)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            balance_cents = data.get('balance', 0)
            balance_dollars = balance_cents / 100
            
            print("\n" + "="*60)
            print("✅ SUCCESS!")
            print("="*60)
            print(f"Your Kalshi Demo Account Balance: ${balance_dollars:.2f}")
            print(f"Balance in cents: {balance_cents}¢")
            print("="*60)
            return data
        else:
            print(f"\n❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"\n❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    get_account_balance()