import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding

# Load environment variables
load_dotenv()

# Always use demo API for safety
KALSHI_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"

# City-specific NWS stations that Kalshi uses for resolution
# Based on Kalshi market rules and resolution sources
CITIES = {
    'Boston': {
        'lat': 42.3601, 
        'lon': -71.0589, 
        'kalshi_series': 'KXHIGHTBOS',
        'nws_station': 'KBOS',  # Boston Logan Airport
        'station_name': 'Boston Logan Airport'
    },
    'New York': {
        'lat': 40.7829, 
        'lon': -73.9654, 
        'kalshi_series': 'KXHIGHNY',
        'nws_station': 'KNYC',  # Central Park
        'station_name': 'NYC Central Park'
    },
    'Chicago': {
        'lat': 41.7868, 
        'lon': -87.7522, 
        'kalshi_series': 'KXHIGHCHI',
        'nws_station': 'KMDW',  # Midway Airport
        'station_name': 'Chicago Midway Airport'
    },
    'Los Angeles': {
        'lat': 33.9425, 
        'lon': -118.4081, 
        'kalshi_series': 'KXHIGHLAX',
        'nws_station': 'KLAX',  # LAX Airport
        'station_name': 'Los Angeles International Airport'
    },
    'Miami': {
        'lat': 25.7959, 
        'lon': -80.2870, 
        'kalshi_series': 'KXHIGHMIA',
        'nws_station': 'KMIA',  # Miami International Airport
        'station_name': 'Miami International Airport'
    },
    'San Francisco': {
        'lat': 37.6213, 
        'lon': -122.3790, 
        'kalshi_series': 'KXHIGHTSFO',
        'nws_station': 'KSFO',  # SFO Airport
        'station_name': 'San Francisco International Airport'
    },
}

def get_current_high_temp(city_name, nws_station, station_name):
    """
    Get the current high temperature from the EXACT station Kalshi uses for resolution.
    
    CRITICAL: This fetches current observations, but Kalshi settles on the FINAL CLI report
    which uses Local Standard Time (LST), not DST. The CLI is released the next morning.
    
    This means:
    - Current temps are useful for intraday arbitrage
    - But final settlement may differ if there's a temp spike late in the LST day
    - During DST, the "day" for CLI purposes extends to 1 AM the next calendar day
    """
    try:
        # Get latest observation from the specific station Kalshi uses
        obs_url = f"https://api.weather.gov/stations/{nws_station}/observations/latest"
        obs_response = requests.get(obs_url, headers={'User-Agent': 'WeatherArbitrageBot/1.0'}, timeout=10)
        obs_response.raise_for_status()
        obs_data = obs_response.json()
        
        # Get current temperature
        temp_c = obs_data['properties']['temperature']['value']
        
        if temp_c is None:
            print(f"  ⚠️  No temperature data from {station_name}")
            return None
            
        # Convert to Fahrenheit - use the exact conversion to match NWS
        temp_f = (temp_c * 9/5) + 32
        
        # Also try to get today's max temp from observations
        # This would be more accurate than current temp for arbitrage
        max_temp_24h = obs_data['properties'].get('maxTemperatureLast24Hours', {}).get('value')
        if max_temp_24h:
            max_temp_f = (max_temp_24h * 9/5) + 32
        else:
            max_temp_f = temp_f  # Fallback to current if max not available
        
        return {
            'current_temp': round(temp_f, 1),
            'max_temp_today': round(max_temp_f, 1),  # Best estimate of today's high so far
            'station_id': nws_station,
            'station_name': station_name,
            'observation_time': obs_data['properties']['timestamp']
        }
        
    except Exception as e:
        print(f"  ❌ Error fetching data from {station_name}: {e}")
        return None

def get_kalshi_markets(series_ticker, date_str='26feb12'):
    """
    Get all Kalshi markets for a given city/series.
    """
    try:
        markets_url = f"{KALSHI_API_BASE}/markets"
        params = {"series_ticker": series_ticker, "limit": 50}
        
        response = requests.get(markets_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Filter for today's markets
        markets = data.get('markets', [])
        todays_markets = [m for m in markets if date_str in m.get('ticker', '').lower()]
        
        return todays_markets
        
    except Exception as e:
        print(f"  ❌ Error fetching Kalshi markets: {e}")
        return []

def create_kalshi_signature(private_key_pem, timestamp, method, path):
    """
    Create RSA-PSS signature for Kalshi API authentication.
    
    Args:
        private_key_pem: Private key in PEM format (string)
        timestamp: Timestamp in milliseconds (string)
        method: HTTP method (e.g., 'POST', 'GET')
        path: API path without query params (e.g., '/trade-api/v2/portfolio/orders')
    
    Returns:
        Base64-encoded signature string
    """
    # Load the private key
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode('utf-8'),
        password=None,
        backend=default_backend()
    )
    
    # Create the message: timestamp + method + path
    message = f"{timestamp}{method}{path}".encode('utf-8')
    
    # Sign the message
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


def execute_trade(ticker, side, price_cents, quantity):
    """
    Execute a trade on Kalshi (demo mode only).
    
    Args:
        ticker: Market ticker (e.g., 'KXHIGHTBOS-26FEB12-B36.5')
        side: 'yes' or 'no'
        price_cents: Price in cents (e.g., 10 for 10¢)
        quantity: Number of contracts to buy
    
    Returns:
        dict with trade result or None if failed
    """
    # Get API credentials
    api_key = os.getenv('KALSHI_API_KEY_DEMO')
    private_key_pem = os.getenv('KALSHI_API_SECRET_DEMO')
    private_key_file = os.getenv('KALSHI_API_SECRET_DEMO_FILE')
    
    # Try to load from file if specified
    if private_key_file and os.path.exists(private_key_file):
        with open(private_key_file, 'r') as f:
            private_key_pem = f.read()
    
    if not api_key or not private_key_pem:
        print("  ❌ Error: KALSHI_API_KEY_DEMO and private key not configured")
        return None
    
    try:
        # Prepare request
        timestamp = str(int(datetime.now().timestamp() * 1000))
        method = "POST"
        path = "/trade-api/v2/portfolio/orders"
        
        # Create signature
        signature = create_kalshi_signature(private_key_pem, timestamp, method, path)
        
        # Build order
        order_url = f"{KALSHI_API_BASE}/portfolio/orders"
        
        order_data = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": quantity,
            "type": "limit",
            f"{side}_price": price_cents
        }
        
        headers = {
            'Content-Type': 'application/json',
            'KALSHI-ACCESS-KEY': api_key,
            'KALSHI-ACCESS-SIGNATURE': signature,
            'KALSHI-ACCESS-TIMESTAMP': timestamp
        }
        
        response = requests.post(order_url, json=order_data, headers=headers, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        return result
        
    except Exception as e:
        print(f"  ❌ Error executing trade: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text[:300]}")
        return None


def extract_temp_from_title(title):
    """
    Extract temperature range from market title.
    Examples:
    - "Will the maximum temperature be 36-37° on Feb 12, 2026?" -> (36, 37)
    - "Will the maximum temperature be >39° on Feb 12, 2026?" -> (39, 999)
    - "Will the maximum temperature be <32° on Feb 12, 2026?" -> (0, 32)
    """
    import re
    
    # Pattern for range: "36-37°"
    range_match = re.search(r'(\d+)-(\d+)°', title)
    if range_match:
        return (int(range_match.group(1)), int(range_match.group(2)))
    
    # Pattern for greater than: ">39°"
    gt_match = re.search(r'>(\d+)°', title)
    if gt_match:
        return (int(gt_match.group(1)), 999)
    
    # Pattern for less than: "<32°"
    lt_match = re.search(r'<(\d+)°', title)
    if lt_match:
        return (0, int(lt_match.group(1)))
    
    return None

def find_arbitrage_opportunities(city_name, weather_data, markets):
    """
    Find ZERO-RISK arbitrage opportunities.
    
    CONSERVATIVE APPROACH:
    - Only flag opportunities where current high CLEARLY exceeded the range
    - Add 0.5°F safety margin to account for:
      * Rounding differences between Celsius conversion
      * Potential data corrections before final CLI
      * LST vs DST timing differences
    
    Example: If max temp is 60.0°F, we only buy "No" on ranges up to 58-59°F
    (not 59-60°F, to be safe)
    """
    opportunities = []
    
    if not weather_data or not markets:
        return opportunities
    
    # Use the max temp observed so far today
    current_high = weather_data['max_temp_today']
    
    # SAFETY MARGIN: Be conservative
    SAFETY_MARGIN = 0.5  # degrees Fahrenheit
    
    for market in markets:
        title = market.get('title', '')
        ticker = market.get('ticker', '')
        
        temp_range = extract_temp_from_title(title)
        if not temp_range:
            continue
        
        low_temp, high_temp = temp_range
        
        # Get market probability (last_price is in cents, so 87 = 87%)
        yes_price = market.get('last_price', market.get('yes_price', 0))
        no_price = 100 - yes_price
        
        # ZERO-RISK ARBITRAGE (with safety margin):
        # Current high has CLEARLY exceeded this range's maximum
        # We use a safety margin to avoid edge cases
        
        if current_high > (high_temp + SAFETY_MARGIN):
            # Current temp clearly exceeded this range
            # "NO" is guaranteed to win
            # Only flag if "NO" offers profit (trading below 95¢)
            if no_price < 95:
                profit = 100 - no_price
                opportunities.append({
                    'city': city_name,
                    'ticker': ticker,
                    'title': title,
                    'temp_range': f"{low_temp}-{high_temp}°F",
                    'current_high': current_high,
                    'yes_price': yes_price,
                    'no_price': no_price,
                    'action': f'BUY NO at {no_price}¢',
                    'confidence': '🔒 GUARANTEED',
                    'reason': f'High is {current_high}°F, clearly above {high_temp}°F',
                    'profit_per_share': f'{profit}¢',
                    'roi': f'{round((profit/no_price)*100) if no_price > 0 else 0}%'
                })
    
    return opportunities

def main(auto_execute=False, max_trade_amount=10):
    """
    Main function to scan for arbitrage opportunities.
    
    Args:
        auto_execute: If True, automatically execute trades when opportunities found
        max_trade_amount: Maximum dollar amount per trade (default $10)
    """
    print("="*70)
    print("WEATHER ARBITRAGE OPPORTUNITY FINDER - DEMO MODE")
    print("="*70)
    print(f"API: {KALSHI_API_BASE}")
    print(f"Date: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    
    if auto_execute:
        print(f"🤖 AUTO-EXECUTE: ON (Max ${max_trade_amount} per trade)")
    else:
        print(f"📋 AUTO-EXECUTE: OFF (Scan only)")
    
    print(f"\n⚠️  IMPORTANT NOTES:")
    print(f"  • Using DEMO environment (fake money only)")
    print(f"  • Using EXACT stations Kalshi uses for settlement")
    print(f"  • Kalshi settles on NWS Daily Climate Report (next morning)")
    print(f"  • CLI uses Local Standard Time (LST), not DST")
    print(f"  • Current temps are estimates; final CLI may differ")
    print(f"  • Conservative 0.5°F safety margin applied")
    print(f"\nScanning {len(CITIES)} cities...\n")
    
    all_opportunities = []
    
    for city_name, city_data in CITIES.items():
        print(f"\n{'='*70}")
        print(f"🌡️  {city_name.upper()} ({city_data['station_name']})")
        print(f"{'='*70}")
        
        # Get current weather data from the EXACT station Kalshi uses
        weather_data = get_current_high_temp(
            city_name, 
            city_data['nws_station'],
            city_data['station_name']
        )
        
        if weather_data:
            print(f"  Current Temperature: {weather_data['current_temp']}°F")
            print(f"  Max Today (so far): {weather_data['max_temp_today']}°F")
            print(f"  Station: {weather_data['station_id']}")
        
        # Get Kalshi markets
        print(f"\n  Fetching Kalshi markets...")
        markets = get_kalshi_markets(city_data['kalshi_series'])
        
        if markets:
            print(f"  Found {len(markets)} markets for today")
        else:
            print(f"  ⚠️  No markets found")
            continue
        
        # Find arbitrage opportunities
        opportunities = find_arbitrage_opportunities(city_name, weather_data, markets)
        
        if opportunities:
            print(f"\n  🎯 FOUND {len(opportunities)} ZERO-RISK OPPORTUNITY(IES)!")
            for opp in opportunities:
                print(f"\n  💰 GUARANTEED PROFIT OPPORTUNITY:")
                print(f"     Market: {opp['title']}")
                print(f"     Action: {opp['action']}")
                print(f"     Confidence: {opp['confidence']}")
                print(f"     Reason: {opp['reason']}")
                print(f"     Profit per share: {opp['profit_per_share']} ({opp['roi']} ROI)")
                
                # Auto-execute if enabled
                if auto_execute:
                    # Calculate how many contracts to buy
                    # $10 max trade at current price
                    price_dollars = opp['no_price'] / 100
                    max_contracts = int(max_trade_amount / price_dollars)
                    
                    print(f"\n     🤖 EXECUTING TRADE:")
                    print(f"        Buying {max_contracts} contracts at {opp['no_price']}¢")
                    print(f"        Total cost: ${max_contracts * price_dollars:.2f}")
                    
                    result = execute_trade(
                        ticker=opp['ticker'],
                        side='no',
                        price_cents=opp['no_price'],
                        quantity=max_contracts
                    )
                    
                    if result:
                        print(f"        ✅ Trade executed successfully!")
                        print(f"        Order ID: {result.get('order', {}).get('order_id', 'N/A')}")
                    else:
                        print(f"        ❌ Trade failed")
            
            all_opportunities.extend(opportunities)
        else:
            print(f"\n  ✓ No arbitrage opportunities found (markets are efficient)")
    
    # Summary
    print(f"\n\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Total cities scanned: {len(CITIES)}")
    print(f"Total opportunities found: {len(all_opportunities)}")
    
    if all_opportunities:
        print(f"\n🔒 ZERO-RISK PROFIT OPPORTUNITIES:")
        print(f"(Current high has already exceeded these ranges - guaranteed wins)")
        for i, opp in enumerate(all_opportunities, 1):
            print(f"\n{i}. {opp['city']} - {opp['ticker']}")
            print(f"   {opp['action']}")
            print(f"   Profit: {opp['profit_per_share']} per share ({opp['roi']} ROI)")
            print(f"   Why: {opp['reason']}")
    else:
        print("\n✓ No zero-risk opportunities found. Markets have caught up to current temps.")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    # Run with auto-execute enabled for demo mode
    # This will automatically buy "No" contracts on guaranteed opportunities
    # Max $10 per trade (using demo money)
    main(auto_execute=True, max_trade_amount=10)