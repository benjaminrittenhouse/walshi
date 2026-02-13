import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
import time
import re

# Load environment variables
load_dotenv()

# ============================================================================
# PRODUCTION FLAG - CHANGE THIS TO GO LIVE
# ============================================================================
PRODUCTION_MODE = False  # Set to True for real money trading
# ============================================================================

# Configuration based on mode
if PRODUCTION_MODE:
    KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
    API_KEY_ENV = "KALSHI_API_KEY"
    API_SECRET_ENV = "KALSHI_API_SECRET_FILE"
    MODE_LABEL = "🚨 PRODUCTION MODE - USING REAL MONEY!"
else:
    KALSHI_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
    API_KEY_ENV = "KALSHI_API_KEY_DEMO"
    API_SECRET_ENV = "KALSHI_API_SECRET_DEMO_FILE"
    MODE_LABEL = "⚠️  DEMO MODE - Using fake Kalshi money"

# Trading parameters for continuous polling
POLL_INTERVAL_SECONDS = 5 * 60  # 5 minutes (normal polling)
BURST_POLL_SECONDS = 60  # 1 minute (during critical window)
MAX_TRADE_AMOUNT = 5  # Max $ per trade (TRIAL RUN - conservative)
MAX_TOTAL_TRADES = 5  # Max total trades per session (TRIAL RUN = $25 max)
SAFETY_MARGIN = 0.01  # Minimal margin for floating point comparison only

print(MODE_LABEL)
print(f"📊 Poll interval: {POLL_INTERVAL_SECONDS} seconds ({POLL_INTERVAL_SECONDS/60:.0f} minutes)")
print(f"⚡ Burst polling: {BURST_POLL_SECONDS} seconds during :53-:03 window")
print(f"💰 Max trade amount: ${MAX_TRADE_AMOUNT}")
print(f"🎯 Max total trades: {MAX_TOTAL_TRADES} (${MAX_TRADE_AMOUNT * MAX_TOTAL_TRADES} max exposure)")
print()

# City-specific NWS stations that Kalshi uses for resolution
# LIMITED TO NYC AND BOSTON FOR TRIAL RUN
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
    # Commented out for trial run - uncomment for full production
    # 'Chicago': {
    #     'lat': 41.7868, 
    #     'lon': -87.7522, 
    #     'kalshi_series': 'KXHIGHCHI',
    #     'nws_station': 'KMDW',
    #     'station_name': 'Chicago Midway Airport'
    # },
    # 'Los Angeles': {
    #     'lat': 33.9425, 
    #     'lon': -118.4081, 
    #     'kalshi_series': 'KXHIGHLAX',
    #     'nws_station': 'KLAX',
    #     'station_name': 'Los Angeles International Airport'
    # },
    # 'Miami': {
    #     'lat': 25.7959, 
    #     'lon': -80.2870, 
    #     'kalshi_series': 'KXHIGHMIA',
    #     'nws_station': 'KMIA',
    #     'station_name': 'Miami International Airport'
    # },
    # 'San Francisco': {
    #     'lat': 37.6213, 
    #     'lon': -122.3790, 
    #     'kalshi_series': 'KXHIGHTSFO',
    #     'nws_station': 'KSFO',
    #     'station_name': 'San Francisco International Airport'
    # },
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
    
    Returns dict or None on error (with error logged)
    """
    try:
        # Get latest observation from the specific station Kalshi uses
        obs_url = f"https://api.weather.gov/stations/{nws_station}/observations/latest"
        obs_response = requests.get(
            obs_url, 
            headers={'User-Agent': 'WeatherArbitrageBot/1.0'}, 
            timeout=10
        )
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
    
    except requests.exceptions.Timeout:
        print(f"  ⚠️  Timeout fetching data from {station_name}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  ⚠️  HTTP error from {station_name}: {e.response.status_code}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  Network error from {station_name}: {str(e)[:80]}")
        return None
    except (KeyError, TypeError, ValueError) as e:
        print(f"  ⚠️  Data parsing error for {station_name}: {str(e)[:80]}")
        return None
    except Exception as e:
        print(f"  ❌ Unexpected error from {station_name}: {str(e)[:80]}")
        return None

def get_kalshi_markets(series_ticker, date_str=None):
    """
    Get all Kalshi markets for a given city/series.
    
    Args:
        series_ticker: The series to query (e.g., 'KXHIGHTBOS')
        date_str: Optional specific date string (e.g., '13feb12'). 
                  If None, uses today's date in Eastern Time.
    
    Returns list of markets or empty list on error
    """
    # Auto-generate date string if not provided
    if date_str is None:
        # CRITICAL: Use Eastern Time (NYC/Boston timezone), not UTC
        # Kalshi markets are based on Eastern Time
        from datetime import timezone, timedelta
        eastern = timezone(timedelta(hours=-5))  # EST (UTC-5)
        today = datetime.now(eastern)
        # Kalshi format: YYMMMDD (e.g., '26feb13' for Feb 13, 2026)
        date_str = today.strftime('%y%b%d').lower()  # Fixed: year-month-day
    
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
    
    except requests.exceptions.Timeout:
        print(f"  ⚠️  Timeout fetching Kalshi markets for {series_ticker}")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"  ⚠️  HTTP error from Kalshi: {e.response.status_code}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  Network error fetching Kalshi markets: {str(e)[:80]}")
        return []
    except (KeyError, TypeError, ValueError) as e:
        print(f"  ⚠️  Data parsing error for Kalshi markets: {str(e)[:80]}")
        return []
    except Exception as e:
        print(f"  ❌ Unexpected error fetching Kalshi markets: {str(e)[:80]}")
        return []
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


def get_account_balance():
    """
    Get current account balance from Kalshi.
    Returns balance in dollars or None on error.
    """
    try:
        api_key = os.getenv(API_KEY_ENV)
        private_key_file = os.getenv(API_SECRET_ENV)
        
        if not api_key or not private_key_file or not os.path.exists(private_key_file):
            return None
        
        with open(private_key_file, 'r') as f:
            private_key_pem = f.read()
        
        timestamp = str(int(datetime.now().timestamp() * 1000))
        method = "GET"
        path = "/trade-api/v2/portfolio/balance"
        
        signature = create_kalshi_signature(private_key_pem, timestamp, method, path)
        
        balance_url = f"{KALSHI_API_BASE}/portfolio/balance"
        headers = {
            'KALSHI-ACCESS-KEY': api_key,
            'KALSHI-ACCESS-SIGNATURE': signature,
            'KALSHI-ACCESS-TIMESTAMP': timestamp
        }
        
        response = requests.get(balance_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        balance_cents = data.get('balance', 0)
        return balance_cents / 100  # Convert cents to dollars
        
    except Exception as e:
        print(f"⚠️  Could not fetch account balance: {e}")
        return None


def execute_trade(ticker, side, price_cents, quantity):
    """
    Execute a trade on Kalshi.
    
    Args:
        ticker: Market ticker (e.g., 'KXHIGHTBOS-26FEB12-B36.5')
        side: 'yes' or 'no'
        price_cents: Price in cents (e.g., 10 for 10¢)
        quantity: Number of contracts to buy
    
    Returns:
        dict with trade result or None if failed
    """
    # Get API credentials based on mode
    api_key = os.getenv(API_KEY_ENV)
    private_key_file = os.getenv(API_SECRET_ENV)
    
    # Load private key from file
    private_key_pem = None
    if private_key_file and os.path.exists(private_key_file):
        with open(private_key_file, 'r') as f:
            private_key_pem = f.read()
    
    if not api_key or not private_key_pem:
        print(f"  ❌ Error: API credentials not configured properly")
        print(f"     Looking for: {API_KEY_ENV} and {API_SECRET_ENV}")
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
        error_msg = str(e)
        print(f"  ❌ Error executing trade: {e}")
        
        # Check if it's an insufficient funds error
        if hasattr(e, 'response') and e.response is not None:
            response_text = e.response.text[:300]
            print(f"  Response: {response_text}")
            
            # Check for insufficient funds
            if 'insufficient funds' in response_text.lower() or 'balance' in response_text.lower():
                print(f"  ⚠️  INSUFFICIENT FUNDS - Please deposit money to continue trading")
                print(f"  💡 Bot will continue running but won't trade until you add funds")
        
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

def is_critical_window():
    """
    Check if current time is in the critical METAR update window.
    
    METAR observations are published at :52-:53 past the hour (based on actual timestamps).
    API typically updates within a few minutes after publication.
    We poll more frequently during :50-:00 to catch new highs ASAP.
    
    Returns:
        True if we should use burst polling (1 minute intervals)
        False if we should use normal polling (5 minute intervals)
    """
    now = datetime.now()
    minute = now.minute
    
    # Critical window: :50 through :00 (wraps around the hour)
    # :50, :51, :52, :53, :54, :55, :56, :57, :58, :59, :00
    return minute >= 50 or minute == 0


def get_next_poll_interval():
    """
    Get the appropriate polling interval based on current time.
    Also checks if markets are closing soon.
    
    Returns:
        Number of seconds until next poll, or None if should stop polling
    """
    from datetime import timezone, timedelta
    eastern = timezone(timedelta(hours=-5))  # EST
    now_et = datetime.now(eastern)
    hour_et = now_et.hour
    minute = now_et.minute
    
    # Stop polling after 10:30 PM ET (markets close at 11:59 PM)
    # Check: hour > 22 OR (hour == 22 AND minute >= 30)
    if hour_et > 22 or (hour_et == 22 and minute >= 30):
        return None  # Signal to stop polling
    
    # Critical window: :53-:03 (METAR publication window)
    if minute >= 53 or minute <= 3:
        return BURST_POLL_SECONDS  # 60 seconds during critical window
    else:
        return POLL_INTERVAL_SECONDS  # 300 seconds (5 minutes) normally


def find_arbitrage_opportunities(city_name, weather_data, markets):
    """
    Find ZERO-RISK arbitrage opportunities.
    
    STRATEGY:
    - If NWS reports current high of 36°F, buy "No" on ANY market asking "Will it be <36°F?"
    - We read the SAME data Kalshi reads (NWS API in Fahrenheit)
    - No conversion issues - we're comparing apples to apples
    - Daily highs can only GO UP, never down
    
    Example: If max temp is 60.0°F, we buy "No" on all ranges with max < 60°F
    (Including 59-60°F, 58-59°F, <60°F, etc.)
    """
    opportunities = []
    
    if not weather_data or not markets:
        return opportunities
    
    # Use the max temp observed so far today
    current_high = weather_data['max_temp_today']
    
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
        
        # ZERO-RISK ARBITRAGE:
        # Current high has exceeded this range's maximum
        # "NO" is guaranteed to win
        # Use tiny margin (0.01) only for floating point comparison
        
        if current_high >= (high_temp + SAFETY_MARGIN):
            # Current temp has reached or exceeded this range
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
                    'reason': f'High is {current_high}°F, above {high_temp}°F',
                    'profit_per_share': f'{profit}¢',
                    'roi': f'{round((profit/no_price)*100) if no_price > 0 else 0}%'
                })
    
    return opportunities

def scan_once(auto_execute=False, max_trade_amount=10, traded_today=None, max_total_trades=None):
    """
    Single scan iteration for arbitrage opportunities.
    
    Args:
        auto_execute: If True, automatically execute trades when opportunities found
        max_trade_amount: Maximum dollar amount per trade (default $10)
        traded_today: Set of tickers already traded today (to avoid duplicates)
        max_total_trades: Maximum total trades allowed (None = unlimited)
    
    Returns:
        dict with scan results and updated traded_today set
    """
    if traded_today is None:
        traded_today = set()
    
    scan_results = {
        'timestamp': datetime.now(),
        'opportunities_found': 0,
        'trades_executed': 0,
        'cities_scanned': 0,
        'high_temps': {},
        'cities_with_data': [],
        'max_trades_hit': False
    }
    
    for city_name, city_data in CITIES.items():
        # Check if we've hit max trades limit
        if max_total_trades and len(traded_today) >= max_total_trades:
            scan_results['max_trades_hit'] = True
            print(f"  🛑 Max trades limit reached ({max_total_trades}). Skipping remaining cities.")
            break
        
        # Get current weather data from the EXACT station Kalshi uses
        weather_data = get_current_high_temp(
            city_name, 
            city_data['nws_station'],
            city_data['station_name']
        )
        
        if not weather_data:
            continue
        
        scan_results['cities_scanned'] += 1
        scan_results['cities_with_data'].append(city_name)
        
        # Store high temp for this city
        max_temp = weather_data['max_temp_today']
        scan_results['high_temps'][city_name] = max_temp
        
        # Get Kalshi markets for this city
        markets = get_kalshi_markets(city_data['kalshi_series'])
        
        if not markets:
            continue
        
        # Find arbitrage opportunities
        opportunities = find_arbitrage_opportunities(city_name, weather_data, markets)
        scan_results['opportunities_found'] += len(opportunities)
        
        # Execute trades if enabled
        if auto_execute and opportunities:
            for opp in opportunities:
                # Check if we've hit max trades limit
                if max_total_trades and len(traded_today) >= max_total_trades:
                    print(f"  🛑 Max trades limit ({max_total_trades}) reached. Skipping remaining opportunities.")
                    scan_results['max_trades_hit'] = True
                    break
                
                ticker = opp['ticker']
                
                # Skip if we already traded this today
                if ticker in traded_today:
                    print(f"  ⏭️  Skipping {ticker} - already traded today")
                    continue
                
                # Calculate how many contracts to buy
                no_price = opp['no_price']
                price_dollars = no_price / 100
                quantity = int(max_trade_amount / price_dollars)
                
                print(f"\n  🤖 EXECUTING: {city_name} - {opp['title'][:50]}...")
                print(f"      Buying {quantity} contracts at {no_price}¢ = ${quantity * price_dollars:.2f}")
                
                # Execute the trade
                result = execute_trade(ticker, 'no', no_price, quantity)
                
                if result:
                    traded_today.add(ticker)
                    scan_results['trades_executed'] += 1
                    print(f"      ✅ Success! Order ID: {result.get('order', {}).get('order_id', 'N/A')}")
                else:
                    print(f"      ❌ Failed")
    
    return scan_results, traded_today


def run_continuous(auto_execute=True, max_trade_amount=5, max_total_trades=5):
    """
    Run continuous polling loop until stopped or max trades hit.
    
    Args:
        auto_execute: If True, automatically execute trades when opportunities found
        max_trade_amount: Maximum dollar amount per trade
        max_total_trades: Maximum total trades for the session (stops when hit)
    """
    print("="*70)
    print("WEATHER ARBITRAGE BOT - CONTINUOUS POLLING MODE")
    print("="*70)
    print(f"API: {KALSHI_API_BASE}")
    print(f"Mode: {MODE_LABEL}")
    print(f"Started: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    print(f"Polling Strategy: ADAPTIVE")
    print(f"  • Normal: {POLL_INTERVAL_SECONDS/60:.0f} min intervals")
    print(f"  • Critical Window (:50-:00): {BURST_POLL_SECONDS} sec intervals")
    
    if auto_execute:
        print(f"\n🤖 AUTO-EXECUTE: ON (Max ${max_trade_amount} per trade)")
        print(f"🎯 MAX TRADES: {max_total_trades} (${max_trade_amount * max_total_trades} total exposure)")
    else:
        print(f"\n📋 AUTO-EXECUTE: OFF (Scan only)")
    
    print(f"\n⚠️  IMPORTANT NOTES:")
    print(f"  • Using {'PRODUCTION' if PRODUCTION_MODE else 'DEMO'} environment")
    print(f"  • Using EXACT stations Kalshi uses for settlement")
    print(f"  • Kalshi settles on NWS Daily Climate Report (next morning)")
    print(f"  • Will track positions to avoid duplicate trades")
    print(f"  • Adaptive polling: Burst mode at :50-:00 when METARs publish")
    print(f"  • Bot will stop automatically when {max_total_trades} trades executed")
    print(f"  • Press Ctrl+C to stop early\n")
    
    # Check account balance if auto-executing
    if auto_execute:
        print("🔍 Checking account balance...")
        balance = get_account_balance()
        if balance is not None:
            print(f"💰 Current balance: ${balance:.2f}")
            
            # Warn if balance is low
            required = max_trade_amount * max_total_trades
            if balance < required:
                print(f"⚠️  WARNING: Balance (${balance:.2f}) is less than max exposure (${required:.2f})")
                print(f"   You may not be able to execute all {max_total_trades} trades")
                print(f"   Consider depositing ${required - balance:.2f} more")
            elif balance < max_trade_amount:
                print(f"🚨 CRITICAL: Balance (${balance:.2f}) is less than trade size (${max_trade_amount})")
                print(f"   Bot will not be able to execute ANY trades!")
                print(f"   Please deposit at least ${max_trade_amount} to continue")
        print()
    
    # State tracking
    traded_today = set()  # Tickers we've already bought today
    previous_highs = {}   # Track highest temp seen for each city
    scan_count = 0
    total_opportunities = 0
    total_trades = 0
    
    try:
        while True:
            # Check if we've hit max trades
            if len(traded_today) >= max_total_trades:
                print(f"\n{'='*70}")
                print(f"🎯 MAX TRADES LIMIT REACHED ({max_total_trades} trades)")
                print(f"{'='*70}")
                print(f"Stopping bot automatically...")
                break
            
            scan_count += 1
            print(f"\n{'='*70}")
            print(f"SCAN #{scan_count} - {datetime.now().strftime('%I:%M:%S %p')}")
            print(f"{'='*70}")
            
            # Perform scan
            results, traded_today = scan_once(
                auto_execute, 
                max_trade_amount, 
                traded_today,
                max_total_trades
            )
            
            # Update totals
            total_opportunities += results['opportunities_found']
            total_trades += results['trades_executed']
            
            # Check for new highs
            new_highs_found = []
            for city, current_high in results['high_temps'].items():
                prev_high = previous_highs.get(city, 0)
                if current_high > prev_high:
                    new_highs_found.append(f"{city}: {prev_high}°F → {current_high}°F")
                    previous_highs[city] = current_high
            
            # Print summary
            print(f"\n📊 SCAN SUMMARY:")
            print(f"  Cities scanned: {results['cities_scanned']}")
            print(f"  Opportunities found: {results['opportunities_found']}")
            print(f"  Trades executed: {results['trades_executed']}")
            
            if new_highs_found:
                print(f"\n🌡️  NEW HIGHS DETECTED:")
                for update in new_highs_found:
                    print(f"  • {update}")
            
            print(f"\n📈 SESSION TOTALS:")
            print(f"  Total scans: {scan_count}")
            print(f"  Total opportunities: {total_opportunities}")
            print(f"  Total trades: {total_trades}")
            print(f"  Positions held: {len(traded_today)}")
            
            # Wait before next poll (unless max trades hit)
            if results.get('max_trades_hit'):
                print(f"\n🎯 Max trades limit reached. Stopping bot...")
                break
            
            # Determine next poll interval (adaptive based on time)
            next_interval = get_next_poll_interval()
            
            # Check if markets are closing (after 10:30 PM ET)
            if next_interval is None:
                print(f"\n🌙 MARKETS CLOSING SOON (after 10:30 PM ET)")
                print(f"Stopping bot for the night...")
                break
            
            in_critical_window = is_critical_window()
            
            if in_critical_window:
                window_status = "⚡ CRITICAL WINDOW (:50-:00) - Burst polling"
            else:
                window_status = "🕐 Normal window - Standard polling"
            
            print(f"\n{window_status}")
            print(f"⏰ Next scan in {next_interval} seconds ({next_interval/60:.1f} minutes)...")
            time.sleep(next_interval)
            
    except KeyboardInterrupt:
        print(f"\n\n{'='*70}")
        print("🛑 STOPPING BOT (USER INTERRUPTED)")
        print(f"{'='*70}")
    except Exception as e:
        print(f"\n\n{'='*70}")
        print("❌ BOT STOPPED DUE TO ERROR")
        print(f"{'='*70}")
        print(f"Error: {str(e)[:200]}")
    
    # Final stats (runs regardless of how we stopped)
    print(f"\n📊 FINAL SESSION STATS:")
    print(f"  Total scans: {scan_count}")
    print(f"  Total opportunities found: {total_opportunities}")
    print(f"  Total trades executed: {total_trades}")
    print(f"  Positions held: {len(traded_today)}")
    print(f"  Total exposure: ${total_trades * max_trade_amount}")
    
    if traded_today:
        print(f"\n📋 POSITIONS HELD:")
        for ticker in sorted(traded_today):
            print(f"  • {ticker}")
    
    print(f"\n✅ Bot stopped gracefully")


if __name__ == "__main__":
    # TRIAL RUN CONFIGURATION
    # $5 per trade, max 5 trades = $25 total exposure
    # NYC and Boston only
    run_continuous(
        auto_execute=True, 
        max_trade_amount=MAX_TRADE_AMOUNT,  # $5
        max_total_trades=MAX_TOTAL_TRADES   # 5 trades max
    )