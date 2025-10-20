"""
Flask API server for Precious Metals Bull Market Tracker
Uses in-memory SQLite database loaded from SQL dump for deployment
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os
import sys
from datetime import datetime
from pathlib import Path

app = Flask(__name__)
CORS(app)

# Global in-memory database connection
db_conn = None
db_init_error = None

def find_sql_dump():
    """Find the SQL dump file using multiple strategies"""
    # Strategy 1: Same directory as this file (src/)
    script_dir = Path(__file__).resolve().parent
    sql_path = script_dir / 'schema_and_data.sql'
    if sql_path.exists():
        return sql_path
    
    # Strategy 2: Relative to this file (../database/)
    base_dir = script_dir.parent
    sql_path = base_dir / 'database' / 'schema_and_data.sql'
    if sql_path.exists():
        return sql_path
    
    # Strategy 3: Check current working directory
    sql_path = Path.cwd() / 'schema_and_data.sql'
    if sql_path.exists():
        return sql_path
    
    # Strategy 4: Check database subdirectory
    sql_path = Path.cwd() / 'database' / 'schema_and_data.sql'
    if sql_path.exists():
        return sql_path
    
    return None

def init_database():
    """Initialize in-memory database from SQL dump"""
    global db_conn, db_init_error
    
    try:
        # Create in-memory database
        db_conn = sqlite3.connect('file::memory:?cache=shared', uri=True, check_same_thread=False)
        db_conn.row_factory = sqlite3.Row
        
        # Find SQL dump
        sql_dump_path = find_sql_dump()
        
        if sql_dump_path and sql_dump_path.exists():
            print(f"✅ Found SQL dump at: {sql_dump_path}")
            with open(sql_dump_path, 'r') as f:
                sql_script = f.read()
                db_conn.executescript(sql_script)
            print(f"✅ Database loaded successfully")
            db_init_error = None
        else:
            error_msg = f"SQL dump not found. Searched in: {Path(__file__).parent.parent}, {Path.cwd()}"
            print(f"❌ {error_msg}")
            db_init_error = error_msg
            raise FileNotFoundError(error_msg)
        
        return db_conn
    except Exception as e:
        db_init_error = str(e)
        print(f"❌ Database initialization failed: {e}")
        raise

def get_db_connection():
    """Get database connection"""
    global db_conn
    if db_conn is None:
        db_conn = init_database()
    return db_conn

def serialize_data(data):
    """Convert database rows to JSON-serializable format"""
    if isinstance(data, list):
        return [dict(row) for row in data]
    elif hasattr(data, 'keys'):
        return dict(data)
    return data

@app.route('/')
def home():
    """API home endpoint - simple status check"""
    return jsonify({
        'name': 'Precious Metals Bull Market Tracker API',
        'version': '1.0',
        'status': 'ok'
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM current_prices')
        result = cursor.fetchone()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'database_type': 'in-memory',
            'current_records': result['count'],
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'init_error': db_init_error,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/weekly-data/<metal>/<cycle>', methods=['GET'])
def get_weekly_data(metal, cycle):
    """Get weekly aggregated data for a specific metal and cycle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                weeks_from_start,
                week_start_date,
                close_price,
                cycle_change_pct,
                limit_up_days,
                limit_down_days,
                volatility_indicator,
                total_trading_days,
                week_over_week_pct
            FROM weekly_aggregates 
            WHERE metal = ? AND cycle_name = ?
            ORDER BY weeks_from_start
        ''', (metal.upper(), cycle))
        
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            wow_pct = row['week_over_week_pct'] if row['week_over_week_pct'] else 0
            abs_wow = abs(wow_pct)
            
            if abs_wow >= 5:
                dot_color = '#ef4444'
                volatility_level = 'high_volatility'
            elif abs_wow >= 2:
                dot_color = '#eab308'
                volatility_level = 'volatile'
            else:
                dot_color = '#22c55e'
                volatility_level = 'normal'
            
            data.append({
                'week': row['weeks_from_start'],
                'date': row['week_start_date'],
                'price': row['close_price'],
                'percentChange': row['cycle_change_pct'],
                'weekOverWeekChange': wow_pct,
                'limitUpDays': row['limit_up_days'],
                'limitDownDays': row['limit_down_days'],
                'tradingDays': row['total_trading_days'],
                'dotColor': dot_color,
                'volatilityLevel': volatility_level,
                'showDot': True
            })
        
        return jsonify({
            'metal': metal.upper(),
            'cycle': cycle,
            'data': data,
            'total_weeks': len(data)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/raw-data/<metal>/<cycle>', methods=['GET'])
def get_raw_data(metal, cycle):
    """Get raw daily data for display in table format"""
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if cycle.endswith('_current'):
            table = 'current_prices'
            cycle_filter = f"metal = '{metal.upper()}'"
        else:
            table = 'historical_prices'
            cycle_filter = f"metal = '{metal.upper()}' AND cycle_name = '{cycle}'"
        
        cursor.execute(f'SELECT COUNT(*) as total FROM {table} WHERE {cycle_filter}')
        total_records = cursor.fetchone()['total']
        
        cursor.execute(f'''
            SELECT date, open_price, high_price, low_price, close_price, 
                   daily_change_pct, is_limit_up, is_limit_down, 
                   days_from_start, weeks_from_start, volume
            FROM {table} 
            WHERE {cycle_filter}
            ORDER BY date DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        rows = cursor.fetchall()
        data = serialize_data(rows)
        
        return jsonify({
            'metal': metal.upper(),
            'cycle': cycle,
            'data': data,
            'total_records': total_records,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total_records
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/market-summary', methods=['GET'])
def get_market_summary():
    """Get current market summary for dashboard"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        summary = {}
        
        for metal in ['GOLD', 'SILVER']:
            cursor.execute('''
                SELECT close_price, date, days_from_start, weeks_from_start
                FROM current_prices 
                WHERE metal = ?
                ORDER BY date DESC 
                LIMIT 1
            ''', (metal,))
            
            current_data = cursor.fetchone()
            
            cursor.execute('''
                SELECT start_price FROM market_cycles 
                WHERE metal = ? AND cycle_name = ?
            ''', (metal, f"{metal.lower()}_2024_current"))
            
            cycle_info = cursor.fetchone()
            
            cursor.execute('''
                SELECT MAX(close_price) as peak_price
                FROM historical_prices
                WHERE metal = ? AND cycle_name = ?
            ''', (metal, f"{metal.lower()}_1978_1980"))
            
            historical_peak = cursor.fetchone()
            
            cursor.execute('''
                SELECT COUNT(*) as limit_up_count
                FROM current_prices
                WHERE metal = ? AND is_limit_up = 1
            ''', (metal,))
            
            limit_up_info = cursor.fetchone()
            
            if current_data and cycle_info:
                current_price = current_data['close_price']
                start_price = cycle_info['start_price']
                current_return = ((current_price - start_price) / start_price) * 100
                
                hist_peak_return = 0
                if historical_peak and historical_peak['peak_price']:
                    hist_start_cursor = conn.cursor()
                    hist_start_cursor.execute('''
                        SELECT start_price FROM market_cycles 
                        WHERE metal = ? AND cycle_name = ?
                    ''', (metal, f"{metal.lower()}_1978_1980"))
                    hist_start = hist_start_cursor.fetchone()
                    if hist_start:
                        hist_peak_return = ((historical_peak['peak_price'] - hist_start['start_price']) / hist_start['start_price']) * 100
                
                summary[metal.lower()] = {
                    'currentPrice': current_price,
                    'currentReturn': round(current_return, 1),
                    'daysInCycle': current_data['days_from_start'],
                    'weeksInCycle': current_data['weeks_from_start'],
                    'lastUpdate': current_data['date'],
                    'historicalPeak': historical_peak['peak_price'] if historical_peak else 0,
                    'historicalPeakReturn': round(hist_peak_return, 1),
                    'limitUpDays': limit_up_info['limit_up_count'] if limit_up_info else 0
                }
        
        return jsonify({
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/database-stats', methods=['GET'])
def get_database_stats():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute('SELECT COUNT(*) as count FROM historical_prices WHERE metal = "GOLD"')
        stats['gold_historical'] = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM historical_prices WHERE metal = "SILVER"')
        stats['silver_historical'] = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM current_prices WHERE metal = "GOLD"')
        stats['gold_current'] = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM current_prices WHERE metal = "SILVER"')
        stats['silver_current'] = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM weekly_aggregates')
        stats['weekly_aggregates'] = cursor.fetchone()['count']
        
        cursor.execute('SELECT MIN(date) as start, MAX(date) as end FROM historical_prices')
        hist_range = cursor.fetchone()
        stats['historical_range'] = {'start': hist_range['start'], 'end': hist_range['end']}
        
        cursor.execute('SELECT MIN(date) as start, MAX(date) as end FROM current_prices')
        current_range = cursor.fetchone()
        stats['current_range'] = {'start': current_range['start'], 'end': current_range['end']}
        
        return jsonify({
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Initialize database on module load
print("🚀 Initializing Precious Metals API...")
print(f"📂 Current working directory: {Path.cwd()}")
print(f"📂 Script location: {Path(__file__).resolve().parent}")
try:
    init_database()
    print("✅ Database initialization complete")
except Exception as e:
    print(f"❌ Database initialization failed: {e}")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

