#!/usr/bin/env python3
"""
Startup script for Book Recommendation API
This script validates environment variables and starts the Flask server
"""

import os
import sys
from dotenv import load_dotenv

def validate_environment():
    """Validate required environment variables"""
    required_vars = [
        'MONGO_URI',
        'OPENAI_API_KEY'
    ]
    
    optional_vars = [
        'SENDGRID_API_KEY',
        'FACEBOOK_WHATSAPP_TOKEN',
        'FACEBOOK_WHATSAPP_PHONE_NUMBER_ID'
    ]
    
    missing_required = []
    missing_optional = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)
    
    for var in optional_vars:
        if not os.getenv(var):
            missing_optional.append(var)
    
    if missing_required:
        print(f"❌ Missing required environment variables: {', '.join(missing_required)}")
        print("Please set these variables in your .env file or environment")
        return False
    
    if missing_optional:
        print(f"⚠️  Missing optional environment variables: {', '.join(missing_optional)}")
        print("Some features may not work properly")
    
    print("✅ Environment validation passed")
    return True

def main():
    """Main startup function"""
    print("🚀 Starting Book Recommendation API Server...")
    
    # Load environment variables
    load_dotenv()
    
    # Validate environment
    if not validate_environment():
        sys.exit(1)
    
    # Import and run the Flask app
    from app import app
    
    # Set default port
    port = int(os.getenv('PORT', 5000))
    host = os.getenv('HOST', '0.0.0.0')
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    print(f"🌐 Server will run on http://{host}:{port}")
    print(f"🔧 Debug mode: {debug}")
    
    # Start the server
    app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    main() 