"""
Vercel serverless function entry point
Note: This is a basic setup - the app needs significant refactoring
for serverless deployment (external storage, database sessions, etc.)
"""
from app import app

# Export the Flask app for Vercel
# Note: This won't work properly without refactoring file storage and sessions
def handler(request):
    return app(request.environ, lambda status, headers: None)

