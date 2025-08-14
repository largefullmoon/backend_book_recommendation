from flask import Flask, request, jsonify
from pymongo import MongoClient
import openai
from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timedelta
from flask_cors import CORS
from bson import ObjectId
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content

import json
import re
import urllib.parse
import time
import pandas as pd

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(MONGO_URI)
db = client['book_recommendation']

# Collections
books_collection = db['books']
users_collection = db['users']
recommendations_collection = db['recommendations']
consent_collection = db['parent_consent']
quiz_users_collection = db['quiz_users']  # New collection for quiz users
quiz_responses_collection = db['quiz_responses']  # New collection for detailed quiz responses
recommendation_plans_collection = db['recommendation_plans']  # New collection for recommendation plans

# Age groups configuration
AGE_GROUPS = ['Below 5', '6-8', '9-10', '11-12', '13+']

# Configure OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

# SerpAPI no longer needed - using direct JustBookify links

# Configure Shopify
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')

# Configure SendGrid
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'your-verified-sender@yourdomain.com')

# Configure Facebook WhatsApp API
FACEBOOK_ACCESS_TOKEN = os.getenv('FACEBOOK_ACCESS_TOKEN')
WHATSAPP_PHONE_NUMBER_ID = os.getenv('WHATSAPP_PHONE_NUMBER_ID')
WHATSAPP_BUSINESS_ACCOUNT_ID = os.getenv('WHATSAPP_BUSINESS_ACCOUNT_ID')

# Helper function to convert ObjectId to string in documents
def format_document(doc):
    if doc is None:
        return None
    doc['id'] = str(doc.pop('_id'))
    return doc

# Helper function to validate age group
def validate_age_group(age_group):
    if age_group not in AGE_GROUPS:
        raise ValueError(f"Invalid age group. Must be one of: {', '.join(AGE_GROUPS)}")

# Helper function to initialize recommendations
def initialize_recommendations():
    try:
        for age_group in AGE_GROUPS:
            existing = recommendations_collection.find_one({'age_group': age_group})
            if not existing:
                recommendations_collection.insert_one({
                    'age_group': age_group,
                    'books': []  # Now stores list of book objects
                })
    except Exception as e:
        print(f"Error initializing recommendations: {str(e)}")

# Call initialize_recommendations on startup
initialize_recommendations()

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

@app.route('/user-profile', methods=['POST'])
def create_user_profile():
    try:
        data = request.json
        age = data.get('age')
        genres = data.get('genres', [])
        liked_books = data.get('liked_books', [])
        
        user_data = {
            'age': age,
            'genres': genres,
            'liked_books': liked_books,
            'created_at': datetime.utcnow()
        }
        
        result = users_collection.insert_one(user_data)
        
        return jsonify({
            'success': True,
            'user_id': str(result.inserted_id),
            'message': 'User profile created successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/parent-consent', methods=['POST'])
def submit_parent_consent():
    try:
        data = request.json
        child_age = data.get('child_age')
        parent_name = data.get('parent_name')
        parent_contact = data.get('parent_contact')
        
        if not all([child_age, parent_name, parent_contact]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields'
            }), 400
            
        consent_data = {
            'child_age': child_age,
            'parent_name': parent_name,
            'parent_contact': parent_contact,
            'consent_date': datetime.utcnow()
        }
        
        result = consent_collection.insert_one(consent_data)
        
        return jsonify({
            'success': True,
            'consent_id': str(result.inserted_id),
            'message': 'Parent consent recorded successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get all recommendations
@app.route('/recommendations', methods=['GET'])
def get_recommendations():
    try:
        all_recommendations = list(recommendations_collection.find())
        recommendations_dict = {}
        for rec in all_recommendations:
            age_group = rec['age_group']
            recommendations_dict[age_group] = rec.get('books', [])
        return jsonify(recommendations_dict)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Update recommendations for an age group
@app.route('/recommendations/<age_group>', methods=['PUT'])
def update_recommendations(age_group):
    try:
        validate_age_group(age_group)
        books = request.json
        if not isinstance(books, list):
            return jsonify({
                'error': 'Invalid request format. Expected list of books.'
            }), 400
        # Validate each book has at least id, title, author
        for book in books:
            if not all(key in book and book[key] for key in ['id', 'title', 'author']):
                return jsonify({
                    'error': 'Each book must have id, title, and author.'
                }), 400
        # Store the book objects directly
        recommendations_collection.update_one(
            {'age_group': age_group},
            {'$set': {'books': books}},
            upsert=True
        )
        return jsonify({'success': True}), 200
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Get recommendations for specific age group
@app.route('/recommendations/<age_group>', methods=['GET'])
def get_age_group_recommendations(age_group):
    try:
        validate_age_group(age_group)
        rec = recommendations_collection.find_one({'age_group': age_group})
        if not rec:
            return jsonify([])
        # Return the stored book objects directly
        return jsonify(rec.get('books', []))
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Get all books
@app.route('/books', methods=['GET'])
def get_books():
    try:
        books = list(books_collection.find())
        formatted_books = [format_document(book) for book in books]
        return jsonify(formatted_books)
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Add a new book
@app.route('/books', methods=['POST'])
def add_book():
    try:
        book_data = request.json
        required_fields = ['title', 'author', 'genres', 'ageRange']
        
        # Validate required fields
        if not all(field in book_data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400
            
        # Validate age range
        age_range = book_data.get('ageRange', {})
        if not isinstance(age_range, dict) or 'min' not in age_range or 'max' not in age_range:
            return jsonify({
                'error': 'Invalid age range format'
            }), 400
            
        # Insert the book
        result = books_collection.insert_one(book_data)
        
        # Get the inserted book
        inserted_book = books_collection.find_one({'_id': result.inserted_id})
        return jsonify(format_document(inserted_book)), 201
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Update a book
@app.route('/books/<book_id>', methods=['PUT'])
def update_book(book_id):
    try:
        book_data = request.json
        
        # Validate book exists
        if not ObjectId.is_valid(book_id):
            return jsonify({'error': 'Invalid book ID'}), 400
            
        existing_book = books_collection.find_one({'_id': ObjectId(book_id)})
        if not existing_book:
            return jsonify({'error': 'Book not found'}), 404
            
        # Update the book
        books_collection.update_one(
            {'_id': ObjectId(book_id)},
            {'$set': book_data}
        )
        
        # Get the updated book
        updated_book = books_collection.find_one({'_id': ObjectId(book_id)})
        return jsonify(format_document(updated_book))
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Delete a book
@app.route('/books/<book_id>', methods=['DELETE'])
def delete_book(book_id):
    try:
        # Validate book exists
        if not ObjectId.is_valid(book_id):
            return jsonify({'error': 'Invalid book ID'}), 400
            
        result = books_collection.delete_one({'_id': ObjectId(book_id)})
        
        if result.deleted_count == 0:
            return jsonify({'error': 'Book not found'}), 404
            
        # Remove book from all age group recommendations
        recommendations_collection.update_many(
            {},
            {'$pull': {'books': book_id}}
        )
            
        return jsonify({'message': 'Book deleted successfully'}), 200
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Get all users
@app.route('/users', methods=['GET'])
def get_users():
    try:
        users = list(users_collection.find())
        formatted_users = [format_document(user) for user in users]
        return jsonify(formatted_users)
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Get user by ID
@app.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    try:
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        user = users_collection.find_one({'_id': ObjectId(user_id)})
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify(format_document(user))
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Create new user
@app.route('/users', methods=['POST'])
def create_user():
    try:
        user_data = request.json
        required_fields = ['name', 'email', 'phone', 'childName', 'childAge']
        
        # Validate required fields
        if not all(field in user_data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400
            
        # Add additional fields
        user_data['recommendations'] = []
        user_data['createdAt'] = datetime.utcnow()
        
        # Insert the user
        result = users_collection.insert_one(user_data)
        
        # Get the inserted user
        inserted_user = users_collection.find_one({'_id': result.inserted_id})
        return jsonify(format_document(inserted_user)), 201
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Update user
@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    try:
        user_data = request.json
        
        # Validate user exists
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        existing_user = users_collection.find_one({'_id': ObjectId(user_id)})
        if not existing_user:
            return jsonify({'error': 'User not found'}), 404
            
        # Update the user
        users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': user_data}
        )
        
        # Get the updated user
        updated_user = users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify(format_document(updated_user))
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Delete user
@app.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    try:
        # Validate user exists
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        result = users_collection.delete_one({'_id': ObjectId(user_id)})
        
        if result.deleted_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify({'message': 'User deleted successfully'}), 200
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Save recommendations for user
@app.route('/users/<user_id>/recommendations', methods=['POST'])
def add_recommendation(user_id):
    try:
        data = request.json
        book_id = data.get('book_id')
        
        if not book_id:
            return jsonify({'error': 'Book ID is required'}), 400
            
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Validate book exists
        book = books_collection.find_one({'_id': ObjectId(book_id)})
        if not book:
            return jsonify({'error': 'Book not found'}), 404
            
        # Add recommendation to user's list
        result = users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$addToSet': {'recommendations': book_id}}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found or book already recommended'}), 404
            
        # Get updated user
        updated_user = users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify(format_document(updated_user))
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Remove book recommendation from user
@app.route('/users/<user_id>/recommendations/<book_id>', methods=['DELETE'])
def remove_recommendation(user_id, book_id):
    try:
        if not ObjectId.is_valid(user_id) or not ObjectId.is_valid(book_id):
            return jsonify({'error': 'Invalid user ID or book ID'}), 400
            
        # Remove recommendation from user's list
        result = users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$pull': {'recommendations': book_id}}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found or book not in recommendations'}), 404
            
        # Get updated user
        updated_user = users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify(format_document(updated_user))
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Generate recommendation plan
@app.route('/recommendation-plan', methods=['POST'])
def generate_recommendation_plan():
    try:
        data = request.json
        
        # Check if this is a quiz user ID or direct data
        user_id = data.get('userId')
        if user_id and ObjectId.is_valid(user_id):
            # Get data from quiz user collection
            quiz_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
            if quiz_user:
                data = quiz_user
        
        required_fields = ['name', 'age', 'selectedGenres', 'selectedInterests', 
                         'nonFictionInterests', 'bookSeries', 'parentEmail', 'parentPhone']
        
        # Debug logging
        print("Received data:", data)
        
        # Validate required fields
        if not all(field in data for field in required_fields):
            missing_fields = [field for field in required_fields if field not in data]
            return jsonify({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400

        age = data['age']
        selected_genres = data['selectedGenres']
        
        # Initialize empty reading plan structure
        empty_reading_plan = [
            {
                'month': (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B'),
                'books': []
            } for i in range(3)
        ]
        
        # Validate data types
        if not isinstance(age, (int, float)) or age < 0:
            return jsonify({
                'error': 'Invalid age value'
            }), 400
            
        if not isinstance(selected_genres, list) or not selected_genres:
            return jsonify({
                'error': 'Selected genres must be a non-empty list'
            }), 400

        # No longer using SerpAPI - creating direct JustBookify search links
        
        # Helper to map age to age group
        def get_age_group(age):
            if age < 5:
                return 'Below 5'
            elif 5 <= age <= 8:
                return '6-8'
            elif 9 <= age <= 10:
                return '9-10'
            elif 11 <= age <= 12:
                return '11-12'
            else:
                return '13+'

        age_group = get_age_group(age)
        # Fetch recommendations for the user's age group
        rec_doc = recommendations_collection.find_one({'age_group': age_group})
        rec_books = rec_doc.get('books', []) if rec_doc else []
        rec_by_id = {b['id']: b for b in rec_books if 'id' in b}

        # Extract exclusion and prioritization lists from bookSeries, using full title/author
        book_series = data.get('bookSeries', [])
        exclude_series = []
        prioritize_series = []
        for s in book_series:
            rec = rec_by_id.get(s.get('seriesId'))
            if not rec:
                continue
            entry = f"{rec['title']} by {rec['author']}"
            if s.get('response') in ['didNotEnjoy', 'dontReadAnymore']:
                exclude_series.append(entry)
            if s.get('response') == 'love':
                prioritize_series.append(entry)
        
        # Debug logging for database query
        print(f"Querying books for age: {age}, genres: {selected_genres}")
        
        # Flexible book query to ensure enough books
        def get_flexible_books(age, selected_genres, min_books=15):
            # Debug logging
            print("Starting flexible book search...")
            
            # 1. Try strict filter
            books = list(books_collection.find({
                'genres': {'$in': selected_genres},
                'ageRange.min': {'$lte': age},
                'ageRange.max': {'$gte': age}
            }))
            print(f"Strict filter found {len(books)} books")
            
            if len(books) >= min_books:
                return books
                
            # 2. Relax genre filter
            books = list(books_collection.find({
                'ageRange.min': {'$lte': age},
                'ageRange.max': {'$gte': age}
            }))
            print(f"Age filter only found {len(books)} books")
            
            if len(books) >= min_books:
                return books
                
            # 3. Relax age filter
            books = list(books_collection.find())
            print(f"No filters found {len(books)} books")
            return books

        potential_books = get_flexible_books(age, selected_genres, min_books=15)
        print(f"Total potential books found: {len(potential_books)}")
        
        if not potential_books:
            print("No books found in database")
            return jsonify({
                'current': [],
                'future': empty_reading_plan,
                'recommendations': [],
                'error': 'No books found in database'
            })

        # Create a single context with all books, including description if available
        books_context = []
        for idx, book in enumerate(potential_books, 1):
            desc = book.get('description', 'No description available.')
            book_info = f"""Book {idx}:
Title: {book['title']}
Author: {book['author']}
Genres: {', '.join(book.get('genres', []))}
Description: {desc}
Age Range: {book.get('ageRange', {}).get('min', 0)}-{book.get('ageRange', {}).get('max', 99)}
---"""
            books_context.append(book_info)
        book_text = "\n".join(books_context)
        
        print("Preparing OpenAI request...")
        
        # Get recommendations from OpenAI with updated prompt
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": """You are an expert children's book recommendation system with a deep understanding of age-appropriate reading levels and child development stages. Your recommendations must follow these strict rules:

1. AGE APPROPRIATENESS (HIGHEST PRIORITY)
   - Only recommend books that are explicitly within the reader's age range
   - Consider reading difficulty level appropriate for the age
   - Factor in emotional maturity for content themes
   - Ensure vocabulary and sentence complexity match the age group

2. CONTENT MATCHING
   - Must strictly match the specified genres and interests
   - Content themes must be age-appropriate
   - Avoid repetitive recommendations or similar storylines
   - Balance fiction and non-fiction based on interests

3. DIVERSITY IN RECOMMENDATIONS
   - Never recommend the same book or series multiple times
   - Ensure variety in writing styles and complexity levels
   - Mix standalone books and series based on preference
   - Include different formats (chapter books, picture books, etc.) appropriate for age

4. QUANTITY REQUIREMENT (CRITICAL)
   - You MUST provide AT LEAST 15-20 unique book recommendations
   - This is essential for creating a 3-month reading plan with 4 books per month
   - If you cannot find enough books in the inventory, suggest additional books that would be perfect for this reader
   - Each recommendation should be distinct and valuable

5. QUALITY CONTROL
   - Each recommendation must have a unique justification
   - Verify age-appropriateness before including any book
   - Double-check for duplicate recommendations
   - Ensure recommendations are distinct and serve different reading needs"""},
                    {"role": "user", "content": f"""I need personalized book recommendations for a {age}-year-old reader with the following preferences:

GENRES THEY ENJOY: {', '.join(data['selectedGenres'])}
SPECIFIC INTERESTS: {', '.join(data['selectedInterests'])}
NON-FICTION INTERESTS: {', '.join(data['nonFictionInterests'])}
BOOK SERIES PREFERENCE: They {'enjoy' if data['bookSeries'] else 'do not prefer'} book series.

SERIES/BOOKS TO EXCLUDE: The reader has indicated they did NOT enjoy or do NOT want to read the following series/books:
{chr(10).join(exclude_series) if exclude_series else 'None'}
Do NOT recommend any books from these series/authors.

SERIES/BOOKS TO PRIORITIZE: The reader LOVES the following series/books:
{chr(10).join(prioritize_series) if prioritize_series else 'None'}
If possible, recommend similar books or series.

Available books in our inventory:

{book_text}

📚 CRITICAL: You MUST provide AT LEAST 15-20 unique book recommendations to ensure we can create a proper 3-month reading plan.

Please recommend unique books that PERFECTLY match these preferences, following these strict criteria:

AGE-SPECIFIC REQUIREMENTS FOR {age}-YEAR-OLD:
- Reading Level: Must be precisely matched to {age}-year-old reading capabilities
- Content Themes: Appropriate for {age}-year-old emotional and cognitive development
- Complexity: Vocabulary and sentence structure suitable for this age
- Format: Age-appropriate book format (picture books, chapter books, etc.)

RECOMMENDATION RULES:
1. QUANTITY FIRST:
   - You MUST provide at least 15-20 recommendations
   - If inventory is limited, suggest additional books that would be perfect for this reader
   - Each recommendation should be unique and valuable

2. NO DUPLICATES:
   - Never recommend the same book twice
   - Avoid multiple books from the same series unless explicitly requested
   - Ensure each recommendation serves a unique reading purpose

3. BALANCED SELECTION:
   - Mix of genres based on preferences
   - Balance between fiction and non-fiction
   - Variety of writing styles and formats
   - Different levels of reading challenge within age-appropriate range

4. STRICT MATCHING:
   - Must exactly match specified genres
   - Must align with listed interests
   - Must be at appropriate reading level
   - Must exclude all mentioned disliked books/series

5. VERIFICATION STEPS:
   - Double-check age appropriateness
   - Verify no duplicate recommendations
   - Ensure each book has unique value proposition
   - Confirm reading level matches age

IMPORTANT: If you cannot find enough books in the provided inventory, suggest additional books that would be perfect for this {age}-year-old reader based on their interests and preferences. The goal is to have enough diverse recommendations to create a rich reading experience.

✅ Return recommendations as a JSON array with this structure:
[
  {{
    "name": "Series/Author Name",
    "likely_score": X,  // Score 1-10 based on match with preferences
    "books": [
      "Book Title 1",
      "Book Title 2"
    ],
    "rationale": "Detailed explanation of why this matches their interests"
  }}
]

🎯 Sort recommendations by likelihood score (highest to lowest), only including books with a score of 7 or higher.
🎯 CRITICAL: Ensure you provide at least 15-20 recommendations to meet the quantity requirement.
"""}
                ],
                temperature=0.7,
                max_tokens=4000
            )
            
            print("OpenAI response received")
            print("Response content:", response.choices[0].message.content)
            
        except Exception as e:
            print(f"OpenAI API error: {str(e)}")
            return jsonify({
                'current': [],
                'future': empty_reading_plan,
                'recommendations': [],
                'error': f'OpenAI API error: {str(e)}'
            })

        # Parse OpenAI response and get series/author names
        try:
            raw_content = response.choices[0].message.content
            print("Parsing OpenAI response...")

            # Try to extract JSON array from the response using regex
            import re
            json_match = re.search(r'(\[\s*{.*}\s*\])', raw_content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Fallback: try to parse the whole content
                json_str = raw_content.strip()

            # Try to load JSON
            try:
                recommendations_json = json.loads(json_str)
            except Exception as e:
                print("Failed to parse JSON directly, trying to fix common issues...")
                # Try to fix common issues (e.g., trailing commas)
                json_str_fixed = re.sub(r',\s*}', '}', json_str)
                json_str_fixed = re.sub(r',\s*\]', ']', json_str_fixed)
                recommendations_json = json.loads(json_str_fixed)

            recommendations = []

            for rec in recommendations_json:
                series_name = rec.get('name', '')
                confidence_score = rec.get('likely_score', 8)
                rationale = rec.get('rationale', '')
                sample_books = [{"title": title, "author": series_name} for title in rec.get('books', [])]

                if series_name and sample_books:
                    # Create direct JustBookify search link - clean up the series name
                    clean_name = series_name.lower()
                    clean_name = clean_name.replace(" series name", "")
                    clean_name = clean_name.replace("series name", "")
                    clean_name = clean_name.replace(" series", "")
                    clean_name = clean_name.replace("series", "")
                    clean_name = " ".join(clean_name.split()).strip()
                    if not clean_name:
                        clean_name = series_name.lower().strip()
                    # Remove generic suffixes like 'comics', 'books', etc. from the search term
                    generic_suffixes = ['comics', 'books', 'series', 'collection', 'novels']
                    words = clean_name.split()
                    filtered_words = [w for w in words if w not in generic_suffixes]
                    filtered_name = " ".join(filtered_words).strip()
                    search_term = urllib.parse.quote(filtered_name if filtered_name else clean_name)
                    justbookify_link = f"https://www.justbookify.com/search?q={search_term}&options%5Bprefix%5D=last"

                    recommendations.append({
                        "name": series_name,
                        "justbookify_link": justbookify_link,
                        "rationale": rationale,
                        "confidence_score": confidence_score,
                        "sample_books": sample_books
                    })
                    print(f"Added recommendation for {series_name} with {len(sample_books)} books and link {justbookify_link}")

            # Sort recommendations by confidence score
            recommendations.sort(key=lambda x: x['confidence_score'], reverse=True)
            print(f"Total recommendations: {len(recommendations)}")

            # Create current month recommendations - ensure we have recommendations
            current_recs = []
            if recommendations:
                for rec in recommendations[:3]:  # Take top 3 recommendations
                    if rec.get('sample_books') and len(rec['sample_books']) > 0:
                        # Get the first book from this recommendation
                        book = rec['sample_books'][0]
                        current_recs.append({
                            'title': book['title'],
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']
                        })
                    else:
                        # Fallback if no sample books
                        current_recs.append({
                            'title': f"Book from {rec['name']}",
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']
                        })
            
            # Ensure we have at least some current recommendations
            if not current_recs and recommendations:
                print("Creating fallback current recommendations")
                for rec in recommendations[:3]:
                    current_recs.append({
                        'title': f"Book from {rec['name']}",
                        'author': rec['name'],
                        'explanation': rec['rationale'],
                        'justbookify_link': rec['justbookify_link']
                    })
            
            print(f"Current month recommendations created: {len(current_recs)} books")

            # Create future months recommendations - MUST have exactly 4 books per month
            future_recs = []
            
            print(f"=== CREATING FUTURE MONTHS WITH EXACTLY 4 BOOKS EACH ===")
            print(f"Total recommendations available: {len(recommendations)}")
            
            # CRITICAL: We MUST have 4 books per month, so we need 12 total
            total_books_needed = 12  # 3 months * 4 books per month
            
            # Use ALL recommendations for future months, not just recommendations[3:]
            # This ensures we have enough books to fill all months
            all_recommendations = recommendations.copy()
            
            # If we don't have enough recommendations, duplicate the ones we have
            if len(all_recommendations) < total_books_needed:
                print(f"WARNING: Only {len(all_recommendations)} recommendations available, need {total_books_needed}")
                print(f"This means the OpenAI prompt didn't generate enough recommendations.")
                print(f"Consider updating the prompt or increasing max_tokens.")
                
                # Create additional fallback recommendations if we have very few
                if len(all_recommendations) < 6:
                    print(f"Creating fallback recommendations to ensure variety...")
                    fallback_recommendations = [
                        {
                            'name': 'Additional Children\'s Books',
                            'confidence_score': 8,
                            'rationale': 'Additional reading material suitable for this age group',
                            'sample_books': [{'title': 'Additional Book 1'}],
                            'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                        },
                        {
                            'name': 'Popular Children\'s Authors',
                            'confidence_score': 7,
                            'rationale': 'Well-loved authors in children\'s literature',
                            'sample_books': [{'title': 'Additional Book 2'}],
                            'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                        },
                        {
                            'name': 'Educational Books',
                            'confidence_score': 7,
                            'rationale': 'Educational and engaging books for learning',
                            'sample_books': [{'title': 'Additional Book 3'}],
                            'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                        }
                    ]
                    all_recommendations.extend(fallback_recommendations)
                
                # Duplicate recommendations to fill the gap
                while len(all_recommendations) < total_books_needed:
                    all_recommendations.extend(all_recommendations[:total_books_needed - len(all_recommendations)])
            
            # Limit to exactly what we need
            all_recommendations = all_recommendations[:total_books_needed]
            
            print(f"Recommendations available for future months: {len(all_recommendations)}")
            print(f"Total books needed: {total_books_needed}")
            
            # Create 3 months with exactly 4 books each - NO EXCEPTIONS
            for month_index in range(3):
                month_books = []
                month_start = month_index * 4
                month_end = month_start + 4
                
                # Get 4 recommendations for this month
                month_recommendations = all_recommendations[month_start:month_end]
                
                print(f"Month {month_index + 1}: processing {len(month_recommendations)} recommendations")
                
                # Process each recommendation for this month
                for rec in month_recommendations:
                    if rec.get('sample_books') and len(rec['sample_books']) > 0:
                        # Get the first book from this recommendation
                        book = rec['sample_books'][0]
                        month_books.append({
                            'title': book['title'],
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']
                        })
                    else:
                        # Fallback if no sample books
                        month_books.append({
                            'title': f"Book from {rec['name']}",
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']
                        })
                
                # CRITICAL: Ensure exactly 4 books for this month - NO EXCEPTIONS
                while len(month_books) < 4:
                    if month_books:
                        # Duplicate the last book to fill the gap
                        last_book = month_books[-1].copy()
                        month_books.append(last_book)
                        print(f"Duplicated book to fill month {month_index + 1}")
                    else:
                        # Create a placeholder book if we have nothing
                        month_books.append({
                            'title': 'Additional Book Recommendation',
                            'author': 'Various Authors',
                            'explanation': 'Additional reading material for this month',
                            'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                        })
                        print(f"Created placeholder book for month {month_index + 1}")
                
                # Ensure we don't exceed 4 books
                month_books = month_books[:4]
                
                # FINAL CHECK: This month MUST have exactly 4 books
                if len(month_books) != 4:
                    print(f"CRITICAL ERROR: Month {month_index + 1} has {len(month_books)} books, MUST have 4!")
                    # Force exactly 4 books by duplicating or creating placeholders
                    while len(month_books) < 4:
                        if month_books:
                            last_book = month_books[-1].copy()
                            month_books.append(last_book)
                        else:
                            month_books.append({
                                'title': 'Book Recommendation',
                                'author': 'Various Authors',
                                'explanation': 'Reading material for this month',
                                'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                            })
                    month_books = month_books[:4]
                
                print(f"Month {month_index + 1}: {len(month_books)} books (MUST be 4)")
                
                future_recs.append({
                    'month': (datetime.now().replace(day=1) + timedelta(days=month_index*31)).strftime('%B'),
                    'books': month_books
                })
            
            # FINAL VALIDATION - ensure each month has exactly 4 books - NO EXCEPTIONS
            print("=== FINAL VALIDATION ===")
            for i, month_plan in enumerate(future_recs):
                book_count = len(month_plan['books'])
                if book_count != 4:
                    print(f"ERROR: Month {i+1} ({month_plan['month']}) has {book_count} books, MUST have 4!")
                    # Force exactly 4 books
                    while len(month_plan['books']) < 4:
                        if month_plan['books']:
                            last_book = month_plan['books'][-1].copy()
                            month_plan['books'].append(last_book)
                        else:
                            month_plan['books'].append({
                                'title': 'Book Recommendation',
                                'author': 'Various Authors',
                                'explanation': 'Reading material for this month',
                                'justbookify_link': 'https://www.justbookify.com/search?q=children+books'
                            })
                    month_plan['books'] = month_plan['books'][:4]
                    print(f"FIXED: Month {i+1} now has {len(month_plan['books'])} books")
                else:
                    print(f"✓ Month {i+1} ({month_plan['month']}): {len(month_plan['books'])} books - CORRECT")
            
            print("=== VALIDATION COMPLETE ===")
            for i, month_plan in enumerate(future_recs):
                print(f"Month {i+1} ({month_plan['month']}): {len(month_plan['books'])} books")

            print("Returning results...")
            print(f"Current recommendations: {len(current_recs)}")
            print(f"Future months: {len(future_recs)}")

            # Save user input and recommendations to database
            try:
                plan_data = {
                    'name': data['name'],
                    'age': data['age'],
                    'selectedGenres': data['selectedGenres'],
                    'selectedInterests': data['selectedInterests'],
                    'nonFictionInterests': data['nonFictionInterests'],
                    'bookSeries': data['bookSeries'],
                    'parentEmail': data['parentEmail'],
                    'parentPhone': data['parentPhone'],
                    'parentReading': data.get('parentReading'),
                    'topThreeGenres': data.get('topThreeGenres', []),
                    'fictionGenres': data.get('fictionGenres', []),
                    'nonFictionGenres': data.get('nonFictionGenres', []),
                    'additionalGenres': data.get('additionalGenres', []),
                    'fictionNonFictionRatio': data.get('fictionNonFictionRatio'),
                    'recommendations': recommendations,
                    'currentRecommendations': current_recs,
                    'futureRecommendations': future_recs,
                    'createdAt': datetime.utcnow(),
                    'updatedAt': datetime.utcnow(),
                    'status': 'active'
                }
                
                # Also capture any additional fields that might be present
                additional_fields = [
                    'fictionNonFictionRatio', 'topThreeGenres', 'fictionGenres', 
                    'nonFictionGenres', 'additionalGenres', 'selectedGenres',
                    'selectedInterests', 'nonFictionInterests', 'bookSeries'
                ]
                
                for field in additional_fields:
                    if field in data and data[field] is not None:
                        plan_data[field] = data[field]
                
                # Insert the recommendation plan
                result = recommendation_plans_collection.insert_one(plan_data)
                plan_id = str(result.inserted_id)
                
                print(f"Saved recommendation plan with ID: {plan_id}")
                print(f"Saved data fields: {list(plan_data.keys())}")
                
                return jsonify({
                    'current': current_recs,
                    'future': future_recs,
                    'recommendations': recommendations,
                    'planId': plan_id,
                    'message': 'Recommendation plan generated and saved successfully'
                })
                
            except Exception as save_error:
                print(f"Error saving recommendation plan: {str(save_error)}")
                # Return results even if save fails
                return jsonify({
                    'current': current_recs,
                    'future': future_recs,
                    'recommendations': recommendations,
                    'error': f'Recommendations generated but failed to save: {str(save_error)}'
                })

        except Exception as e:
            print(f"Error processing recommendations: {str(e)}")
            import traceback
            print("Traceback:", traceback.format_exc())
            return jsonify({
                'current': [],
                'future': empty_reading_plan,
                'recommendations': [],
                'error': f'Error processing recommendations: {str(e)}'
            })

    except Exception as e:
        print(f"Error in generate_recommendation_plan: {str(e)}")
        import traceback
        print("Traceback:", traceback.format_exc())
        return jsonify({
            'current': [],
            'future': empty_reading_plan,
            'recommendations': [],
            'error': f'Error in generate_recommendation_plan: {str(e)}'
        })

# Send recommendations via email
@app.route('/send-recommendations/email', methods=['POST'])
def send_email_recommendations():
    try:
        data = request.json
        required_fields = ['email', 'recommendations', 'readingPlan', 'name']
        
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400

        # Create email content
        html_content = f"""
        <h2>Hello {data['name']}'s Parent!</h2>
        <p>Here are the book recommendations for {data['name']}:</p>
        
        <h3>Current Recommendations:</h3>
        <ul>
        {''.join(f'<li><strong>{book["title"]}</strong> by {book["author"]}<br/><em>{book.get("explanation", "")}</em></li>' for book in data['recommendations'])}
        </ul>

        <h3>Recommended Series and Authors:</h3>
        {''.join(f'''
        <div style="margin-bottom: 20px;">
            <h4><a href="{rec['justbookify_link']}" target="_blank">{rec['name']}</a> (Confidence Score: {rec.get('confidence_score', 'N/A')}/10)</h4>
            <p><em>{rec.get('rationale', '')}</em></p>
            <ul>
            {''.join(f'<li><strong>{book["title"]}</strong> by {book["author"]}</li>' for book in rec.get('sample_books', []))}
            </ul>
        </div>
        ''' for rec in data.get('seriesRecommendations', []))}

        <h3>3-Month Reading Plan:</h3>
        {''.join(f'''
        <div style="margin-bottom: 20px;">
            <h4>{month['month']}</h4>
            <ul>
            {''.join(f'<li><strong>{book["title"]}</strong> by {book["author"]}<br/><em>{book.get("explanation", "")}</em></li>' for book in month['books'])}
            </ul>
        </div>
        ''' for month in data['readingPlan'])}
        
        <p>Happy Reading!</p>
        """

        # Create message
        message = Mail(
            from_email=FROM_EMAIL,
            to_emails=data['email'],
            subject=f"Book Recommendations for {data['name']}",
            html_content=html_content
        )

        # Send email
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        
        if response.status_code >= 200 and response.status_code < 300:
            return jsonify({
                'message': 'Recommendations sent successfully to email'
            })
        else:
            return jsonify({
                'error': 'Failed to send email'
            }), 500

    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Send recommendations via WhatsApp using Facebook Graph API
@app.route('/send-recommendations/whatsapp', methods=['POST'])
def send_whatsapp_recommendations():
    try:
        from whatsapp_api import create_whatsapp_client, format_book_recommendations_messages
        
        data = request.json
        required_fields = ['phone', 'name', 'recommendations', 'current', 'future']
        
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400

        # Create WhatsApp client
        whatsapp_client = create_whatsapp_client()
        if not whatsapp_client:
            return jsonify({
                'error': 'WhatsApp service not configured. Missing Facebook API credentials.'
            }), 500

        # Format messages
        messages = format_book_recommendations_messages(data)
        
        if not messages:
            return jsonify({
                'error': 'No messages to send'
            }), 400

        # Send messages
        result = whatsapp_client.send_multiple_messages(data['phone'], messages)
        
        return jsonify({
            'message': f"Successfully sent {result['successful_messages']} out of {result['total_messages']} messages",
            **result
        })

    except Exception as e:
        print(f"General error in WhatsApp sending: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500

# Test WhatsApp with hello_world template
@app.route('/test-whatsapp', methods=['POST'])
def test_whatsapp():
    try:
        from whatsapp_api import create_whatsapp_client
        
        data = request.json
        phone = data.get('phone')
        
        if not phone:
            return jsonify({
                'error': 'Phone number is required'
            }), 400

        # Create WhatsApp client
        whatsapp_client = create_whatsapp_client()
        if not whatsapp_client:
            return jsonify({
                'error': 'WhatsApp service not configured. Missing Facebook API credentials.'
            }), 500

        # Format phone number and send hello_world template
        formatted_phone = whatsapp_client.format_phone_number(phone)
        response = whatsapp_client.send_template_message(formatted_phone, "hello_world")
        
        if response.status_code == 200:
            response_data = response.json()
            return jsonify({
                'message': 'Hello world template sent successfully',
                'recipient_phone': formatted_phone,
                'response_data': response_data
            })
        else:
            return jsonify({
                'error': f'Failed to send message: {response.status_code} - {response.text}',
                'recipient_phone': formatted_phone
            }), 500

    except Exception as e:
        print(f"Error in test WhatsApp: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500

# Helper function to process reader types
def process_reader_types(types_str):
    if not types_str or pd.isna(types_str):
        return []
    types = [t.strip() for t in str(types_str).split(';')]
    age_ranges = {
        'early-readers': {'min': 3, 'max': 5},
        'emerging-readers': {'min': 6, 'max': 8},
        'junior-readers': {'min': 9, 'max': 10},
        'preteen-readers': {'min': 11, 'max': 12},
        'teen-readers': {'min': 13, 'max': 18},
    }
    
    min_age = float('inf')
    max_age = 0
    for reader_type in types:
        if reader_type in age_ranges:
            range_info = age_ranges[reader_type]
            min_age = min(min_age, range_info['min'])
            max_age = max(max_age, range_info['max'])
    
    # If no valid reader types found, use default range
    if min_age == float('inf'):
        return {'min': 4, 'max': 14}
    
    return {'min': min_age, 'max': max_age}

# Helper function to process genres
def process_genres(genres_str):
    if not genres_str or pd.isna(genres_str):
        return []
    return [genre.strip() for genre in str(genres_str).split(';')]

# Import books from CSV
@app.route('/import-books', methods=['POST'])
def import_books():
    try:
        if 'file' not in request.files:
            return jsonify({
                'error': 'No file uploaded'
            }), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'error': 'No file selected'
            }), 400
            
        # Check file extension
        if not file.filename.endswith('.csv'):
            return jsonify({
                'error': 'Invalid file format. Please upload a CSV file.'
            }), 400
            
        # Read CSV file
        try:
            df = pd.read_csv(file)
        except Exception as e:
            return jsonify({
                'error': f'Error reading CSV file: {str(e)}'
            }), 400
            
        # Check required columns
        required_columns = ['Title', 'Vendor', 'Type', 'Tags', 'Image Src', 'Genre (product.metafields.shopify.genre)']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({
                'error': f'Missing required columns: {", ".join(missing_columns)}'
            }), 400
            
        # Process and insert books
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in df.iterrows():
            try:
                # Process book data
                book_data = {
                    'title': row['Title'] if not pd.isna(row['Title']) else '',
                    'author': row['Vendor'] if not pd.isna(row['Vendor']) else 'Unknown',
                    'ageRange': process_reader_types(row['Type']),
                    'tags': process_genres(row['Tags']),
                    'image': row['Image Src'] if not pd.isna(row['Image Src']) else None,
                    'genres': process_genres(row['Genre (product.metafields.shopify.genre)']),
                    'importedAt': datetime.utcnow()
                }
                
                # Skip if title is empty
                if not book_data['title']:
                    error_count += 1
                    errors.append(f'Row {index + 2}: Empty title')
                    continue
                
                # Check if book already exists
                existing_book = books_collection.find_one({
                    'title': book_data['title'],
                    'author': book_data['author']
                })
                
                if existing_book:
                    # Update existing book
                    books_collection.update_one(
                        {'_id': existing_book['_id']},
                        {'$set': book_data}
                    )
                else:
                    # Insert new book
                    books_collection.insert_one(book_data)
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f'Row {index + 2}: {str(e)}')
                
        return jsonify({
            'message': 'Import completed',
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors[:100]  # Limit number of errors in response
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# ==================== QUIZ API ENDPOINTS ====================

# Save parent consent and create initial user
@app.route('/quiz/parent-consent', methods=['POST'])
def save_parent_consent():
    try:
        data = request.json
        email = data.get('email')
        phone = data.get('phone')
        timestamp = data.get('timestamp')
        
        if not all([email, phone]):
            return jsonify({
                'success': False,
                'error': 'Email and phone are required'
            }), 400
            
        # Create initial quiz user record
        user_data = {
            'parentEmail': email,
            'parentPhone': phone,
            'consentTimestamp': timestamp or datetime.utcnow().isoformat(),
            'createdAt': datetime.utcnow(),
            'status': 'consent_given',
            'quizProgress': {
                'parentConsent': True,
                'basicInfo': False,
                'parentReading': False,
                'genres': False,
                'interests': False,
                'bookSeries': False,
                'completed': False
            }
        }
        
        result = quiz_users_collection.insert_one(user_data)
        
        return jsonify({
            'success': True,
            'userId': str(result.inserted_id),
            'message': 'Parent consent saved and user created successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update user basic info
@app.route('/quiz/users/<user_id>/basic-info', methods=['PUT'])
def update_user_basic_info(user_id):
    try:
        data = request.json
        name = data.get('name')
        age = data.get('age')
        
        if not all([name, age]):
            return jsonify({
                'success': False,
                'error': 'Name and age are required'
            }), 400
            
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Update user with basic info
        update_data = {
            'name': name,
            'age': age,
            'updatedAt': datetime.utcnow(),
            'quizProgress.basicInfo': True
        }
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update parent reading habits
@app.route('/quiz/users/<user_id>/parent-reading', methods=['PUT'])
def update_parent_reading(user_id):
    try:
        data = request.json
        parent_reading = data.get('parentReading')
        
        if not parent_reading:
            return jsonify({
                'success': False,
                'error': 'Parent reading habits are required'
            }), 400
            
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Update user with parent reading info
        update_data = {
            'parentReading': parent_reading,
            'updatedAt': datetime.utcnow(),
            'quizProgress.parentReading': True
        }
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update genre preferences
@app.route('/quiz/users/<user_id>/genres', methods=['PUT'])
def update_genre_preferences(user_id):
    try:
        data = request.json
        
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Prepare update data
        update_data = {
            'updatedAt': datetime.utcnow(),
            'quizProgress.genres': True
        }
        
        # Add genre-related fields if provided
        genre_fields = [
            'selectedGenres', 'topThreeGenres', 'fictionGenres', 
            'nonFictionGenres', 'additionalGenres', 'fictionNonFictionRatio'
        ]
        
        for field in genre_fields:
            if field in data:
                update_data[field] = data[field]
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update interests
@app.route('/quiz/users/<user_id>/interests', methods=['PUT'])
def update_interests(user_id):
    try:
        data = request.json
        selected_interests = data.get('selectedInterests', [])
        non_fiction_interests = data.get('nonFictionInterests', [])
        
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Update user with interests
        update_data = {
            'selectedInterests': selected_interests,
            'nonFictionInterests': non_fiction_interests,
            'updatedAt': datetime.utcnow(),
            'quizProgress.interests': True
        }
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update book series responses
@app.route('/quiz/users/<user_id>/book-series', methods=['PUT'])
def update_book_series_responses(user_id):
    try:
        data = request.json
        book_series = data.get('bookSeries', [])
        
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Update user with book series responses
        update_data = {
            'bookSeries': book_series,
            'updatedAt': datetime.utcnow(),
            'quizProgress.bookSeries': True
        }
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Save individual book series response
@app.route('/quiz/users/<user_id>/book-series/response', methods=['POST'])
def save_book_series_response(user_id):
    try:
        data = request.json
        series_id = data.get('seriesId')
        has_read = data.get('hasRead')
        response_value = data.get('response')
        timestamp = data.get('timestamp')
        
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        if series_id is None or has_read is None:
            return jsonify({
                'error': 'Series ID and hasRead are required'
            }), 400
            
        # Create response record
        response_data = {
            'userId': user_id,
            'seriesId': series_id,
            'hasRead': has_read,
            'response': response_value,
            'timestamp': timestamp or datetime.utcnow().isoformat(),
            'createdAt': datetime.utcnow()
        }
        
        # Save individual response
        quiz_responses_collection.insert_one(response_data)
        
        # Update user's book series array
        user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        book_series = user.get('bookSeries', [])
        
        # Update or add the response
        found = False
        for i, series in enumerate(book_series):
            if series.get('seriesId') == series_id:
                book_series[i] = {
                    'seriesId': series_id,
                    'hasRead': has_read,
                    'response': response_value
                }
                found = True
                break
        
        if not found:
            book_series.append({
                'seriesId': series_id,
                'hasRead': has_read,
                'response': response_value
            })
        
        # Update user
        quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': {
                'bookSeries': book_series,
                'updatedAt': datetime.utcnow()
            }}
        )
        
        return jsonify({
            'success': True,
            'message': 'Book series response saved successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Complete quiz
@app.route('/quiz/complete', methods=['POST'])
def complete_quiz():
    try:
        data = request.json
        user_id = data.get('userId')
        
        if not user_id or not ObjectId.is_valid(user_id):
            return jsonify({
                'success': False,
                'error': 'Valid user ID is required'
            }), 400
            
        # Update user with all final data and mark as completed
        update_data = {
            'updatedAt': datetime.utcnow(),
            'completedAt': data.get('completedAt', datetime.utcnow().isoformat()),
            'quizProgress.completed': True,
            'status': 'completed'
        }
        
        # Add all quiz data fields - expanded to capture everything
        quiz_fields = [
            'name', 'age', 'parentEmail', 'parentPhone', 'parentReading',
            'selectedGenres', 'selectedInterests', 'nonFictionInterests',
            'topThreeGenres', 'fictionGenres', 'nonFictionGenres',
            'additionalGenres', 'fictionNonFictionRatio', 'bookSeries',
            'fictionNonFictionRatio', 'topThreeGenres', 'fictionGenres', 
            'nonFictionGenres', 'additionalGenres'
        ]
        
        for field in quiz_fields:
            if field in data and data[field] is not None:
                update_data[field] = data[field]
        
        # Also save any additional fields that might be present
        for key, value in data.items():
            if key not in ['userId', 'completedAt'] and value is not None:
                update_data[key] = value
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({
                'success': False,
                'error': 'User not found'
            }), 404
            
        # Get completed user data
        completed_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        
        return jsonify({
            'success': True,
            'message': 'Quiz completed successfully',
            'user': format_document(completed_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update user data incrementally
@app.route('/quiz/users/<user_id>', methods=['PUT'])
def update_quiz_user_data(user_id):
    try:
        data = request.json
        
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Remove userId from data if present
        data.pop('userId', None)
        
        # Add update timestamp
        data['updatedAt'] = datetime.utcnow()
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        # Get updated user
        updated_user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        return jsonify({
            'success': True,
            'user': format_document(updated_user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get user data
@app.route('/quiz/users/<user_id>', methods=['GET'])
def get_quiz_user_data(user_id):
    try:
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        user = quiz_users_collection.find_one({'_id': ObjectId(user_id)})
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify({
            'success': True,
            'user': format_document(user)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Save recommendations for quiz user
@app.route('/quiz/users/<user_id>/recommendations', methods=['POST'])
def save_quiz_user_recommendations(user_id):
    try:
        data = request.json
        recommendations = data.get('recommendations')
        
        if not recommendations:
            return jsonify({
                'success': False,
                'error': 'Recommendations are required'
            }), 400
            
        if not ObjectId.is_valid(user_id):
            return jsonify({'error': 'Invalid user ID'}), 400
            
        # Update user with recommendations
        update_data = {
            'recommendations': recommendations,
            'recommendationsGeneratedAt': data.get('generatedAt', datetime.utcnow().isoformat()),
            'updatedAt': datetime.utcnow()
        }
        
        result = quiz_users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_data}
        )
        
        if result.modified_count == 0:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify({
            'success': True,
            'message': 'Recommendations saved successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get all quiz users (for admin purposes)
@app.route('/quiz/users', methods=['GET'])
def get_all_quiz_users():
    try:
        users = list(quiz_users_collection.find())
        formatted_users = [format_document(user) for user in users]
        return jsonify({
            'success': True,
            'users': formatted_users
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
# ==================== END QUIZ API ENDPOINTS ====================

# ==================== RECOMMENDATION PLANS API ENDPOINTS ====================

# Get all recommendation plans
@app.route('/recommendation-plans', methods=['GET'])
def get_all_recommendation_plans():
    try:
        # Get query parameters for filtering
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 10))
        status = request.args.get('status', 'active')
        email = request.args.get('email')
        
        # Build filter
        filter_query = {}
        if status:
            filter_query['status'] = status
        if email:
            filter_query['parentEmail'] = {'$regex': email, '$options': 'i'}
        
        # Calculate skip value for pagination
        skip = (page - 1) * limit
        
        # Get total count
        total_count = recommendation_plans_collection.count_documents(filter_query)
        
        # Get plans with pagination
        plans = list(recommendation_plans_collection.find(filter_query)
                    .sort('createdAt', -1)
                    .skip(skip)
                    .limit(limit))
        
        # Format plans
        formatted_plans = [format_document(plan) for plan in plans]
        
        return jsonify({
            'success': True,
            'plans': formatted_plans,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'pages': (total_count + limit - 1) // limit
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get single recommendation plan by ID
@app.route('/recommendation-plans/<plan_id>', methods=['GET'])
def get_recommendation_plan(plan_id):
    try:
        if not ObjectId.is_valid(plan_id):
            return jsonify({
                'success': False,
                'error': 'Invalid plan ID'
            }), 400
        
        plan = recommendation_plans_collection.find_one({'_id': ObjectId(plan_id)})
        
        if not plan:
            return jsonify({
                'success': False,
                'error': 'Recommendation plan not found'
            }), 404
        
        return jsonify({
            'success': True,
            'plan': format_document(plan)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Update recommendation plan
@app.route('/recommendation-plans/<plan_id>', methods=['PUT'])
def update_recommendation_plan(plan_id):
    try:
        if not ObjectId.is_valid(plan_id):
            return jsonify({
                'success': False,
                'error': 'Invalid plan ID'
            }), 400
        
        data = request.json
        
        # Check if plan exists
        existing_plan = recommendation_plans_collection.find_one({'_id': ObjectId(plan_id)})
        if not existing_plan:
            return jsonify({
                'success': False,
                'error': 'Recommendation plan not found'
            }), 404
        
        # Add update timestamp
        data['updatedAt'] = datetime.utcnow()
        
        # Update the plan
        result = recommendation_plans_collection.update_one(
            {'_id': ObjectId(plan_id)},
            {'$set': data}
        )
        
        if result.modified_count == 0:
            return jsonify({
                'success': False,
                'error': 'No changes made to the plan'
            }), 400
        
        # Get updated plan
        updated_plan = recommendation_plans_collection.find_one({'_id': ObjectId(plan_id)})
        
        return jsonify({
            'success': True,
            'message': 'Recommendation plan updated successfully',
            'plan': format_document(updated_plan)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Delete single recommendation plan
@app.route('/recommendation-plans/<plan_id>', methods=['DELETE'])
def delete_recommendation_plan(plan_id):
    try:
        if not ObjectId.is_valid(plan_id):
            return jsonify({
                'success': False,
                'error': 'Invalid plan ID'
            }), 400
        
        # Check if plan exists
        existing_plan = recommendation_plans_collection.find_one({'_id': ObjectId(plan_id)})
        if not existing_plan:
            return jsonify({
                'success': False,
                'error': 'Recommendation plan not found'
            }), 404
        
        # Delete the plan
        result = recommendation_plans_collection.delete_one({'_id': ObjectId(plan_id)})
        
        if result.deleted_count == 0:
            return jsonify({
                'success': False,
                'error': 'Failed to delete recommendation plan'
            }), 500
        
        return jsonify({
            'success': True,
            'message': 'Recommendation plan deleted successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Delete all recommendation plans
@app.route('/recommendation-plans', methods=['DELETE'])
def delete_all_recommendation_plans():
    try:
        # Get query parameters for filtering what to delete
        status = request.args.get('status')
        email = request.args.get('email')
        
        # Build filter
        filter_query = {}
        if status:
            filter_query['status'] = status
        if email:
            filter_query['parentEmail'] = {'$regex': email, '$options': 'i'}
        
        # If no filter is provided, delete all plans
        if not filter_query:
            # For safety, require confirmation
            confirm = request.args.get('confirm')
            if confirm != 'true':
                return jsonify({
                    'success': False,
                    'error': 'To delete all plans, add ?confirm=true to the URL'
                }), 400
        
        # Get count before deletion
        count_before = recommendation_plans_collection.count_documents(filter_query)
        
        # Delete plans
        result = recommendation_plans_collection.delete_many(filter_query)
        
        return jsonify({
            'success': True,
            'message': f'Successfully deleted {result.deleted_count} recommendation plans',
            'deletedCount': result.deleted_count,
            'totalBefore': count_before
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get recommendation plans by email
@app.route('/recommendation-plans/email/<email>', methods=['GET'])
def get_recommendation_plans_by_email(email):
    try:
        # URL decode the email
        decoded_email = urllib.parse.unquote(email)
        
        # Get plans for the email
        plans = list(recommendation_plans_collection.find({
            'parentEmail': {'$regex': decoded_email, '$options': 'i'}
        }).sort('createdAt', -1))
        
        formatted_plans = [format_document(plan) for plan in plans]
        
        return jsonify({
            'success': True,
            'plans': formatted_plans,
            'count': len(formatted_plans)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Get recommendation plans statistics
@app.route('/recommendation-plans/stats', methods=['GET'])
def get_recommendation_plans_stats():
    try:
        # Get total count
        total_plans = recommendation_plans_collection.count_documents({})
        
        # Get count by status
        active_plans = recommendation_plans_collection.count_documents({'status': 'active'})
        inactive_plans = recommendation_plans_collection.count_documents({'status': 'inactive'})
        
        # Get count by age groups
        age_stats = list(recommendation_plans_collection.aggregate([
            {
                '$group': {
                    '_id': {
                        '$cond': {
                            'if': {'$lt': ['$age', 5]},
                            'then': 'Below 5',
                            'else': {
                                '$cond': {
                                    'if': {'$lte': ['$age', 8]},
                                    'then': '6-8',
                                    'else': {
                                        '$cond': {
                                            'if': {'$lte': ['$age', 10]},
                                            'then': '9-10',
                                            'else': {
                                                '$cond': {
                                                    'if': {'$lte': ['$age', 12]},
                                                    'then': '11-12',
                                                    'else': '13+'
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    'count': {'$sum': 1}
                }
            },
            {'$sort': {'_id': 1}}
        ]))
        
        # Get recent plans (last 7 days)
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        recent_plans = recommendation_plans_collection.count_documents({
            'createdAt': {'$gte': seven_days_ago}
        })
        
        return jsonify({
            'success': True,
            'stats': {
                'total': total_plans,
                'active': active_plans,
                'inactive': inactive_plans,
                'recent': recent_plans,
                'ageGroups': age_stats
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== END RECOMMENDATION PLANS API ENDPOINTS ====================

# ==================== EXCEL EXPORT ENDPOINTS ====================

@app.route('/export/excel', methods=['GET'])
def export_to_excel():
    try:
        # Get query parameters for filtering
        status = request.args.get('status', 'all')
        email = request.args.get('email')
        
        # Build filter
        filter_query = {}
        if status and status != 'all':
            filter_query['status'] = status
        if email:
            filter_query['parentEmail'] = {'$regex': email, '$options': 'i'}
        
        # Get all recommendation plans
        plans = list(recommendation_plans_collection.find(filter_query).sort('createdAt', -1))
        
        # Get all quiz users
        quiz_users = list(quiz_users_collection.find(filter_query).sort('createdAt', -1))
        
        # Create Excel file with multiple sheets
        with pd.ExcelWriter('book_recommendations_export.xlsx', engine='openpyxl') as writer:
            
            # Sheet 1: Complete User Data
            user_data = []
            for plan in plans:
                user_data.append({
                    'User ID': str(plan.get('_id', '')),
                    'Name': plan.get('name', ''),
                    'Age': plan.get('age', ''),
                    'Parent Email': plan.get('parentEmail', ''),
                    'Parent Phone': plan.get('parentPhone', ''),
                    'Parent Reading Habits': plan.get('parentReading', ''),
                    'Selected Genres': ', '.join(plan.get('selectedGenres', [])),
                    'Selected Interests': ', '.join(plan.get('selectedInterests', [])),
                    'Non-Fiction Interests': ', '.join(plan.get('nonFictionInterests', [])),
                    'Top Three Genres': ', '.join(plan.get('topThreeGenres', [])),
                    'Fiction Genres': ', '.join(plan.get('fictionGenres', [])),
                    'Non-Fiction Genres': ', '.join(plan.get('nonFictionGenres', [])),
                    'Additional Genres': ', '.join(plan.get('additionalGenres', [])),
                    'Fiction/Non-Fiction Ratio': plan.get('fictionNonFictionRatio', ''),
                    'Book Series Preferences': str(plan.get('bookSeries', [])),
                    'Status': plan.get('status', ''),
                    'Created Date': plan.get('createdAt', ''),
                    'Updated Date': plan.get('updatedAt', '')
                })
            
            # Add quiz users that don't have recommendation plans
            for user in quiz_users:
                # Check if this user already has a recommendation plan
                existing_plan = next((p for p in plans if p.get('parentEmail') == user.get('parentEmail')), None)
                if not existing_plan:
                    user_data.append({
                        'User ID': str(user.get('_id', '')),
                        'Name': user.get('name', ''),
                        'Age': user.get('age', ''),
                        'Parent Email': user.get('parentEmail', ''),
                        'Parent Phone': user.get('parentPhone', ''),
                        'Parent Reading Habits': user.get('parentReading', ''),
                        'Selected Genres': ', '.join(user.get('selectedGenres', [])),
                        'Selected Interests': ', '.join(user.get('selectedInterests', [])),
                        'Non-Fiction Interests': ', '.join(user.get('nonFictionInterests', [])),
                        'Top Three Genres': ', '.join(user.get('topThreeGenres', [])),
                        'Fiction Genres': ', '.join(user.get('fictionGenres', [])),
                        'Non-Fiction Genres': ', '.join(user.get('nonFictionGenres', [])),
                        'Additional Genres': ', '.join(user.get('additionalGenres', [])),
                        'Fiction/Non-Fiction Ratio': user.get('fictionNonFictionRatio', ''),
                        'Book Series Preferences': str(user.get('bookSeries', [])),
                        'Status': user.get('status', ''),
                        'Created Date': user.get('createdAt', ''),
                        'Updated Date': user.get('updatedAt', '')
                    })
            
            df_users = pd.DataFrame(user_data)
            df_users.to_excel(writer, sheet_name='User_Data', index=False)
            
            # Sheet 2: Current Recommendations
            current_recs_data = []
            for plan in plans:
                if plan.get('currentRecommendations'):
                    for rec in plan['currentRecommendations']:
                        current_recs_data.append({
                            'User Name': plan.get('name', ''),
                            'User Email': plan.get('parentEmail', ''),
                            'Book Title': rec.get('title', ''),
                            'Author/Series': rec.get('author', ''),
                            'Explanation': rec.get('explanation', ''),
                            'JustBookify Link': rec.get('justbookify_link', ''),
                            'Recommendation Type': 'Current Month'
                        })
            
            df_current = pd.DataFrame(current_recs_data)
            if not df_current.empty:
                df_current.to_excel(writer, sheet_name='Current_Recommendations', index=False)
            
            # Sheet 3: Monthly Reading Plans
            monthly_plans_data = []
            for plan in plans:
                if plan.get('futureRecommendations'):
                    for month_plan in plan['futureRecommendations']:
                        month_name = month_plan.get('month', '')
                        for book in month_plan.get('books', []):
                            monthly_plans_data.append({
                                'User Name': plan.get('name', ''),
                                'User Email': plan.get('parentEmail', ''),
                                'Month': month_name,
                                'Book Title': book.get('title', ''),
                                'Author/Series': book.get('author', ''),
                                'Explanation': book.get('explanation', ''),
                                'JustBookify Link': book.get('justbookify_link', ''),
                                'Recommendation Type': 'Monthly Plan'
                            })
            
            df_monthly = pd.DataFrame(monthly_plans_data)
            if not df_monthly.empty:
                df_monthly.to_excel(writer, sheet_name='Monthly_Reading_Plans', index=False)
            
            # Sheet 4: Series/Author Recommendations
            series_recs_data = []
            for plan in plans:
                if plan.get('recommendations'):
                    for rec in plan['recommendations']:
                        series_recs_data.append({
                            'User Name': plan.get('name', ''),
                            'User Email': plan.get('parentEmail', ''),
                            'Series/Author Name': rec.get('name', ''),
                            'Confidence Score': rec.get('confidence_score', ''),
                            'Rationale': rec.get('rationale', ''),
                            'JustBookify Link': rec.get('justbookify_link', ''),
                            'Sample Books': ', '.join([book.get('title', '') for book in rec.get('sample_books', [])])
                        })
            
            df_series = pd.DataFrame(series_recs_data)
            if not df_series.empty:
                df_series.to_excel(writer, sheet_name='Series_Recommendations', index=False)
            
            # Sheet 5: Book Series Responses
            # This sheet shows user responses to book series questions during the quiz
            # Series Name: The actual name of the book series/author (mapped from seriesId)
            # Series ID: The original identifier used during the quiz process
            book_series_data = []
            for plan in plans:
                if plan.get('bookSeries'):
                    # Create a mapping from seriesId to series name
                    series_id_to_name = {}
                    
                    # If we have recommendations, use them to map seriesId to series name
                    if plan.get('recommendations'):
                        for i, rec in enumerate(plan['recommendations']):
                            # Map by index (most likely case)
                            series_id_to_name[str(i)] = rec.get('name', 'Unknown Series')
                            # Also map by any other potential ID fields
                            if 'id' in rec:
                                series_id_to_name[rec['id']] = rec.get('name', 'Unknown Series')
                            # Map by confidence score if available
                            if 'confidence_score' in rec:
                                series_id_to_name[str(rec['confidence_score'])] = rec.get('name', 'Unknown Series')
                    
                    for series in plan['bookSeries']:
                        series_id = series.get('seriesId', '')
                        # Try to get the series name from the mapping
                        # The seriesId is typically the index in the recommendations array
                        series_name = series_id_to_name.get(str(series_id), f'Series ID: {series_id}')
                        

                        
                        book_series_data.append({
                            'User Name': plan.get('name', ''),
                            'User Email': plan.get('parentEmail', ''),
                            'Series Name': series_name,
                            'Series ID': series_id,
                            'Mapping Status': 'Mapped' if series_name != f'Series ID: {series_id}' else 'Not Mapped',
                            'Has Read': series.get('hasRead', ''),
                            'Response': series.get('response', ''),
                            'Timestamp': series.get('timestamp', '')
                        })
            
            # Also add book series responses from quiz users who don't have recommendation plans yet
            for user in quiz_users:
                if user.get('bookSeries'):
                    # Check if this user already has a recommendation plan
                    existing_plan = next((p for p in plans if p.get('parentEmail') == user.get('parentEmail')), None)
                    if not existing_plan:
                        for series in user['bookSeries']:
                            series_id = series.get('seriesId', '')
                            book_series_data.append({
                                'User Name': user.get('name', ''),
                                'User Email': user.get('parentEmail', ''),
                                'Series Name': f'Series ID: {series_id} (Quiz completed, recommendations pending)',
                                'Series ID': series_id,
                                'Mapping Status': 'Pending Recommendations',
                                'Has Read': series.get('hasRead', ''),
                                'Response': series.get('response', ''),
                                'Timestamp': series.get('timestamp', '')
                            })
            
            df_book_series = pd.DataFrame(book_series_data)
            if not df_book_series.empty:
                df_book_series.to_excel(writer, sheet_name='Book_Series_Responses', index=False)
            
            # Sheet 6: Summary Statistics
            summary_data = []
            total_users = len(user_data)
            active_plans = len([p for p in plans if p.get('status') == 'active'])
            inactive_plans = len([p for p in plans if p.get('status') == 'inactive'])
            
            # Age group breakdown
            age_groups = {}
            for plan in plans:
                age = plan.get('age')
                if age:
                    if age < 5:
                        group = 'Below 5'
                    elif 5 <= age <= 8:
                        group = '6-8'
                    elif 9 <= age <= 10:
                        group = '9-10'
                    elif 11 <= age <= 12:
                        group = '11-12'
                    else:
                        group = '13+'
                    age_groups[group] = age_groups.get(group, 0) + 1
            
            summary_data.append({
                'Metric': 'Total Users',
                'Value': total_users
            })
            summary_data.append({
                'Metric': 'Active Plans',
                'Value': active_plans
            })
            summary_data.append({
                'Metric': 'Inactive Plans',
                'Value': inactive_plans
            })
            
            for group, count in age_groups.items():
                summary_data.append({
                    'Metric': f'Users Age {group}',
                    'Value': count
                })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary_Statistics', index=False)
        
        # Read the file and return it
        with open('book_recommendations_export.xlsx', 'rb') as f:
            file_content = f.read()
        
        # Clean up the file
        import os
        os.remove('book_recommendations_export.xlsx')
        
        from flask import send_file
        from io import BytesIO
        
        # Create response with file
        output = BytesIO(file_content)
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'book_recommendations_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== END EXCEL EXPORT ENDPOINTS ====================

# Test endpoint to verify 4 books per month logic - SIMPLIFIED VERSION
@app.route('/test/4-books-per-month', methods=['GET'])
def test_4_books_per_month():
    try:
        # Simulate the EXACT scenario you're experiencing
        test_recommendations = [
            {
                'name': 'Rick Riordan',
                'confidence_score': 9,
                'rationale': 'Test rationale 1',
                'sample_books': [{'title': 'Percy Jackson and The Sea of Monsters'}],
                'justbookify_link': 'https://test1.com'
            },
            {
                'name': 'Shannon Hale',
                'confidence_score': 8,
                'rationale': 'Test rationale 2',
                'sample_books': [{'title': 'Real Friends'}],
                'justbookify_link': 'https://test2.com'
            }
        ]
        
        # Simulate the EXACT logic now used in the main function
        future_recs = []
        all_recommendations = test_recommendations.copy()
        total_books_needed = 12  # 3 months * 4 books per month
        
        # Duplicate recommendations to fill the gap
        if len(all_recommendations) < total_books_needed:
            while len(all_recommendations) < total_books_needed:
                all_recommendations.extend(all_recommendations[:total_books_needed - len(all_recommendations)])
        
        all_recommendations = all_recommendations[:total_books_needed]
        
        # Create 3 months with exactly 4 books each
        for month_index in range(3):
            month_books = []
            month_start = month_index * 4
            month_end = month_start + 4
            month_recommendations = all_recommendations[month_start:month_end]
            
            for rec in month_recommendations:
                if rec.get('sample_books') and len(rec['sample_books']) > 0:
                    book = rec['sample_books'][0]
                    month_books.append({
                        'title': book['title'],
                        'author': rec['name'],
                        'explanation': rec['rationale'],
                        'justbookify_link': rec['justbookify_link']
                    })
            
            # Ensure exactly 4 books
            while len(month_books) < 4:
                if month_books:
                    last_book = month_books[-1].copy()
                    month_books.append(last_book)
                else:
                    month_books.append({
                        'title': 'Placeholder Book',
                        'author': 'Various Authors',
                        'explanation': 'Placeholder',
                        'justbookify_link': 'https://placeholder.com'
                    })
            
            month_books = month_books[:4]
            
            future_recs.append({
                'month': f'Month {month_index + 1}',
                'books': month_books
            })
        
        return jsonify({
            'success': True,
            'test_result': {
                'original_recommendations': len(test_recommendations),
                'total_books_needed': total_books_needed,
                'recommendations_after_duplication': len(all_recommendations),
                'months_created': len(future_recs),
                'books_per_month': [len(month['books']) for month in future_recs],
                'monthly_breakdown': [
                    {
                        'month': month['month'],
                        'book_count': len(month['books']),
                        'books': month['books']
                    } for month in future_recs
                ]
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)