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

4. QUALITY CONTROL
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

📚 Please recommend unique books that PERFECTLY match these preferences, following these strict criteria:

AGE-SPECIFIC REQUIREMENTS FOR {age}-YEAR-OLD:
- Reading Level: Must be precisely matched to {age}-year-old reading capabilities
- Content Themes: Appropriate for {age}-year-old emotional and cognitive development
- Complexity: Vocabulary and sentence structure suitable for this age
- Format: Age-appropriate book format (picture books, chapter books, etc.)

RECOMMENDATION RULES:
1. NO DUPLICATES:
   - Never recommend the same book twice
   - Avoid multiple books from the same series unless explicitly requested
   - Ensure each recommendation serves a unique reading purpose

2. BALANCED SELECTION:
   - Mix of genres based on preferences
   - Balance between fiction and non-fiction
   - Variety of writing styles and formats
   - Different levels of reading challenge within age-appropriate range

3. STRICT MATCHING:
   - Must exactly match specified genres
   - Must align with listed interests
   - Must be at appropriate reading level
   - Must exclude all mentioned disliked books/series

4. VERIFICATION STEPS:
   - Double-check age appropriateness
   - Verify no duplicate recommendations
   - Ensure each book has unique value proposition
   - Confirm reading level matches age

Remember: Quality over quantity - only include books that are 100% suitable for this specific age and interests.

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
"""}
                ],
                temperature=0.7,
                max_tokens=2000
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

            # Create current month recommendations
            current_recs = []
            if recommendations:
                for rec in recommendations[:3]:  # Take top 3 recommendations
                    for book in rec['sample_books'][:1]:  # Take first book from each recommendation
                        current_recs.append({
                            'title': book['title'],
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']
                        })

            # Create future months recommendations
            future_recs = []
            remaining_recs = recommendations[3:] if len(recommendations) > 3 else []

            for i in range(3):
                month_books = []
                month_start = i * 4  # Changed from 2 to 4 books per month
                month_end = month_start + 4  # Changed from 2 to 4 books per month

                for rec in remaining_recs[month_start:month_end]:
                    for book in rec['sample_books'][:1]:
                        month_books.append({
                            'title': book['title'],
                            'author': rec['name'],
                            'explanation': rec['rationale'],
                            'justbookify_link': rec['justbookify_link']  # Also adding justbookify_link to future recommendations
                        })

                future_recs.append({
                    'month': (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B'),
                    'books': month_books
                })

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
                
                # Insert the recommendation plan
                result = recommendation_plans_collection.insert_one(plan_data)
                plan_id = str(result.inserted_id)
                
                print(f"Saved recommendation plan with ID: {plan_id}")
                
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
        
        # Add all quiz data fields
        quiz_fields = [
            'name', 'age', 'parentEmail', 'parentPhone', 'parentReading',
            'selectedGenres', 'selectedInterests', 'nonFictionInterests',
            'topThreeGenres', 'fictionGenres', 'nonFictionGenres',
            'additionalGenres', 'fictionNonFictionRatio', 'bookSeries'
        ]
        
        for field in quiz_fields:
            if field in data:
                update_data[field] = data[field]
        
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

if __name__ == '__main__':
    app.run(debug=True)