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

# Age groups configuration
AGE_GROUPS = ['4-7', '8-10', '11+']

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
        # Check if recommendations exist for all age groups
        for age_group in AGE_GROUPS:
            existing = recommendations_collection.find_one({'age_group': age_group})
            if not existing:
                recommendations_collection.insert_one({
                    'age_group': age_group,
                    'books': []
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
        # Get all recommendations
        all_recommendations = list(recommendations_collection.find())
        
        # Format response
        recommendations_dict = {}
        for rec in all_recommendations:
            age_group = rec['age_group']
            book_ids = rec.get('books', [])
            
            # Get book details for each book ID
            books = []
            for book_id in book_ids:
                if ObjectId.is_valid(book_id):
                    book = books_collection.find_one({'_id': ObjectId(book_id)})
                    if book:
                        books.append(format_document(book))
            
            recommendations_dict[age_group] = books
            
        return jsonify(recommendations_dict)
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Update recommendations for an age group
@app.route('/recommendations/<age_group>', methods=['PUT'])
def update_recommendations(age_group):
    try:
        # Validate age group
        validate_age_group(age_group)
        
        # Get book IDs from request
        books = request.json
        if not isinstance(books, list):
            return jsonify({
                'error': 'Invalid request format. Expected list of books.'
            }), 400
            
        # Extract book IDs and validate they exist
        book_ids = []
        for book in books:
            book_id = book.get('id')
            if not book_id or not ObjectId.is_valid(book_id):
                return jsonify({
                    'error': f'Invalid book ID: {book_id}'
                }), 400
                
            # Verify book exists
            if not books_collection.find_one({'_id': ObjectId(book_id)}):
                return jsonify({
                    'error': f'Book not found: {book_id}'
                }), 404
                
            book_ids.append(book_id)
            
        # Update recommendations
        recommendations_collection.update_one(
            {'age_group': age_group},
            {'$set': {'books': book_ids}},
            upsert=True
        )
        
        # Get updated recommendations
        updated_rec = recommendations_collection.find_one({'age_group': age_group})
        if not updated_rec:
            return jsonify({
                'error': 'Failed to update recommendations'
            }), 500
            
        # Get book details
        books = []
        for book_id in updated_rec.get('books', []):
            if ObjectId.is_valid(book_id):
                book = books_collection.find_one({'_id': ObjectId(book_id)})
                if book:
                    books.append(format_document(book))
                    
        return jsonify(books)
        
    except ValueError as e:
        return jsonify({
            'error': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

# Get recommendations for specific age group
@app.route('/recommendations/<age_group>', methods=['GET'])
def get_age_group_recommendations(age_group):
    try:
        # Validate age group
        validate_age_group(age_group)
        
        # Get recommendations
        rec = recommendations_collection.find_one({'age_group': age_group})
        if not rec:
            return jsonify([])
            
        # Get book details
        books = []
        for book_id in rec.get('books', []):
            if ObjectId.is_valid(book_id):
                book = books_collection.find_one({'_id': ObjectId(book_id)})
                if book:
                    books.append(format_document(book))
                    
        return jsonify(books)
        
    except ValueError as e:
        return jsonify({
            'error': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

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
                model="gpt-4",
                messages=[
                    {"role": "system", "content": """You are an expert children's book recommendation system that carefully considers age appropriateness, reading preferences, and personal interests. Your recommendations should:
1. Strictly match the reader's age range and interests
2. Only include books that would be enjoyable based on the provided preferences
3. Prioritize books that align with multiple interest areas
4. Consider reading level appropriateness
5. Exclude any books that don't match the specified genres or interests"""},
                    {"role": "user", "content": f"""I need personalized book recommendations for a {age}-year-old reader with the following preferences:

GENRES THEY ENJOY: {', '.join(data['selectedGenres'])}
SPECIFIC INTERESTS: {', '.join(data['selectedInterests'])}
NON-FICTION INTERESTS: {', '.join(data['nonFictionInterests'])}
BOOK SERIES PREFERENCE: They {'enjoy' if data['bookSeries'] else 'do not prefer'} book series.

Available books in our inventory:

{book_text}

📚 Please recommend **25 books** that PERFECTLY match these preferences. Group them by author or series, with at least 10 different authors/series.

IMPORTANT GUIDELINES:
- Only include books that strongly match the specified genres and interests
- Ensure age appropriateness for a {age}-year-old reader
- If they don't prefer series, prioritize standalone books
- Focus on books that align with their specific interests
- Consider both fiction and non-fiction based on their preferences
- Exclude any books that don't match their interests or reading level

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
            
            # Parse JSON response
            recommendations_json = json.loads(raw_content)
            recommendations = []
            
            for rec in recommendations_json:
                series_name = rec.get('name', '')
                confidence_score = rec.get('likely_score', 8)
                rationale = rec.get('rationale', '')
                sample_books = [{"title": title, "author": series_name} for title in rec.get('books', [])]
                
                if series_name and sample_books:
                    # Create direct JustBookify search link - clean up the series name
                    clean_name = series_name.lower()
                    # Remove various forms of "series" from the name
                    clean_name = clean_name.replace(" series name", "")
                    clean_name = clean_name.replace("series name", "")
                    clean_name = clean_name.replace(" series", "")
                    clean_name = clean_name.replace("series", "")
                    # Remove any extra whitespace and trim
                    clean_name = " ".join(clean_name.split()).strip()
                    # If name is empty after cleaning, use original name
                    if not clean_name:
                        clean_name = series_name.lower().strip()
                    search_term = urllib.parse.quote(clean_name)
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
                            'explanation': rec['rationale']
                        })

            # Create future months recommendations
            future_recs = []
            remaining_recs = recommendations[3:] if len(recommendations) > 3 else []
            
            for i in range(3):
                month_books = []
                month_start = i * 2
                month_end = month_start + 2
                
                for rec in remaining_recs[month_start:month_end]:
                    for book in rec['sample_books'][:1]:
                        month_books.append({
                            'title': book['title'],
                            'author': rec['name'],
                            'explanation': rec['rationale']
                        })
                
                future_recs.append({
                    'month': (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B'),
                    'books': month_books
                })

            print("Returning results...")
            print(f"Current recommendations: {len(current_recs)}")
            print(f"Future months: {len(future_recs)}")
            
            return jsonify({
                'current': current_recs,
                'future': future_recs,
                'recommendations': recommendations
            })

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {str(e)}")
            return jsonify({
                'current': [],
                'future': empty_reading_plan,
                'recommendations': [],
                'error': f'JSON parsing error: {str(e)}'
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

if __name__ == '__main__':
    app.run(debug=True)