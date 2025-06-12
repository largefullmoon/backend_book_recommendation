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
from twilio.rest import Client
import json

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

# Age groups configuration
AGE_GROUPS = ['4-7', '8-10', '11+']

# Configure OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

# Configure Shopify
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')

# Configure SendGrid
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'your-verified-sender@yourdomain.com')

# Configure Twilio
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER')

def get_age_appropriate_prompt(age):
    if age < 12:
        return "You are a helpful children's book recommendation assistant. Recommend only age-appropriate books for young readers. Focus on educational and entertaining content."
    elif age < 18:
        return "You are a helpful young adult book recommendation assistant. Recommend age-appropriate books for teenagers, considering their maturity level."
    else:
        return "You are a helpful book recommendation assistant. Provide recommendations suitable for adult readers."

# Helper function to convert ObjectId to string in book documents
def format_book(book):
    book['id'] = str(book.pop('_id'))
    return book

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
        required_fields = ['title', 'author', 'description', 'genres', 'ageRange']
        
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

# Add book recommendation for user
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
        required_fields = ['name', 'age', 'selectedGenres', 'selectedInterests', 
                         'nonFictionInterests', 'bookSeries', 'parentEmail', 'parentPhone']
        
        # Validate required fields
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400

        # Get age-appropriate books based on user's age
        age = data['age']
        age_group = '4-7' if age < 8 else '8-10' if age < 11 else '11+'

        # Get all potential books matching age range and genres
        potential_books = list(books_collection.find({
            'genres': {'$in': data['selectedGenres']},
            'ageRange.min': {'$lte': age},
            'ageRange.max': {'$gte': age}
        }))

        # Initialize empty response structure
        empty_reading_plan = [
            {
                'month': (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B'),
                'books': []
            } for i in range(3)
        ]

        # If no books found, return empty results
        if not potential_books:
            return jsonify({
                'recommendations': [],
                'readingPlan': empty_reading_plan
            })

        # Prepare user interests and preferences
        interests = data['selectedInterests'] + data['nonFictionInterests']
        interests_str = ', '.join(interests)

        # Create a single context with all books
        books_context = []
        for idx, book in enumerate(potential_books, 1):
            book_info = f"""Book {idx}:
Title: {book['title']}
Author: {book['author']}
Description: {book.get('description', 'No description available')}
Genres: {', '.join(book.get('genres', []))}
Age Range: {book.get('ageRange', {}).get('min', 0)}-{book.get('ageRange', {}).get('max', 99)}
---"""
            books_context.append(book_info)

        # Create comprehensive prompt for OpenAI
        system_prompt = f"""You are a children's book recommendation expert. Analyze the following books and select the most suitable ones for a child with these characteristics:

Age: {age} years old
Interests: {interests_str}
Series Preference: {'Enjoys' if data['bookSeries'] else 'Does not prefer'} book series

For each book, evaluate:
1. Age appropriateness
2. Match with interests
3. Reading level suitability
4. Educational value
5. Entertainment value

Return your response in JSON format with this structure:
{{
    "recommendations": [
        {{
            "book_number": <number>,
            "score": <0-100>,
            "explanation": <why this book is recommended>,
            "reading_month": <1, 2, or 3 - distribute books across 3 months>
        }}
    ]
}}

Select and score the top 5 most suitable books. Distribute them across 3 months based on progressive reading difficulty and thematic connections."""

        analysis_prompt = "\n".join(books_context)

        # Get recommendations from OpenAI
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": analysis_prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )

        # Parse OpenAI response
        try:
            analysis = json.loads(response.choices[0].message.content)
            recommendations_data = analysis.get('recommendations', [])

            # Process recommendations
            recommendations = []
            reading_plan = [{'month': month, 'books': []} for month in [
                (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B')
                for i in range(3)
            ]]

            for rec in recommendations_data:
                book_idx = rec['book_number'] - 1
                if 0 <= book_idx < len(potential_books):
                    book = potential_books[book_idx]
                    recommendation = {
                        'title': book['title'],
                        'author': book['author'],
                        'link': f"/books/{str(book['_id'])}",
                        'score': rec['score'],
                        'explanation': rec['explanation']
                    }
                    recommendations.append(recommendation)

                    # Add to reading plan
                    month_idx = rec['reading_month'] - 1
                    if 0 <= month_idx < 3:
                        reading_plan[month_idx]['books'].append({
                            'title': book['title'],
                            'explanation': rec['explanation']
                        })

            # Sort recommendations by score
            recommendations.sort(key=lambda x: x['score'], reverse=True)

            return jsonify({
                'recommendations': recommendations,
                'readingPlan': reading_plan
            })

        except json.JSONDecodeError as e:
            print(f"Error parsing OpenAI response: {str(e)}")
            # Return empty results instead of error
            return jsonify({
                'recommendations': [],
                'readingPlan': empty_reading_plan
            })

    except Exception as e:
        print(f"Error in generate_recommendation_plan: {str(e)}")
        # Return empty results instead of error
        return jsonify({
            'recommendations': [],
            'readingPlan': [
                {
                    'month': (datetime.now().replace(day=1) + timedelta(days=i*31)).strftime('%B'),
                    'books': []
                } for i in range(3)
            ]
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
        
        <h3>Recommended Books:</h3>
        <ul>
        {''.join(f'<li><strong>{book["title"]}</strong> by {book["author"]}</li>' for book in data['recommendations'])}
        </ul>

        <h3>3-Month Reading Plan:</h3>
        {''.join(f'''
        <div style="margin-bottom: 20px;">
            <h4>{month['month']}</h4>
            <ul>
            {''.join(f'<li>{book}</li>' for book in month['books'])}
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

# Send recommendations via WhatsApp
@app.route('/send-recommendations/whatsapp', methods=['POST'])
def send_whatsapp_recommendations():
    try:
        data = request.json
        required_fields = ['phone', 'recommendations', 'readingPlan', 'name']
        
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields'
            }), 400

        # Format the message
        message_body = f"Hello! Here are the book recommendations for {data['name']}:\n\n"
        message_body += "📚 Recommended Books:\n"
        for book in data['recommendations']:
            message_body += f"• {book['title']} by {book['author']}\n"
        
        message_body += "\n📅 3-Month Reading Plan:\n"
        for month in data['readingPlan']:
            message_body += f"\n{month['month']}:\n"
            for book in month['books']:
                message_body += f"• {book}\n"
        
        message_body += "\nHappy Reading! 📖"

        # Initialize Twilio client
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

        # Send WhatsApp message
        message = client.messages.create(
            body=message_body,
            from_=f"whatsapp:{TWILIO_WHATSAPP_NUMBER}",
            to=f"whatsapp:{data['phone']}"
        )

        if message.sid:
            return jsonify({
                'message': 'Recommendations sent successfully to WhatsApp'
            })
        else:
            return jsonify({
                'error': 'Failed to send WhatsApp message'
            }), 500

    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)