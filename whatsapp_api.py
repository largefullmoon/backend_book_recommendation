"""
WhatsApp API utility functions using Facebook Graph API
"""
import requests
import time
import os
from typing import List, Dict, Any, Optional

class WhatsAppAPI:
    def __init__(self, access_token: str, phone_number_id: str):
        self.access_token = access_token
        self.phone_number_id = phone_number_id
        self.base_url = f"https://graph.facebook.com/v22.0/{phone_number_id}"
        
    def format_phone_number(self, phone: str) -> str:
        """
        Format phone number for WhatsApp API
        Removes all non-digit characters and formats for international use
        """
        import re
        phone = str(phone).strip()
        # Remove all non-digit characters except +
        phone = re.sub(r'[^\d+]', '', phone)
        
        # Remove leading + if present
        if phone.startswith('+'):
            phone = phone[1:]
            
        # Add country code if missing (assuming US/international format)
        if len(phone) == 10:
            phone = '1' + phone  # Add US country code
        
        return phone
    
    def send_text_message(self, to_phone: str, message_text: str) -> requests.Response:
        """
        Send a text message via WhatsApp Business API
        """
        url = f"{self.base_url}/messages"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to_phone,
            "type": "text",
            "text": {
                "body": message_text
            }
        }
        
        response = requests.post(url, headers=headers, json=payload)
        return response
    
    def send_template_message(self, to_phone: str, template_name: str, language_code: str = "en_US") -> requests.Response:
        """
        Send a template message via WhatsApp Business API
        """
        url = f"{self.base_url}/messages"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to_phone,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {
                    "code": language_code
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=payload)
        return response
    
    def send_multiple_messages(self, to_phone: str, messages: List[str], delay: float = 1.0) -> List[Dict[str, Any]]:
        """
        Send multiple messages with delay between each
        Returns list of response information for each message
        """
        formatted_phone = self.format_phone_number(to_phone)
        message_responses = []
        successful_messages = 0
        
        for i, msg in enumerate(messages):
            # Ensure message doesn't exceed WhatsApp limit (4096 characters)
            if len(msg) > 4000:
                msg = msg[:3950] + "...\n(Message truncated)"
            
            response = self.send_text_message(formatted_phone, msg)
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    message_responses.append({
                        'message_id': response_data.get('messages', [{}])[0].get('id'),
                        'status': 'sent',
                        'message_number': i + 1,
                        'response_data': response_data
                    })
                    successful_messages += 1
                except Exception as e:
                    message_responses.append({
                        'status': 'sent_but_parse_error',
                        'error': str(e),
                        'message_number': i + 1,
                        'raw_response': response.text
                    })
            else:
                print(f"WhatsApp API error for message {i+1}: {response.status_code} - {response.text}")
                message_responses.append({
                    'status': 'failed',
                    'error_code': response.status_code,
                    'error': response.text,
                    'message_number': i + 1
                })
            
            # Add delay between messages to prevent rate limiting
            if i < len(messages) - 1:  # Don't delay after the last message
                time.sleep(delay)
        
        return {
            'total_messages': len(messages),
            'successful_messages': successful_messages,
            'message_responses': message_responses,
            'recipient_phone': formatted_phone
        }

def create_whatsapp_client() -> Optional[WhatsAppAPI]:
    """
    Create WhatsApp API client from environment variables
    """
    access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
    phone_number_id = os.getenv('WHATSAPP_PHONE_NUMBER_ID')
    
    if not access_token or not phone_number_id:
        return None
    
    return WhatsAppAPI(access_token, phone_number_id)

def format_book_recommendations_messages(data: Dict[str, Any]) -> List[str]:
    """
    Format book recommendations into multiple WhatsApp messages
    """
    messages = []
    name = data.get('name', 'Reader')
    
    # Header message
    header = f"📚 Book Recommendations for {name} 📚\n"
    current_message = header

    # Current Top Picks
    if data.get('current'):
        picks_message = "⭐ TOP PICKS FOR YOU ⭐\n"
        for book in data['current']:
            book_text = f"• {book['title']}"
            if book.get('series'):
                book_text += f" ({book['series']} Series)"
            book_text += f" by {book['author']}\n"
            if book.get('explanation'):
                book_text += f"  Why: {book['explanation'][:100]}...\n"
            picks_message += book_text + "\n"
        
        # Check if adding picks would exceed limit (WhatsApp text limit is 4096 chars)
        if len(current_message + picks_message) > 3800:  # Buffer for safety
            messages.append(current_message)
            current_message = header + picks_message
        else:
            current_message += picks_message

    # Series & Authors Recommendations (Split into multiple messages)
    if data.get('recommendations'):
        series_header = "📖 RECOMMENDED SERIES & AUTHORS 📖\n"
        current_series_message = series_header
        
        for rec in data['recommendations']:
            series_text = f"\n{rec.get('series_name', rec.get('name', 'Unknown Series'))} by {rec.get('author_name', 'Unknown Author')} (Score: {rec.get('confidence_score', 'N/A')}/10)\n"
            if rec.get('rationale'):
                series_text += f"Why: {rec['rationale'][:100]}...\n"
            
            # Add up to 2 sample books
            if rec.get('sample_books'):
                series_text += "Featured Books:\n"
                for book in rec['sample_books'][:2]:
                    book_text = f"• {book['title']} by {book['author']}\n"
                    series_text += book_text
            
            series_text += f"🔍 View More: {rec['justbookify_link']}\n"

            # Check if adding this series would exceed limit
            if len(current_series_message + series_text) > 3800:
                messages.append(current_series_message)
                current_series_message = header + series_header + series_text
            else:
                current_series_message += series_text

        if current_series_message != series_header:
            messages.append(current_series_message)

    # Reading Plan (Split by month)
    if data.get('future'):
        for month in data['future']:
            month_message = f"📅 {month['month'].upper()} READING PLAN 📅\n"
            if month.get('books'):
                for book in month['books']:
                    book_text = f"• {book['title']}"
                    if book.get('series'):
                        book_text += f" ({book['series']} Series)"
                    book_text += f" by {book['author']}\n"
                    month_message += book_text
            else:
                month_message += "More recommendations coming soon!\n"
            
            messages.append(header + month_message + "\n")

    # Add footer to last message
    if messages:
        messages[-1] += "\n📚 Happy Reading! 📚"
    
    return messages 