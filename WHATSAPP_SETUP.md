# Facebook WhatsApp Business API Setup

This application now uses Facebook's WhatsApp Business API instead of Twilio for sending WhatsApp messages.

## Required Environment Variables

Add these to your `.env` file:

```env
# Facebook WhatsApp Business API Configuration
FACEBOOK_WHATSAPP_TOKEN=your_facebook_whatsapp_business_token_here
FACEBOOK_WHATSAPP_PHONE_NUMBER_ID=your_phone_number_id_here
```

## How to Get Your Credentials

### 1. Facebook WhatsApp Business Token
- Go to [Facebook Developers](https://developers.facebook.com/)
- Create or select your app
- Navigate to WhatsApp Business API
- Generate a permanent access token

### 2. Phone Number ID
- In your Facebook app dashboard
- Go to WhatsApp Business API > Getting Started
- Find your Phone Number ID in the configuration section

## Phone Number Format

The API expects phone numbers in international format without the `+` sign:
- ✅ Correct: `1234567890` (for US numbers)
- ✅ Correct: `380986750527` (for international numbers)
- ❌ Incorrect: `+1234567890`

## API Limits

- Message length: 4096 characters (automatically truncated if longer)
- Rate limiting: 1 second delay between messages
- Daily message limits apply based on your Facebook app verification status

## Testing

You can test the WhatsApp functionality using the `/send-recommendations/whatsapp` endpoint:

```bash
curl -X POST http://localhost:5000/send-recommendations/whatsapp \
  -H "Content-Type: application/json" \
  -d '{
    "phone": "1234567890",
    "name": "Test Child",
    "recommendations": [],
    "current": [],
    "future": []
  }'
```

## Troubleshooting

1. **Invalid Token**: Ensure your token has the correct permissions
2. **Phone Number Issues**: Verify the phone number is registered with WhatsApp
3. **Rate Limiting**: The app includes automatic delays between messages
4. **Message Too Long**: Messages are automatically truncated to fit limits 