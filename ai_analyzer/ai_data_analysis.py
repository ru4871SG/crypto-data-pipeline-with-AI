"""
This module contains the function that sends the data to GPT API for analysis.
"""

import os
import json
import requests

def get_api_key():
    return os.getenv('OPENAI_API_KEY')

def data_analyzer(prompt):
    # GPT API endpoint
    url = 'https://api.openai.com/v1/chat/completions'

    # OpenAI API key
    api_key = get_api_key()

    # Data to be sent to the API
    data = {
        'model': 'gpt-4o',
        'messages': [
            {'role': 'system', 'content': 'You are a helpful data analyst that writes crypto analysis reports from the data that is shown to you.'},
            {'role': 'user', 'content': prompt}
        ],
        'max_tokens': 100
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

    # Validate the API response
    if response.status_code == 200:
        formatted_response = response.json()
        message = formatted_response['choices'][0]['message']['content'].strip()
    else:
        message = f"Unexpected API response: {response.status_code}, {response.text}"

    return message