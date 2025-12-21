#!/bin/bash
# Script to generate API keys inside Docker container
docker-compose exec backend python -c "
import sys
import os
sys.path.insert(0, '/app')
os.chdir('/app')

# Import and execute the script
from scripts.generate_api_keys import *
if __name__ == '__main__':
    keys = {
        'moderation-worker': create_api_key('Moderation Worker', expires_days=None),
        'preview-worker': create_api_key('Preview Worker', expires_days=None),
        'publish-worker': create_api_key('Publish Worker', expires_days=None),
        'dlq-worker': create_api_key('DLQ Worker', expires_days=None),
    }
    print('\n' + '='*60)
    print('API Keys generated successfully!')
    print('='*60)
    print('\nAdd these to your .env file:')
    print('INTERNAL_API_KEY=<any-of-the-keys-above>')
    print('\nOr set them individually for each worker service.')
"

