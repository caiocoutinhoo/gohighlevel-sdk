# GoHighLevel SDK

A Python SDK for interacting with the GoHighLevel API.

## Features

- Complete API client with authentication flow
- Model abstractions for GoHighLevel entities
- Endpoint wrappers for users, calendars, contacts, and opportunities
- Data manipulation and parsing utilities
- Pandas DataFrame integration

## Installation

```bash
pip install -r requirements.txt
```

## Usage Example

```python
from highlevel_sdk.api import endpoints
import dotenv
import os

# Load environment variables
dotenv.load_dotenv()

# Initialize the service
ghl_token = os.environ.get("GHL_TOKEN")
ghl_location_id = os.environ.get("GHL_LOCATION_ID")
ghl_service = endpoints.GoHighLevelService(token=ghl_token, id_location=ghl_location_id)

# Get data as pandas DataFrames
users = ghl_service.get_users_dataframe()
contacts = ghl_service.get_contacts_dataframe()
opportunities = ghl_service.get_opportunities_dataframe()
```

## Available Endpoints

- Users
- Calendars & Events
- Custom Fields
- Custom Values
- Pipelines
- Contacts
- Opportunities
- Attributions

## License

MIT
