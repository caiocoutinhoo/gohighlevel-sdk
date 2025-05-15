import os
from dotenv import load_dotenv

load_dotenv()


class HighLevelConfig(object):
    CLIENT_ID = os.environ.get("GHL_CLIENT_ID", None)
    CLIENT_SECRET = os.environ.get("GHL_CLIENT_SECRET", None)
    API_BASE_URL = "https://services.leadconnectorhq.com"
    AUTH_BASE_URL = "https://marketplace.gohighlevel.com"
    VERSION = "2021-07-28"
    SCOPES = [
        "businesses.readonly",
        "calendars.readonly",
        "contacts.readonly",
        "contacts.write",
        "locations.readonly",
        "locations.write",
        "opportunities.readonly",
        "opportunities.write",
        "calendars/events.readonly",
        "calendars/events.write",
        "users.readonly",
        "users.write",
        "conversations.readonly",
        "conversations/message.readonly",
        "locations/customFields.readonly",
        "forms.readonly",
        "surveys.readonly",
        "workflows.readonly",
    ]
    REDIRECT_URI = "http://localhost:3000/oauth/callback"
