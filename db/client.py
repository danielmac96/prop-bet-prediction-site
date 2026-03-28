"""
db/client.py
============
Supabase client — initialised once, imported everywhere.
Reads SUPABASE_URL and SUPABASE_KEY from environment / .env file.
"""

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_KEY must be set in your environment or .env file."
        )

    return create_client(url, key)


# Module-level singleton — import this directly
supabase: Client = get_client()
