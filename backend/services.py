from dotenv import load_dotenv
from supabase import Client
import httpx
import os

load_dotenv()

# --- Supabase Auth Service ---

async def verify_supabase_jwt(token: str, supabase: Client) -> dict:
    """
    Verifies a Supabase JWT and fetches the user's role from the profiles table.
    Includes extra debugging print statements.
    """
    try:
        # 1. Verify the token with Supabase Auth to get the user object
        print("\n--- Verifying JWT ---")
        url = f"{os.environ.get('URL')}/auth/v1/user"
        # The ANON_KEY is required for this specific API call.
        anon_key = os.environ.get("ANON_KEY")
        if not anon_key:
            print("FATAL ERROR: ANON_KEY is not set in environment.")
            return None

        headers = {'Authorization': f'Bearer {token}', 'apikey': anon_key}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            user_data = response.json()
            print(f"Step 1 SUCCESS: JWT is valid for user: {user_data.get('email')}")
            print(f"User ID from Auth: {user_data.get('id')}")

            # 2. Fetch the user's role from your public 'profiles' table
            print("\n--- Fetching profile from database ---")
            profile_res = supabase.table('profiles').select('id, role, is_verified').eq('id', user_data['id']).single().execute()

            print(f"Step 2: Database query result: {profile_res.data}")

            if profile_res.data:
                user_data['role'] = profile_res.data.get('role')
                user_data['is_verified'] = profile_res.data.get('is_verified', False)
                user_data['id'] = user_data.get('id')
                print(f"Step 2 SUCCESS: Found profile. Assigned role: '{user_data['role']}'")
                print("------------------------\n")
                return user_data
            else:
                print(f"Step 2 FAILED: No profile found in 'profiles' table for user ID: {user_data['id']}")
                print("------------------------\n")
                return None

    except httpx.HTTPStatusError as e:
        print(f"JWT Verification FAILED: {e.response.text}")
        print("------------------------\n")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during JWT verification: {str(e)}")
        print("------------------------\n")
        return None


# --- Notification Services ---

async def get_subscribers_for_advisor(advisor_id: str, supabase: Client) -> list[str]:
    """
    Fetches the FCM tokens for all users subscribed to a specific advisor.
    """
    try:
        # Get all user_ids that are subscribed to the advisor
        subscriptions_query = supabase.table('subscriptions').select('user_id').eq('advisor_id', advisor_id)
        subscriptions_data = fetch_all_paginated_data(subscriptions_query)

        if not subscriptions_data:
            return []

        subscriber_ids = [sub['user_id'] for sub in subscriptions_data]

        # Get the FCM tokens for those user_ids
        tokens_query = supabase.table('profiles').select('fcm_token').in_('id', subscriber_ids)
        tokens_data = fetch_all_paginated_data(tokens_query)

        if not tokens_data:
            return []

        fcm_tokens = [profile['fcm_token'] for profile in tokens_data if profile.get('fcm_token')]
        return fcm_tokens

    except Exception as e:
        print(f"Error fetching subscribers: {str(e)}")
        return []


async def send_mock_fcm_notification(tokens: list[str], title: str, body: str):
    """
    A mock function to simulate sending a push notification via FCM.
    """
    print("\n--- MOCK FCM NOTIFICATION ---")
    print(f"Title: {title}")
    print(f"Body: {body}")
    print(f"Sending to {len(tokens)} tokens: {tokens}")
    print("-----------------------------\n")
    # In a real app, you would use a library like 'google-auth' and 'httpx'
    # to send a POST request to https://fcm.googleapis.com/v1/projects/YOUR_PROJECT_ID/messages:send
    pass


# --- Data Fetching Helper ---

def fetch_all_paginated_data(query_builder) -> list:
    """
    Fetches all rows for a given query by handling pagination,
    bypassing the default row limit.
    """
    all_data = []
    page = 0
    page_size = 1000  # Max page size for Supabase queries

    while True:
        range_start = page * page_size
        response = query_builder.range(range_start, range_start + page_size - 1).execute()

        if response.data:
            all_data.extend(response.data)
            if len(response.data) < page_size:
                break
        else:
            break

        page += 1

    return all_data
