# Prerequisites:
# 1. Install Python: https://www.python.org/
# 2. Install redis-py library: pip install redis
# 3. Have a Redis server running (e.g., locally or via Docker: docker run -d -p 6379:6379 --name my-redis redis)

import redis
import random
import time # For join_date in user profile, and potential demo pauses

# --- Configuration for Redis Connection ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0 # Default Redis database number

# --- Helper Function for Redis Connection ---
def get_redis_connection():
    """Creates and returns a Redis connection. Exits if connection fails."""
    try:
        # decode_responses=True means Redis will return strings instead of bytes,
        # which is generally easier to work with in Python.
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping() # Send a PING command to check if the server is responsive
        print("Successfully connected to Redis.")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        print("Please ensure Redis server is running on {REDIS_HOST}:{REDIS_PORT}.")
        exit(1) # Exit the script if we can't connect

# --- Part 1: Adding and Retrieving Data ---

def part1_add_retrieve_sequential(r: redis.Redis):
    """
    Inserts values 1-100 into a Redis List and prints them in reverse order.
    We use LPUSH to add to the head of the list, so 1, then 2, then 3...
    results in a list like [..., 3, 2, 1].
    LRANGE 0 -1 then retrieves the entire list in this naturally reversed order.
    """
    print("\n--- Part 1, Task 1: Sequential Numbers (1-100), Reverse Order ---")
    key_name = "sequential_numbers_list" # Name for our Redis key (the list)

    # Clean up any data from a previous run for this key
    r.delete(key_name)
    print(f"Inserting numbers 1-100 into Redis list '{key_name}'...")

    # Loop from 1 to 100
    for i in range(1, 101):
        # LPUSH adds the element 'i' to the *left* (head) of the list.
        # So, after pushing 1, list is [1]
        # After pushing 2, list is [2, 1]
        # After pushing 3, list is [3, 2, 1]
        # ...
        # After pushing 100, list is [100, 99, ..., 3, 2, 1]
        r.lpush(key_name, i)

    print("Retrieving numbers in reverse insertion order (100 down to 1)...")
    # LRANGE key_name 0 -1 retrieves all elements from the list,
    # from index 0 (first) to index -1 (last).
    # Since we LPUSHed, the list is already in the desired [100, ..., 1] order.
    numbers = r.lrange(key_name, 0, -1)

    print(f"Retrieved {len(numbers)} numbers:")
    print(numbers)
    # You could optionally clean up the key after use:
    # r.delete(key_name)

def part1_add_retrieve_random(r: redis.Redis):
    """
    Inserts 100 random values into a Redis Sorted Set and prints them in descending order.
    A Sorted Set (ZSET) is efficient because it keeps elements sorted by a score.
    We use the random number itself as both the member and its score.
    ZREVRANGE then retrieves elements from highest score to lowest.
    """
    print("\n--- Part 1, Task 2: Random Numbers (Descending Order) ---")
    key_name = "random_numbers_sorted_set" # Name for our Redis key (the sorted set)

    # Clean up any data from a previous run for this key
    r.delete(key_name)
    print(f"Inserting 100 random numbers into Redis sorted set '{key_name}'...")

    # We'll use a Python dictionary to prepare data for ZADD
    # ZADD can take a mapping of {member1: score1, member2: score2, ...}
    members_with_scores = {}
    for _ in range(100):
        num = random.randint(1, 1000) # Generate a random number (e.g., between 1 and 1000)
        # For a Sorted Set, we need a 'member' (the item) and a 'score' (for sorting).
        # Here, we use the number itself as BOTH the member and the score.
        # Sorted Sets only store unique members. If we generate the same random number
        # twice, ZADD will just update its score (which won't change if score=member).
        members_with_scores[num] = num # {random_number: random_number_as_score}

    if members_with_scores: # Make sure we have something to add
        # ZADD adds members with their scores to the sorted set.
        # Redis automatically orders them by score.
        r.zadd(key_name, members_with_scores)
        print(f"Inserted {len(members_with_scores)} unique random numbers.")
    else:
        print("No random numbers were generated to insert.")
        return

    print("Retrieving random numbers in descending order (largest to smallest)...")
    # ZREVRANGE key_name 0 -1 retrieves all members from the sorted set,
    # ordered from the highest score (index 0 in reverse) to the lowest (index -1 in reverse).
    numbers = r.zrevrange(key_name, 0, -1)

    print(f"Retrieved {len(numbers)} numbers:")
    print(numbers)
    # You could optionally clean up the key after use:
    # r.delete(key_name)

# --- Part 2: Data Modeling (Shopping Cart and User) ---

# Data Model Chosen:
# User: Redis Hash. Key: `user:{user_id}`. Fields: `name`, `email`, etc.
# Shopping Cart: Redis Hash. Key: `cart:{user_id}`. Fields: SKU, Values: Quantity.

def create_user(r: redis.Redis, user_id: str, name: str, email: str):
    """Creates or updates user data using a Redis Hash."""
    user_key = f"user:{user_id}" # e.g., "user:u123"
    # HSET stores field-value pairs in a Hash.
    # If the hash or fields don't exist, they are created.
    # If they exist, they are updated.
    r.hset(user_key, mapping={
        "name": name,
        "email": email,
        "join_date": time.strftime("%Y-%m-%d %H:%M:%S")
    })
    print(f"User '{user_id}' (key: '{user_key}') created/updated.")

def add_item_to_cart(r: redis.Redis, user_id: str, sku: str, quantity: int = 1):
    """Adds a specific quantity of an item (SKU) to the user's cart Hash."""
    cart_key = f"cart:{user_id}" # e.g., "cart:u123"
    # HINCRBY atomically increments the value of 'sku' field in the 'cart_key' hash by 'quantity'.
    # If 'sku' doesn't exist, it's created with 'quantity' as its value.
    # This is perfect for managing item counts.
    new_quantity_in_cart = r.hincrby(cart_key, sku, quantity)
    print(f"Added {quantity} of SKU '{sku}' to cart for user '{user_id}'. New quantity for SKU: {new_quantity_in_cart}")

def remove_item_from_cart(r: redis.Redis, user_id: str, sku: str, quantity_to_remove: int = 1):
    """Removes a specific quantity of an item (SKU) from the user's cart Hash."""
    cart_key = f"cart:{user_id}"
    current_quantity_str = r.hget(cart_key, sku) # HGET gets the value of a single field

    if current_quantity_str is None:
        print(f"SKU '{sku}' not found in cart for user '{user_id}'. Cannot remove.")
        return

    current_quantity = int(current_quantity_str) # Convert string from Redis to int
    if quantity_to_remove >= current_quantity:
        # If trying to remove more than or equal to what's there, delete the item from cart
        r.hdel(cart_key, sku) # HDEL deletes a field from the hash
        print(f"Removed SKU '{sku}' completely from cart for user '{user_id}'.")
    else:
        # Otherwise, decrement the quantity using HINCRBY with a negative value
        new_quantity_in_cart = r.hincrby(cart_key, sku, -quantity_to_remove)
        print(f"Removed {quantity_to_remove} of SKU '{sku}' from cart for user '{user_id}'. New quantity for SKU: {new_quantity_in_cart}")

def get_cart_contents(r: redis.Redis, user_id: str) -> dict:
    """Retrieves the entire contents of the user's cart Hash."""
    cart_key = f"cart:{user_id}"
    # HGETALL returns a dictionary of all field-value pairs from the hash.
    # Values from Redis are strings, so we convert quantities to integers.
    cart_data_str_values = r.hgetall(cart_key)
    return {sku: int(qty) for sku, qty in cart_data_str_values.items()}

def display_cart(user_id: str, cart_contents: dict):
    """Prints the cart contents in a user-friendly format."""
    print(f"\n--- Shopping Cart for User '{user_id}' ---")
    if not cart_contents:
        print("Cart is empty.")
    else:
        print(f"{'SKU':<25} {'Quantity':<10}")
        print("-" * 35)
        for sku, quantity in cart_contents.items():
            print(f"{sku:<25} {quantity:<10}")
    print("-" * 35)

def clear_user_and_cart_data(r: redis.Redis, user_id: str):
    """Deletes the user's info Hash and their cart Hash for cleanup."""
    user_key = f"user:{user_id}"
    cart_key = f"cart:{user_id}"
    # r.delete can take multiple keys to delete at once
    deleted_count = r.delete(user_key, cart_key)
    print(f"Cleaned up data for user '{user_id}'. Keys deleted: {deleted_count} (expected 2 if both existed).")


# --- Main Execution Block ---
if __name__ == "__main__":
    # This block runs when you execute the script directly (python your_script_name.py)

    # 1. Establish connection to Redis
    redis_conn = get_redis_connection()

    # --- Execute Part 1 ---
    print("\n======== EXECUTING PART 1 ========")
    part1_add_retrieve_sequential(redis_conn)
    part1_add_retrieve_random(redis_conn)

    # --- Execute Part 2: Shopping Cart Demo ---
    print("\n======== EXECUTING PART 2 (SHOPPING CART DEMO) ========")
    user1_id = "u1001" # Example user ID
    user2_id = "u2002" # Another example user ID

    # Cleanup any old data for these users before starting the demo
    print("\n--- Initial Cleanup for Demo Users ---")
    clear_user_and_cart_data(redis_conn, user1_id)
    clear_user_and_cart_data(redis_conn, user2_id)

    # Create users
    print("\n--- Creating Users ---")
    create_user(redis_conn, user1_id, "Alice Wonderland", "alice@example.com")
    create_user(redis_conn, user2_id, "Bob The Builder", "bob@example.com")

    # Add items to Alice's cart
    print("\n--- Alice's Cart Operations ---")
    add_item_to_cart(redis_conn, user1_id, "sku:book-thriller-001", 1)
    add_item_to_cart(redis_conn, user1_id, "sku:gadget-mouse-wireless", 2)
    add_item_to_cart(redis_conn, user1_id, "sku:book-thriller-001", 1) # Add another one of the same book

    # Add items to Bob's cart
    print("\n--- Bob's Cart Operations ---")
    add_item_to_cart(redis_conn, user2_id, "sku:tool-hammer-claw", 1)
    add_item_to_cart(redis_conn, user2_id, "sku:gadget-mouse-wireless", 1) # Bob also buys a mouse

    # Display current carts
    print("\n--- Displaying Cart Contents ---")
    alice_cart = get_cart_contents(redis_conn, user1_id)
    display_cart(user1_id, alice_cart)

    bob_cart = get_cart_contents(redis_conn, user2_id)
    display_cart(user2_id, bob_cart)

    # Remove items from Alice's cart
    print("\n--- Alice's Cart Modifications ---")
    remove_item_from_cart(redis_conn, user1_id, "sku:gadget-mouse-wireless", 1) # Remove one mouse
    remove_item_from_cart(redis_conn, user1_id, "sku:non-existent-item-000", 1) # Try removing an item not in cart
    remove_item_from_cart(redis_conn, user1_id, "sku:book-thriller-001", 5) # Try removing more books than exist (should clear the item)

    # Display Alice's cart again after modifications
    print("\n--- Alice's Cart After Modifications ---")
    alice_cart_updated = get_cart_contents(redis_conn, user1_id)
    display_cart(user1_id, alice_cart_updated)

    # Bob's cart should be unchanged
    print("\n--- Bob's Cart (Should be Unchanged) ---")
    bob_cart_unchanged = get_cart_contents(redis_conn, user2_id)
    display_cart(user2_id, bob_cart_unchanged)

    # Optional: Final cleanup after the demo
    # print("\n--- Final Cleanup of Demo Users ---")
    # clear_user_and_cart_data(redis_conn, user1_id)
    # clear_user_and_cart_data(redis_conn, user2_id)

    print("\nAssessment script finished successfully.")