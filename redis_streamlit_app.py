"""
Streamlit demo app for the Redis assessment
-------------------------------------------

Features
~~~~~~~~
1. **Number Demo**
   • Inserts 1‑100 sequentially into a ZSET
   • Inserts 100 random unique integers into the same ZSET
   • Displays the entire set in descending order

2. **Shopping‑Cart Demo**
   • Adds / removes SKU quantities for a given user ID
   • Shows the cart contents as an editable table
   • Clears cart or checks out (moves to an `orders` stream)

Setup
~~~~~
1. Launch Redis locally (or point `REDIS_URL` env var to another host):

   docker run -d --name redis -p 6379:6379 redis:7

2. Install deps:

   pip install streamlit redis pandas

3. Run:

   streamlit run your_script_name.py
"""
import os
import random
import time
from typing import Dict, List, Any

import pandas as pd
import redis
import streamlit as st # Make sure this is one of the first imports

# ---------------------------------------------------------------------------
# Streamlit UI - Page Config MUST BE FIRST Streamlit command
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Redis Assessment Demo", layout="wide")

# ---------------------------------------------------------------------------
# Redis connection helper
# ---------------------------------------------------------------------------
@st.cache_resource # Cache the connection across reruns
def get_redis_connection() -> redis.Redis:
    """Attempts to connect to Redis and returns the connection object.
    Raises redis.exceptions.ConnectionError or other exceptions on failure."""
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(url, decode_responses=True)
    r.ping() # This will raise an exception if connection fails
    return r

# Attempt to connect to Redis AFTER page config
rdb = None # Initialize rdb
try:
    rdb = get_redis_connection()
    # Optional: A small, non-intrusive success indicator if needed, e.g., in sidebar later
    # st.sidebar.caption("Connected to Redis")
except redis.exceptions.ConnectionError as e:
    st.error(f"🔴 Failed to connect to Redis. Please ensure Redis is running and accessible. Error: {e}")
    st.stop() # Stop the app if Redis connection fails
except Exception as e: # Catch other potential errors during connection
    st.error(f"🔴 An unexpected error occurred while connecting to Redis: {e}")
    st.stop()

# If rdb is still None here, it means st.stop() was called.
# However, for linters and type checking, it's good practice to ensure rdb is not None
# for subsequent code if st.stop() wasn't guaranteed to exit immediately for the linter.
# In Streamlit's flow, st.stop() does halt further execution of this script run.

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NUM_KEY = "numbers"          # Sorted‑set key for number demo
CART_HASH_PREFIX = "cart:"     # Prefix for user cart Hashes
CART_TTL_PREFIX = "cart_ttl:"  # Prefix for cart TTL keys
CART_TTL_SEC = 30 * 60       # 30‑minute abandonment window
ORDER_STREAM = "orders"      # Stream for completed checkouts

def get_cart_key(uid: str) -> str:
    return f"{CART_HASH_PREFIX}{uid}"

def get_cart_ttl_key(uid: str) -> str:
    return f"{CART_TTL_PREFIX}{uid}"

# ---------------------------------------------------------------------------
# Helper functions - TTL Management
# ---------------------------------------------------------------------------
def _update_cart_ttl(uid: str):
    """Refreshes or removes cart TTL based on whether the cart is empty."""
    cart_key = get_cart_key(uid)
    ttl_key = get_cart_ttl_key(uid)
    if rdb.hlen(cart_key) > 0:
        rdb.setex(ttl_key, CART_TTL_SEC, "alive")
    else:
        rdb.delete(ttl_key) # Cart is empty, remove its TTL marker

# ---------------------------------------------------------------------------
# Helper functions - Number Demo
# ---------------------------------------------------------------------------
def insert_sequential():
    pipe = rdb.pipeline()
    for n in range(1, 101):
        pipe.zadd(NUM_KEY, {str(n): n}) # Store members as strings for consistency
    pipe.execute()


def insert_random():
    rand_vals = random.sample(range(101, 10_000), 100) # Sample from a different range
    pipe = rdb.pipeline()
    for v in rand_vals:
        pipe.zadd(NUM_KEY, {str(v): v})
    pipe.execute()


def get_numbers() -> List[int]:
    return [int(member) for member in rdb.zrevrange(NUM_KEY, 0, -1)]


# ---------------------------------------------------------------------------
# Helper functions - Shopping Cart
# ---------------------------------------------------------------------------
def add_to_cart(uid: str, sku: str, qty: int):
    """Increments quantity of an SKU in the cart."""
    if qty <= 0:
        st.sidebar.warning("Quantity must be positive to add.")
        return
    if not sku:
        st.sidebar.warning("SKU cannot be empty.")
        return

    cart_key = get_cart_key(uid)
    rdb.hincrby(cart_key, sku, qty)
    _update_cart_ttl(uid)
    st.sidebar.success(f"Added {qty} × {sku} to cart {uid}")


def get_cart(uid: str) -> Dict[str, int]:
    cart_key = get_cart_key(uid)
    raw_cart = rdb.hgetall(cart_key)
    return {k: int(v) for k, v in raw_cart.items()}


def clear_cart(uid: str):
    cart_key = get_cart_key(uid)
    ttl_key = get_cart_ttl_key(uid)
    rdb.delete(cart_key, ttl_key) # Delete both cart and its TTL marker
    st.sidebar.warning(f"Cart for user {uid} cleared.")


def checkout(uid: str) -> bool:
    cart = get_cart(uid)
    if not cart:
        st.sidebar.info(f"Cart for user {uid} is empty, nothing to checkout.")
        return False

    order_details = {"user_id": uid, "timestamp": str(time.time())} # ensure timestamp is string for xadd
    for item_sku, item_qty in cart.items():
        order_details[f"item_{item_sku}"] = str(item_qty)

    rdb.xadd(ORDER_STREAM, order_details)
    clear_cart(uid) # Clear cart after successful checkout
    st.sidebar.success(f"User {uid} checked out successfully! Order sent to stream '{ORDER_STREAM}'.")
    return True

# ---------------------------------------------------------------------------
# Streamlit UI - Main Application
# ---------------------------------------------------------------------------
st.title("🚀 Redis Assessment Demo App")

tab_numbers, tab_cart = st.tabs(["📊 Number Demo", "🛒 Shopping Cart"])

# --- Numbers demo -----------------------------------------------------------
with tab_numbers:
    st.header("Sorted-Set Number Demo")
    st.markdown(
        """
        - Inserts numbers 1-100 (score equals value).
        - Inserts 100 random unique integers (score equals value) into the *same* set.
        - Displays the combined set in descending order by value.
        """
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Insert 1-100 into ZSET", key="btn_seq"):
            insert_sequential()
            st.success("Inserted 1-100 into ZSET!")

    with col2:
        if st.button("Insert 100 random numbers into ZSET", key="btn_rand"):
            insert_random()
            st.success("Inserted 100 random numbers into ZSET!")

    with col3:
        if st.button("Clear Number ZSET", key="btn_clear_zset"):
            rdb.delete(NUM_KEY)
            st.warning(f"Cleared the ZSET '{NUM_KEY}'.")

    st.subheader("Current numbers in ZSET (descending order)")
    numbers_from_zset = get_numbers()
    if numbers_from_zset:
        st.code(numbers_from_zset[:], language="python")
        
        st.subheader("Sparkline of top 50 numbers (values as inserted)")
        df_numbers = pd.DataFrame({"value": numbers_from_zset[:50][::-1]}) # Reversed for ascending line chart
        st.line_chart(df_numbers.set_index(pd.RangeIndex(start=1, stop=len(df_numbers) + 1)))
    else:
        st.info(f"The ZSET '{NUM_KEY}' is empty.")


# --- Cart demo --------------------------------------------------------------
with tab_cart:
    st.header("Shopping Cart Demo with Editable Table")

    # --- Sidebar for User Input and Actions ---
    st.sidebar.title("🛒 Cart Controls")
    # Use session_state to persist User ID across reruns and edits
    if 'cart_user_id_input' not in st.session_state:
        st.session_state.cart_user_id_input = "user123"

    uid_input = st.sidebar.text_input(
        "User ID",
        value=st.session_state.cart_user_id_input,
        key="sidebar_uid_input_widget" # Unique key for the widget
    )
    # Update session state if the widget's value changes
    if uid_input != st.session_state.cart_user_id_input:
        st.session_state.cart_user_id_input = uid_input
        # No st.rerun() here, let Streamlit handle natural reruns or rely on button actions

    st.sidebar.subheader("Add Item Manually")
    sku_input = st.sidebar.text_input("SKU", value="ITEM001", key="cart_sku_widget")
    qty_input = st.sidebar.number_input("Quantity to Add", min_value=1, value=1, step=1, key="cart_qty_widget")

    if st.sidebar.button("➕ Add to Cart", key="cart_add_button"):
        if st.session_state.cart_user_id_input and sku_input: # Use session state for uid
            add_to_cart(st.session_state.cart_user_id_input, sku_input, qty_input)
            st.rerun() # Rerun to ensure cart and editor reflect the change
        else:
            st.sidebar.error("User ID and SKU cannot be empty.")

    st.sidebar.markdown("---")
    if st.sidebar.button("🗑️ Clear Entire Cart", key="cart_clear_button"):
        if st.session_state.cart_user_id_input:
            clear_cart(st.session_state.cart_user_id_input)
            st.rerun()
        else:
            st.sidebar.error("User ID cannot be empty.")

    if st.sidebar.button("✅ Checkout", key="cart_checkout_button"):
        if st.session_state.cart_user_id_input:
            checkout_successful = checkout(st.session_state.cart_user_id_input)
            if checkout_successful:
                st.rerun()
        else:
            st.sidebar.error("User ID cannot be empty.")

    # --- Main Cart Display Area ---
    active_uid = st.session_state.cart_user_id_input # Use the UID from session state
    st.subheader(f"Cart for User: **{active_uid}**")
    current_cart_redis = get_cart(active_uid)

    if not current_cart_redis:
        st.info("Cart is currently empty.")
        # Provide a way to add rows if cart is empty and user wants to use data_editor
        if st.button("➕ Add first item via table editor", key="add_first_item_editor_btn"):
            # Create a dummy empty DataFrame for the editor to start with one empty row
            cart_list_for_editor = [{"SKU": "", "Quantity": 1}]
            # No need to set current_cart_redis here, just providing structure for editor
        else:
            cart_list_for_editor = [] # No items, no dummy row
    else:
        cart_list_for_editor = [{"SKU": sku, "Quantity": quantity} for sku, quantity in current_cart_redis.items()]

    cart_df_for_editor = pd.DataFrame(cart_list_for_editor)

    st.markdown("Edit quantities below (set to 0 or delete row to remove item):")
    edited_df = st.data_editor(
        cart_df_for_editor,
        key=f"cart_editor_{active_uid}", # Dynamic key based on user ID
        num_rows="dynamic",
        column_config={
            "SKU": st.column_config.TextColumn("SKU", required=True, width="large"),
            "Quantity": st.column_config.NumberColumn("Quantity", min_value=0, step=1, required=True),
        },
        hide_index=True,
        use_container_width=True
    )

    edited_cart_dict = {}
    if edited_df is not None:
        for _index, row in edited_df.iterrows(): # Use _index to denote unused loop variable
            sku = row.get("SKU")
            quantity = row.get("Quantity")
            # Ensure SKU is not None/NaN and is a non-empty string after potential strip
            if sku and pd.notna(sku) and isinstance(sku, str) and sku.strip():
                sku_clean = sku.strip()
                if quantity is not None and pd.notna(quantity):
                    edited_cart_dict[sku_clean] = edited_cart_dict.get(sku_clean, 0) + int(quantity)
                # If quantity is None/NaN for an existing SKU, it might mean the user cleared the cell.
                # The logic below will treat it as if the SKU wasn't in the editor, effectively removing it if it was in Redis.
            # If SKU is empty or None after editing, that row is ignored for Redis update.

    pipe = rdb.pipeline()
    made_changes_in_editor = False

    # 1. Items to update or add from the editor
    for sku, new_qty in edited_cart_dict.items():
        original_qty = current_cart_redis.get(sku)

        if new_qty <= 0:
            if original_qty is not None:
                pipe.hdel(get_cart_key(active_uid), sku)
                made_changes_in_editor = True
        elif original_qty is None or new_qty != original_qty: # New item or changed quantity
            pipe.hset(get_cart_key(active_uid), sku, new_qty)
            made_changes_in_editor = True

    # 2. Items to remove (were in Redis cart but are no longer in edited_cart_dict)
    for sku in current_cart_redis:
        if sku not in edited_cart_dict:
            pipe.hdel(get_cart_key(active_uid), sku)
            made_changes_in_editor = True

    if made_changes_in_editor:
        pipe.execute()
        _update_cart_ttl(active_uid)
        st.toast("Cart updated from editor changes!", icon="🔁")
        st.rerun()


# In your Streamlit app, e.g., in a new tab or section:
st.subheader("Recent Orders (from Stream)")
try:
    # Fetch, for example, the last 10 orders.
    # XREVRANGE returns a list of [message_id, [field1, value1, field2, value2, ...]]
    # Stream messages are lists of [message_id, fields_dict]
    # However, redis-py's xrevrange/xrange can return it parsed differently
    # depending on version or if it's a pipeline.
    # Let's assume it returns a list of tuples: (message_id, fields_dictionary)
    
    # The redis-py `xrevrange` and `xrange` methods return a list of tuples,
    # where each tuple is (message_id, fields_dict).
    recent_orders_raw = rdb.xrevrange(ORDER_STREAM, count=10) # Get last 10

    if recent_orders_raw:
        orders_for_display = []
        for message_id, fields_dict in recent_orders_raw:
            order_data = {"Order ID (Stream Msg ID)": message_id}
            order_data.update(fields_dict) # Add all fields from the stream message
            orders_for_display.append(order_data)
        
        df_orders = pd.DataFrame(orders_for_display)
        st.dataframe(df_orders)
    else:
        st.info("No orders found in the stream yet.")
except Exception as e:
    st.error(f"Error fetching orders from stream: {e}")