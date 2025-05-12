import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Any, Optional


def create_tables(conn: sqlite3.Connection):
    """Create all the necessary tables in the SQLite database"""
    cursor = conn.cursor()

    # Create Events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create Stores table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stores (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    ''')

    # Create Orders table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        store_id TEXT NOT NULL,
        event_id INTEGER NOT NULL,
        reference_order_gid TEXT,
        reference_order_name TEXT,
        placed_at TIMESTAMP,
        currency TEXT NOT NULL,
        channel TEXT,
        subtotal INTEGER,
        discount INTEGER,
        total INTEGER,
        note TEXT,
        source TEXT,
        source_id TEXT,
        version INTEGER,
        weight INTEGER,
        is_manual BOOLEAN,
        is_test BOOLEAN,
        risk TEXT,
        tax_included BOOLEAN,
        discount_code TEXT,
        discount_type TEXT,
        FOREIGN KEY (store_id) REFERENCES stores(id),
        FOREIGN KEY (event_id) REFERENCES events(id)
    )
    ''')

    # Create LineItems table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS line_items (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        product_id TEXT,
        variant_id TEXT,
        sku TEXT,
        quantity INTEGER,
        pricing_quantity INTEGER,
        reference TEXT,
        reference_line_item_gid TEXT,
        parent_line_item_id TEXT,
        group_identifier TEXT,
        weight INTEGER,
        subtotal INTEGER,
        discount INTEGER,
        total INTEGER,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create TaxLines table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tax_lines (
        id TEXT PRIMARY KEY,
        parent_type TEXT NOT NULL,  -- 'line_item', 'shipping_line', etc.
        parent_id TEXT NOT NULL,
        name TEXT,
        rate TEXT,
        rate_type TEXT,
        amount INTEGER,
        currency TEXT,
        reference TEXT
    )
    ''')

    # Create ShippingLines table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipping_lines (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        name TEXT,
        handle TEXT,
        reference TEXT,
        amount INTEGER,
        currency TEXT,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create Addresses table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,  -- 'billing', 'shipping'
        order_id TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        phone TEXT,
        line1 TEXT,
        line2 TEXT,
        line3 TEXT,
        city TEXT,
        county TEXT,
        country TEXT,
        postcode TEXT,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create CustomerDetails table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customer_details (
        order_id TEXT PRIMARY KEY,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        customer_reference TEXT,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create Charges table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS charges (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        gateway TEXT,
        gateway_reference TEXT,
        gateway_payment_method_reference TEXT,
        payment_method_id TEXT,
        reference TEXT,
        status TEXT,
        amount INTEGER,
        currency TEXT,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create DiscountCodes table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS discount_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        code TEXT NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create AppliedDiscounts table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS applied_discounts (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        amount INTEGER,
        code TEXT,
        reference TEXT,
        title TEXT,
        type TEXT,
        value REAL,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
    ''')

    # Create AppliedDiscountTargets table for the complex "appliesTo" structure
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS applied_discount_targets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discount_id TEXT NOT NULL,
        target_type TEXT,
        variant_product_id TEXT,
        variant_variant_id TEXT,
        FOREIGN KEY (discount_id) REFERENCES applied_discounts(id)
    )
    ''')

    conn.commit()


def insert_event(conn: sqlite3.Connection, event_name: str) -> int:
    """Insert an event and return its ID"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO events (event_name) VALUES (?)",
        (event_name,)
    )
    conn.commit()
    return cursor.lastrowid


def insert_store(conn: sqlite3.Connection, store: Dict[str, Any]) -> None:
    """Insert store information"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO stores (id, name) VALUES (?, ?)",
        (store["id"], store["name"])
    )
    conn.commit()


def insert_order(conn: sqlite3.Connection, order: Dict[str, Any], event_id: int, store_id: str) -> None:
    """Insert order and all its related information"""
    cursor = conn.cursor()

    # Extract reference data
    reference_order_gid = None
    reference_order_name = None
    if "reference" in order and order["reference"]:
        reference_order_gid = order["reference"].get("orderGid")
        reference_order_name = order["reference"].get("orderName")

    # Extract amounts
    subtotal = None
    discount = None
    total = None
    if "amounts" in order and order["amounts"]:
        subtotal = order["amounts"].get("subtotal")
        discount = order["amounts"].get("discount")
        total = order["amounts"].get("total")

    # Convert ISO date string to datetime object
    placed_at = None
    if "placedAt" in order and order["placedAt"]:
        try:
            placed_at = datetime.fromisoformat(order["placedAt"])
        except (ValueError, TypeError):
            # Handle invalid date format
            pass

    # Insert main order data
    cursor.execute('''
    INSERT OR REPLACE INTO orders (
        id, store_id, event_id, reference_order_gid, reference_order_name,
        placed_at, currency, channel, subtotal, discount, total, note,
        source, source_id, version, weight, is_manual, is_test, risk,
        tax_included, discount_code, discount_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        order["orderId"], store_id, event_id, reference_order_gid, reference_order_name,
        placed_at, order.get("currency"), order.get("channel"), subtotal, discount, total,
        order.get("note"), order.get("source"), order.get("sourceId"), order.get("version"),
        order.get("weight"), order.get("isManual", False), order.get("isTest", False),
        order.get("risk"), order.get("taxIncluded", False), order.get("discountCode"),
        order.get("discountType")
    ))

    # Insert customer details
    if "customerDetails" in order and order["customerDetails"]:
        customer = order["customerDetails"]
        cursor.execute('''
        INSERT INTO customer_details (order_id, email, first_name, last_name, customer_reference)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            order["orderId"], customer.get("email"), customer.get("firstName"),
            customer.get("lastName"), order.get("customerReference")
        ))

    # Insert billing address
    if "billingDetails" in order and order["billingDetails"]:
        billing = order["billingDetails"]
        address = billing.get("address", {})
        cursor.execute('''
        INSERT INTO addresses (
            type, order_id, first_name, last_name, company, phone,
            line1, line2, line3, city, county, country, postcode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            "billing", order["orderId"], billing.get("firstName"), billing.get("lastName"),
            billing.get("company"), billing.get("phone"), address.get("line1"),
            address.get("line2"), address.get("line3"), address.get("city"),
            address.get("county"), address.get("country"), address.get("postcode")
        ))

    # Insert shipping address
    if "shippingDetails" in order and order["shippingDetails"]:
        shipping = order["shippingDetails"]
        address = shipping.get("address", {})
        cursor.execute('''
        INSERT INTO addresses (
            type, order_id, first_name, last_name, company, phone,
            line1, line2, line3, city, county, country, postcode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            "shipping", order["orderId"], shipping.get("firstName"), shipping.get("lastName"),
            shipping.get("company"), shipping.get("phone"), address.get("line1"),
            address.get("line2"), address.get("line3"), address.get("city"),
            address.get("county"), address.get("country"), address.get("postcode")
        ))

    # Insert line items
    if "lineItems" in order and order["lineItems"]:
        for item in order["lineItems"]:
            # Get reference_line_item_gid
            reference_line_item_gid = None
            if "references" in item and item["references"]:
                reference_line_item_gid = item["references"].get("lineItemGid")

            # Get amounts
            item_subtotal = None
            item_discount = None
            item_total = None
            if "amounts" in item and item["amounts"]:
                item_subtotal = item["amounts"].get("subtotal")
                item_discount = item["amounts"].get("discount")
                item_total = item["amounts"].get("total")

            cursor.execute('''
            INSERT INTO line_items (
                id, order_id, product_id, variant_id, sku, quantity, pricing_quantity,
                reference, reference_line_item_gid, parent_line_item_id, group_identifier,
                weight, subtotal, discount, total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item["id"], order["orderId"], item.get("productId"), item.get("variantId"),
                item.get("sku"), item.get("quantity"), item.get("pricingQuantity"),
                item.get("reference"), reference_line_item_gid, item.get("parentLineItemId"),
                item.get("groupIdentifier"), item.get("weight"), item_subtotal,
                item_discount, item_total
            ))

            # Insert tax lines for line items
            if "taxLines" in item and item["taxLines"]:
                for tax in item["taxLines"]:
                    cursor.execute('''
                    INSERT INTO tax_lines (
                        id, parent_type, parent_id, name, rate, rate_type, amount, currency, reference
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tax["id"], "line_item", item["id"], tax.get("name"), tax.get("rate"),
                        tax.get("rateType"), tax.get("amount"), tax.get("currency"), tax.get("reference")
                    ))

    # Insert shipping lines
    if "shippingLines" in order and order["shippingLines"]:
        for shipping_line in order["shippingLines"]:
            cursor.execute('''
            INSERT INTO shipping_lines (
                id, order_id, name, handle, reference, amount, currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                shipping_line["id"], order["orderId"], shipping_line.get("name"),
                shipping_line.get("handle"), shipping_line.get("reference"),
                shipping_line.get("amount"), shipping_line.get("currency")
            ))

            # Insert tax lines for shipping lines
            if "taxLines" in shipping_line and shipping_line["taxLines"]:
                for tax in shipping_line["taxLines"]:
                    cursor.execute('''
                    INSERT INTO tax_lines (
                        id, parent_type, parent_id, name, rate, rate_type, amount, currency, reference
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tax["id"], "shipping_line", shipping_line["id"], tax.get("name"),
                        tax.get("rate"), tax.get("rateType"), tax.get("amount"),
                        tax.get("currency"), tax.get("reference")
                    ))

    # Insert charges
    if "charges" in order and order["charges"]:
        for charge in order["charges"]:
            cursor.execute('''
            INSERT INTO charges (
                id, order_id, gateway, gateway_reference, gateway_payment_method_reference,
                payment_method_id, reference, status, amount, currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                charge["id"], order["orderId"], charge.get("gateway"), charge.get("gatewayReference"),
                charge.get("gatewayPaymentMethodReference"), charge.get("paymentMethodId"),
                charge.get("reference"), charge.get("status"), charge.get("amount"),
                charge.get("currency")
            ))

    # Insert discount codes
    if "discountCodes" in order and order["discountCodes"]:
        for discount_code in order["discountCodes"]:
            cursor.execute('''
            INSERT INTO discount_codes (order_id, code)
            VALUES (?, ?)
            ''', (order["orderId"], discount_code.get("code")))

    # Insert applied discounts
    if "appliedDiscounts" in order and order["appliedDiscounts"]:
        for discount in order["appliedDiscounts"]:
            cursor.execute('''
            INSERT INTO applied_discounts (
                id, order_id, amount, code, reference, title, type, value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                discount["id"], order["orderId"], discount.get("amount"), discount.get("code"),
                discount.get("reference"), discount.get("title"), discount.get("type"),
                discount.get("value")
            ))

            # Handle the complex "appliesTo" structure
            if "appliesTo" in discount and discount["appliesTo"]:
                applies_to = discount["appliesTo"]
                target_type = applies_to.get("targetType")

                if target_type == "variant" and "target" in applies_to and "variants" in applies_to["target"]:
                    for variant in applies_to["target"]["variants"]:
                        cursor.execute('''
                        INSERT INTO applied_discount_targets (
                            discount_id, target_type, variant_product_id, variant_variant_id
                        ) VALUES (?, ?, ?, ?)
                        ''', (
                            discount["id"], target_type, variant.get("productId"), variant.get("variantId")
                        ))

    conn.commit()


def process_json_data(json_file_path: str, db_file_path: str) -> None:
    """Process JSON data and insert into SQLite database"""
    # Create or connect to SQLite database
    conn = sqlite3.connect(db_file_path)

    # Create tables
    create_tables(conn)

    # Read and parse the JSON file
    try:
        with open(json_file_path, 'r') as file:
            # The file contains multiple JSON objects, so we need to parse them one by one
            text_data = file.read()

            # Clean the data - the provided file seems to have objects without a proper array wrapper
            # We'll add square brackets to make it a valid JSON array
            if not text_data.strip().startswith('['):
                text_data = '[' + text_data
            if not text_data.strip().endswith(']'):
                text_data = text_data + ']'

            # Parse the formatted JSON
            json_data = json.loads(text_data)

            # Process each event
            for event_data in json_data:
                event_name = event_data.get('event_name')
                payload = event_data.get('event_payload', {})

                if event_name and payload:
                    # Insert event
                    event_id = insert_event(conn, event_name)

                    # Insert store
                    store = payload.get('store', {})
                    if store:
                        insert_store(conn, store)
                        store_id = store.get('id')

                        # Insert order and related data
                        order = payload.get('order', {})
                        if order:
                            insert_order(conn, order, event_id, store_id)

        print(f"Data successfully processed and inserted into {db_file_path}")

    except Exception as e:
        print(f"Error processing JSON data: {e}")
    finally:
        conn.close()


def main():
    # File paths
    json_file_path = 'orders.json'  # Update this to your JSON file path
    db_file_path = 'orders_database.db'  # Output SQLite database file

    # Process the data
    process_json_data(json_file_path, db_file_path)

    # Verify the database contents
    print("\nDatabase Content Summary:")
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # Check number of records in main tables
    tables = ['events', 'stores', 'orders', 'line_items', 'customer_details',
              'addresses', 'shipping_lines', 'charges', 'discount_codes']

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"- {table}: {count} records")

        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
        rows = cursor.fetchall()
        print(f"  Sample rows:")
        for row in rows:
            print(f"    {row}")
        print("\n")

    conn.close()


if __name__ == "__main__":
    main()