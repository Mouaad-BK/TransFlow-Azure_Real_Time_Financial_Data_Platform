import random

# ==========================
# Stripe Inputs
# ==========================

CURRENCIES = [
    "usd",
    "eur"
]

STRIPE_PAYMENT_METHODS = [
    "pm_card_visa",
    "pm_card_mastercard",
    "pm_card_amex",
    "pm_card_discover"
]

CARD_USAGE_METHODS = [
    "Chip",
    "Swipe",
    "Contactless",
    "Online"
]

# ==========================
# Customer
# ==========================

GENDERS = [
    "Male",
    "Female"
]

COUNTRIES = [
    "United States",
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Portugal",
    "Belgium",
    "Netherlands",
    "Switzerland",
    "Austria",
    "Luxembourg",
    "Ireland",
    "Poland",
    "Czech Republic",
    "Sweden",
    "Norway",
    "Denmark",
    "Finland"
]

# ==========================
# Merchant
# ==========================

LOCATIONS = [
    ("New York", "New York"),
    ("Los Angeles", "California"),
    ("Chicago", "Illinois"),
    ("Houston", "Texas"),
    ("Miami", "Florida"),
    ("Seattle", "Washington"),
    ("Boston", "Massachusetts"),

    ("Paris", "Île-de-France"),
    ("Lyon", "Auvergne-Rhône-Alpes"),
    ("Marseille", "Provence-Alpes-Côte d'Azur"),

    ("Berlin", "Berlin"),
    ("Munich", "Bavaria"),
    ("Hamburg", "Hamburg"),

    ("Madrid", "Community of Madrid"),
    ("Barcelona", "Catalonia"),
    ("Valencia", "Valencian Community"),

    ("Rome", "Lazio"),
    ("Milan", "Lombardy"),
    ("Naples", "Campania"),

    ("Amsterdam", "North Holland"),
    ("Rotterdam", "South Holland"),

    ("Brussels", "Brussels"),

    ("Lisbon", "Lisbon"),
    ("Porto", "Porto"),

    ("Vienna", "Vienna"),

    ("Zurich", "Zurich"),
    ("Geneva", "Geneva"),

    ("Stockholm", "Stockholm"),
    ("Oslo", "Oslo"),
    ("Copenhagen", "Capital Region"),
    ("Helsinki", "Uusimaa"),
    ("Dublin", "Leinster"),
    ("Warsaw", "Masovian"),
    ("Prague", "Prague")
]

MERCHANT_CATEGORY_CODES = [
    5411,
    5541,
    5812,
    5732,
    5912,
    5999,
    4111,
    5311
]

# ==========================
# Stripe Inputs
# ==========================

def generate_amount():
    return random.randint(100, 500000)


def generate_currency():
    return random.choice(CURRENCIES)


def generate_stripe_payment_method():
    return random.choice(STRIPE_PAYMENT_METHODS)


# ==========================
# Customer
# ==========================

def generate_customer(customer_id):

    return {
        "customer_id": customer_id,
        "age": random.randint(18, 80),
        "gender": random.choice(GENDERS),
        "country": random.choice(COUNTRIES),
        "annual_income": random.randint(20_000, 200_000),
        "credit_score": random.randint(300, 850),
        "number_of_cards": random.randint(1, 5)
    }


# ==========================
# Card
# ==========================

def generate_card_limit():
    return random.randint(500, 100000)


def generate_card(card_id, card_brand, card_type):

    return {
        "card_id": card_id,
        "card_brand": card_brand,
        "card_type": card_type,
        "has_chip": random.choice([True, False]),
        "card_limit": generate_card_limit()
    }


# ==========================
# Transaction
# ==========================

def generate_transaction():

    city, state = random.choice(LOCATIONS)

    return {
        "card_usage_method": random.choice(CARD_USAGE_METHODS),

        "merchant_id": f"merchant_{random.randint(100000,999999)}",
        "merchant_city": city,
        "merchant_state": state,
        "postal_code": random.randint(10000, 99999),
        "merchant_category_code": random.choice(MERCHANT_CATEGORY_CODES)
    }