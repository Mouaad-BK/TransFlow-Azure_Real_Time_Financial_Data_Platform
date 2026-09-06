import os
import json
from dotenv import load_dotenv
import stripe

from .generator import (
    generate_amount,
    generate_currency,
    generate_stripe_payment_method,
    generate_customer,
    generate_card,
    generate_transaction
)

# ==========================================================
# Configuration
# ==========================================================

load_dotenv()

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")


def extract():

    # -----------------------------
    # Stripe Customer
    # -----------------------------

    stripe_customer = stripe.Customer.create()

    # -----------------------------
    # Stripe Payment Method
    # -----------------------------

    stripe_payment_method = stripe.PaymentMethod.create(
        type="card",
        card={
            "token": "tok_visa"
        }
    )

    # -----------------------------
    # Stripe Payment
    # -----------------------------

    payment = stripe.PaymentIntent.create(
        amount=generate_amount(),
        currency=generate_currency(),
        customer=stripe_customer.id,
        payment_method=generate_stripe_payment_method(),
        confirm=True,
        automatic_payment_methods={
            "enabled": True,
            "allow_redirects": "never"
        }
    )

    # -----------------------------
    # Customer
    # -----------------------------

    customer = generate_customer(
        customer_id=stripe_customer.id
    )

    # -----------------------------
    # Card
    # -----------------------------

    card = generate_card(
        card_id=stripe_payment_method.id,
        card_brand=stripe_payment_method.card.brand,
        card_type=stripe_payment_method.card.funding
    )

    # -----------------------------
    # Transaction
    # -----------------------------

    transaction = generate_transaction()

    transaction["transaction_id"] = payment.id
    transaction["transaction_timestamp"] = payment.created
    transaction["amount"] = payment.amount
    transaction["currency"] = payment.currency
    transaction["transaction_error"] = payment.last_payment_error

    # -----------------------------
    # Event
    # -----------------------------

    event = {
        "customer": customer,
        "card": card,
        "transaction": transaction
    }

    return event


if __name__ == "__main__":

    
    print(json.dumps(extract(), indent=4, default=str))