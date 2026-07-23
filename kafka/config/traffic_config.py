"""
Role:
    Centralize the traffic simulation schedule for all data sources.

Responsibilities:
    - Define transaction rates by time period.
    - Simulate realistic daily traffic.
"""
from datetime import datetime

TRAFFIC_SCHEDULE = {

    "postgres": {
        "00:00-06:00": 10.0,
        "06:00-09:00": 3.0,
        "09:00-18:00": 1.0,
        "18:00-23:00": 3.0,
        "23:00-00:00": 10.0,
    },

    "stripe": {
        "00:00-06:00": 5.0,
        "06:00-09:00": 2.0,
        "09:00-18:00": 0.5,
        "18:00-23:00": 2.0,
        "23:00-00:00": 5.0,
    }

}

# Return the traffic delay for a data source.

def get_traffic_delay(source):
    current_time = datetime.now().strftime("%H:%M")

    for period, delay in TRAFFIC_SCHEDULE[source].items():

        start, end = period.split("-")

        if start <= current_time < end:
            return delay

    return TRAFFIC_SCHEDULE[source]["23:00-00:00"]