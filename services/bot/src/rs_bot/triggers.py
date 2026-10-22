"""The Triggers a member sees, by the name they read."""

from rs_domain import POPULARITY_SURGE

# Every Trigger with the name members see: in the Alert's title, in `/pings` and in `/setup roles`.
TRIGGER_LABELS = {POPULARITY_SURGE: "Popularity surge"}
