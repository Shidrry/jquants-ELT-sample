import os
import requests

from libs.utils import access_secret, get_project_id


def get_discord_webhook_url() -> str:
    secret_id = os.environ.get("DISCORD_WEBHOOK_SECRET", "discord-webhook-url")
    project_id = get_project_id()
    return access_secret(secret_id, project_id)


def notify_discord(message: str, webhook_url: str | None = None) -> None:
    if webhook_url is None:
        webhook_url = get_discord_webhook_url()
    resp = requests.post(webhook_url, json={"content": message}, timeout=10)
    resp.raise_for_status()
