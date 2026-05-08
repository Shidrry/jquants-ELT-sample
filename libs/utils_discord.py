import os
import requests

from libs.utils import access_secret, get_project_id

DISCORD_API_BASE = "https://discord.com/api/v10"


def get_discord_webhook_url(secret_base: str = "discord-webhook-url") -> str:
    env_name = os.environ.get("ENV_NAME", "dev")
    # レポート通知のみ env 別に分ける（エラー通知 discord-webhook-url は共通）
    if env_name != "prod" and secret_base != "discord-webhook-url":
        secret_base = f"{secret_base}-{env_name}"
    return access_secret(secret_base, get_project_id())


def notify_discord(message: str, webhook_url: str | None = None) -> None:
    if webhook_url is None:
        webhook_url = get_discord_webhook_url()
    resp = requests.post(webhook_url, json={"content": message}, timeout=10)
    resp.raise_for_status()


def get_discord_bot_token() -> str:
    return access_secret("discord-bot-token", get_project_id())


def get_discord_channel_id() -> str:
    env_name = os.environ.get("ENV_NAME", "dev")
    secret_id = "discord-channel-id" if env_name == "prod" else f"discord-channel-id-{env_name}"
    return access_secret(secret_id, get_project_id())


def _post_message(webhook_url: str, content: str) -> str:
    """Webhook でメッセージを投稿し message_id を返す。"""
    resp = requests.post(
        webhook_url,
        params={"wait": "true"},
        json={"content": content},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def _create_thread(channel_id: str, message_id: str, name: str, bot_token: str) -> str:
    """メッセージからスレッドを作成し thread_id を返す。"""
    resp = requests.post(
        f"{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/threads",
        headers={"Authorization": f"Bot {bot_token}"},
        json={"name": name},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def notify_discord_report(
    title: str,
    summary: str,
    detail: str,
    webhook_url: str,
    env_name: str = "prod",
    date: str = "",
    channel_id: str | None = None,
    bot_token: str | None = None,
    references: str = "",
) -> None:
    """
    サマリーを通常投稿し、詳細をスレッドに続ける。
    参考リンク (references) がある場合はスレッド末尾に別メッセージとして投稿する。
    channel_id / bot_token が未指定の場合は Secret Manager から取得する。
    2000 文字制限に対応するため必要に応じて分割する。
    env_name が "prod" 以外の場合はタイトルにプレフィックスを付与する。
    """
    env_prefix = f"[{env_name}] " if env_name != "prod" else ""
    date_suffix = f"（{date}）" if date else ""
    header = f"**{env_prefix}{title}{date_suffix}**\n\n"

    summary_msg = header + (summary or "（サマリーなし）")
    message_id = _post_message(webhook_url, summary_msg)

    if not detail and not references:
        return

    if channel_id is None:
        channel_id = get_discord_channel_id()
    if bot_token is None:
        bot_token = get_discord_bot_token()

    thread_name = f"{env_prefix}{title}{date_suffix}"[:100]
    thread_id = _create_thread(channel_id, message_id, thread_name, bot_token)

    thread_webhook = webhook_url + f"?thread_id={thread_id}"
    max_body = 1900

    sections = []
    if detail:
        sections.append(detail)
    if references:
        sections.append("📎 **参考リンク**\n" + references)

    for i, section in enumerate(sections):
        chunks = _split_by_paragraph(section, max_body) or (["（詳細なし）"] if i == 0 else [])
        for j, chunk in enumerate(chunks):
            notify_discord(chunk if (i == 0 and j == 0) else "\n" + chunk, thread_webhook)


def _split_by_paragraph(text: str, max_chars: int) -> list[str]:
    """段落（空行）区切りで text を max_chars 以内のチャンクに分割する。
    1段落が max_chars を超える場合のみ改行で切る。
    """
    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current = ""

    for para in paragraphs:
        block = (current + "\n\n" + para).lstrip("\n") if current else para
        if len(block) <= max_chars:
            current = block
        else:
            if current:
                chunks.append(current)
            # 1段落が上限超えなら改行単位でさらに分割
            if len(para) > max_chars:
                for line in para.splitlines(keepends=True):
                    if len((current + line) if current else line) <= max_chars:
                        current = (current + line) if current else line
                    else:
                        if current:
                            chunks.append(current)
                        current = line
            else:
                current = para

    if current:
        chunks.append(current)

    return chunks
