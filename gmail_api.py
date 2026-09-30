"""
gmail_api.py  –  Gmail-based payment verification.

Flow:
  1. User submits UTR text (images are blocked at handler level).
  2. verify_payment() checks recent FamPay emails for that UTR + amount.
  3. If found, claim_payment() atomically registers the claim in the SHARED
     payments DB (cross-bot dedup) AND locally.
  4. mark_payment_claimed() archives the Gmail message so it's not scanned again.

Security:
  • Screenshots / images are rejected in handlers.py before reaching here.
  • UTR must appear in an actual FamPay email — fake UTRs fail.
  • Amount must match exactly — wrong-amount UTRs fail.
  • Once claimed (on any bot), the same UTR / email msg is rejected everywhere.
"""

import requests
import re
import logging
import os

logger = logging.getLogger(__name__)

CLIENT_ID     = os.environ.get("GMAIL_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN", "")
UPI_ID        = os.environ.get("UPI_ID", "sagarxpay@fam")


def get_access_token() -> str | None:
    token_url  = "https://oauth2.googleapis.com/token"
    token_data = {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    }
    try:
        res = requests.post(token_url, data=token_data, timeout=15)
        res.raise_for_status()
        return res.json().get("access_token")
    except Exception as e:
        logger.error(f"Failed to get Gmail access token: {e}")
        return None


async def verify_payment(
    utr_input: str,
    expected_amount: float,
) -> tuple[bool, str, str | None]:
    """
    Verify a payment by UTR against recent FamPay emails.

    Returns:
        (True,  "",          msg_id)  – valid payment found, not yet claimed
        (False, reason_str,  None)    – invalid or already claimed
    """
    utr = utr_input.strip()

    # Basic sanity check
    if not utr or len(utr) < 8:
        return False, "❌ Invalid UTR format. It must be at least 8 characters.", None

    # Check shared cross-bot DB before hitting Gmail API
    try:
        from payments_db import is_utr_claimed
        if await is_utr_claimed(utr):
            return False, "❌ This UTR has already been used to claim a subscription.", None
    except Exception as e:
        logger.warning(f"Shared DB UTR check skipped: {e}")

    access_token = get_access_token()
    if not access_token:
        return False, "❌ Payment verification service unavailable. Contact admin.", None

    headers  = {"Authorization": f"Bearer {access_token}"}
    # Search only FamPay notification emails in inbox
    query    = "from:no-reply@famapp.in in:inbox"
    list_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q={requests.utils.quote(query)}&maxResults=30"

    try:
        res      = requests.get(list_url, headers=headers, timeout=15)
        res.raise_for_status()
        messages = res.json().get("messages", [])
    except Exception as e:
        logger.error(f"Failed to fetch email list: {e}")
        return False, "❌ Could not access payment emails. Try again later.", None

    if not messages:
        return False, "❌ No recent FamPay payment emails found.", None

    for msg in messages[:30]:
        msg_id = msg["id"]

        # Check if this email was already claimed (locally + shared)
        try:
            import database as db
            if await db.check_payment_claimed(msg_id):
                continue
        except Exception:
            pass

        detail_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"
        try:
            detail_res = requests.get(detail_url, headers=headers, timeout=15)
            detail_res.raise_for_status()
            detail  = detail_res.json()
            snippet = detail.get("snippet", "")

            if utr in snippet:
                # UTR found — verify amount
                amount_match = re.search(r'₹\s?([0-9]+(?:\.[0-9]+)?)', snippet)
                if amount_match:
                    real_amount = float(amount_match.group(1))
                    if abs(real_amount - float(expected_amount)) < 0.01:
                        return True, "✅ Payment verified.", msg_id
                    else:
                        return (
                            False,
                            f"❌ Amount mismatch. Expected ₹{expected_amount:.0f}, "
                            f"found ₹{real_amount:.0f} in email.",
                            None,
                        )
                else:
                    return False, "❌ Could not read payment amount from email.", None

        except Exception as e:
            logger.error(f"Failed to fetch email {msg_id}: {e}")
            continue

    return False, "❌ UTR not found in recent payment emails. Check UTR and try again.", None


async def mark_payment_claimed(msg_id: str) -> bool:
    """
    Archive the Gmail message so it won't appear in future scans.
    Removes INBOX and UNREAD labels.
    """
    access_token = get_access_token()
    if not access_token:
        return False

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json",
    }
    url  = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}/modify"
    data = {"removeLabelIds": ["UNREAD", "INBOX"]}

    try:
        res = requests.post(url, headers=headers, json=data, timeout=15)
        res.raise_for_status()
        logger.info(f"✅ Email {msg_id} archived (removed from INBOX)")
        return True
    except Exception as e:
        logger.error(f"Failed to archive email {msg_id}: {e}")
        return False
