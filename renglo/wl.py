"""Tenant white-label branding for controllers.

The pack import name is always ``wl``. Product copy lives there
(``appName``, invite email strings, logo files). ``WL_NAME`` is the
environment id and must not be used as a display name.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

DEFAULT_APP_NAME = "Renglo"
LOGO_CID = "wl-logo"

DEFAULT_INVITE = {
    "subject": "You have been invited to {team} on {appName}",
    "subjectHint": "You have been invited to a team on {appName}",
    "heading": "Hello from {appName}",
    "intro": "You have been invited by {inviter} to team {team}.",
    "code": "Your invite code is: {code}",
    "link": "Follow this link:",
}


class _Blank(dict):
    def __missing__(self, key: str) -> str:
        return ""


@dataclass(frozen=True)
class InviteEmail:
    subject: str
    body_text: str
    body_html: str
    logo_path: Path | None
    logo_cid: str | None


def load_wl_module():
    try:
        import wl
    except ImportError:
        return None
    return wl


def app_name() -> str:
    pack = load_wl_module()
    if pack is None:
        return DEFAULT_APP_NAME
    name = getattr(pack, "app_name", None)
    if name is None:
        locales = getattr(pack, "locales", None) or {}
        en = locales.get("en") if isinstance(locales, dict) else None
        if isinstance(en, dict):
            name = en.get("appName")
    text = str(name or "").strip()
    return text or DEFAULT_APP_NAME


def small_logo_path() -> Path | None:
    pack = load_wl_module()
    if pack is None:
        return None
    path = getattr(pack, "small_logo_path", None)
    if path is None:
        return None
    resolved = Path(path)
    return resolved if resolved.is_file() else None


def invite_strings(locale: str = "en") -> dict[str, str]:
    merged = dict(DEFAULT_INVITE)
    pack = load_wl_module()
    if pack is None:
        return merged
    locales = getattr(pack, "locales", None) or {}
    loc = locales.get(locale) if isinstance(locales, dict) else None
    email = loc.get("email") if isinstance(loc, dict) else None
    invite = email.get("invite") if isinstance(email, dict) else None
    if isinstance(invite, dict):
        for key in DEFAULT_INVITE:
            value = invite.get(key)
            if isinstance(value, str) and value.strip():
                merged[key] = value
    return merged


def _format(template: str, **values: str) -> str:
    return template.format_map(_Blank(values))


def render_invite_email(
    *,
    inviter: str,
    team: str,
    code: str,
    link: str,
    locale: str = "en",
) -> InviteEmail:
    name = app_name()
    values = {
        "appName": name,
        "inviter": inviter,
        "team": team,
        "code": code,
        "link": link,
    }
    strings = invite_strings(locale)
    subject = _format(strings["subject"], **values)
    heading = _format(strings["heading"], **values)
    intro = _format(strings["intro"], **values)
    code_line = _format(strings["code"], **values)
    link_label = _format(strings["link"], **values)

    body_text = "\n".join(
        [
            heading,
            "",
            intro,
            code_line,
            link_label,
            link,
        ]
    )

    logo = small_logo_path()
    cid = LOGO_CID if logo is not None else None
    img = ""
    if cid:
        img = (
            f'<img src="cid:{cid}" alt="{html.escape(name)}" width="64" '
            'style="display:block;margin-bottom:16px" />'
        )

    body_html = (
        "<html><body>"
        f"{img}"
        f"<h1>{html.escape(heading)}</h1>"
        f"<h2>{html.escape(intro)}</h2>"
        f"<div>{html.escape(code_line)}</div>"
        f"<div>{html.escape(link_label)}</div>"
        f'<div><a href="{html.escape(link, quote=True)}">{html.escape(link)}</a></div>'
        "</body></html>"
    )
    return InviteEmail(
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        logo_path=logo,
        logo_cid=cid,
    )


def invite_inline_images(email: InviteEmail) -> list[tuple[Path, str]]:
    if email.logo_path is None or not email.logo_cid:
        return []
    return [(email.logo_path, email.logo_cid)]


def build_raw_email(
    sender: str,
    recipient: str,
    subject: str,
    body_text: str,
    body_html: str,
    inline_images: list[tuple[Path, str]] | None,
) -> bytes:
    """MIME message for SES send_raw_email (needed for CID logo attachments)."""
    related = MIMEMultipart("related")
    related["Subject"] = subject
    related["From"] = sender
    related["To"] = recipient

    alternative = MIMEMultipart("alternative")
    alternative.attach(MIMEText(body_text, "plain", "utf-8"))
    alternative.attach(MIMEText(body_html, "html", "utf-8"))
    related.attach(alternative)

    for path, cid in inline_images or []:
        image_path = Path(path)
        subtype = (image_path.suffix.lstrip(".") or "png").lower()
        if subtype == "jpg":
            subtype = "jpeg"
        image = MIMEImage(image_path.read_bytes(), _subtype=subtype)
        image.add_header("Content-ID", f"<{cid}>")
        image.add_header("Content-Disposition", "inline", filename=image_path.name)
        related.attach(image)

    return related.as_bytes()
