import os
import logging
from logging.handlers import SMTPHandler
from setting.inject import provision_inject_orm 

def _cfg_get(conf, key, default=None):
    try:
        val = getattr(conf, key)
        return default if val is None else val
    except Exception:
        try:
            return conf.get(key, default)
        except Exception:
            return default

def attach_error_email_handler(logger: logging.Logger, service_name: str = "default_service"):
    conf = provision_inject_orm()
    user = _cfg_get(conf, "EMAIL_USER")
    password = _cfg_get(conf, "EMAIL_PASS")
    admin_raw = _cfg_get(conf, "ADMIN_EMAIL", "")
    to_addrs = [a.strip() for a in admin_raw.split(",") if a.strip()]
    
    if not (user and password and to_addrs):
        logger.warning("[ALERT_MAIL] EMAIL_USER/EMAIL_PASS/ADMIN_EMAIL 미설정 → 메일 알림 비활성화")
        return

    host = "smtp.gmail.com"
    port = 587
    sender = user
    subject = f"[ALERT][{service_name}] 오류 발생"

    handler = SMTPHandler(
        mailhost=(host, port),
        fromaddr=sender,
        toaddrs=to_addrs,
        subject=subject,
        credentials=(user, password),
        secure=()  # STARTTLS
    )
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    logger.addHandler(handler)