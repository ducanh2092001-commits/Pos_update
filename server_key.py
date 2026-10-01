"""SERVER BẢN QUYỀN 24/7 (FastAPI) - treo lên Render cho app POS Ducanh Store.

Luồng hoạt động
---------------
1. App POS hiện mã VietQR động MB Bank (0551112075555 - LÊ ĐỨC ANH) với ĐÚNG số
   tiền của gói + nội dung ``KICHHOAT <MÃ MÁY>``.
2. Tiến trình nền của server (``asyncio.sleep(4)``) TỰ ĐỘNG cào lịch sử biến động
   số dư MB Bank (miễn phí). Thấy giao dịch TIỀN VÀO khớp ĐÚNG số tiền của 1 trong
   4 gói VÀ nội dung chứa ``KICHHOAT <mã máy>`` -> đẻ Key ``PREM-XXXX-XXXX``.
3. Server gửi Key qua Telegram BOT cho khách (chat_id đã liên kết với mã máy).
4. App POS bắn ``POST /verify_key`` -> server đối chiếu CSDL -> trả valid True/False.

Biến môi trường (đặt trên Render -> Environment)
------------------------------------------------
* ``TELEGRAM_BOT_TOKEN``   : token bot Telegram (BẮT BUỘC để gửi key).
* ``MB_USERNAME``/``MB_PASSWORD`` : tài khoản Internet Banking MB Bank của chủ shop
  (BẮT BUỘC để cào lịch sử). Nên tạo 1 tài khoản phụ chỉ để xem số dư.
* ``MB_ACCOUNT``           : số tài khoản nhận tiền (mặc định ``0551112075555``).
* ``MB_DEVICE_ID``         : (tuỳ chọn) device id cố định cho phiên đăng nhập MB.
* ``SERVER_SECRET``        : (tuỳ chọn) khoá bí mật cho ``/confirm_payment``.
* ``ADMIN_CHAT_ID``        : (tuỳ chọn) chat_id chủ shop - nhận thông báo mỗi đơn.

Chạy thử máy local::

    pip install fastapi uvicorn requests cryptography
    python server_key.py

Deploy Render: Start Command = ``uvicorn server_key:app --host 0.0.0.0 --port $PORT``
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import sqlite3
import string
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

try:  # FastAPI chỉ cần khi chạy server thật; thiếu vẫn import file được để test.
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - máy test offline
    FastAPI = None          # type: ignore[assignment]
    Request = None          # type: ignore[assignment]
    JSONResponse = None     # type: ignore[assignment]

# =========================================================================== #
# CẤU HÌNH (đọc từ biến môi trường - đặt trên Render -> Environment)
# =========================================================================== #
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN",
                               "DÁN_MÃ_BOT_TOKEN_CỦA_BẠN_VÀO_ĐÂY")
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
ADMIN_CHAT_ID = str(os.getenv("ADMIN_CHAT_ID", "")).strip()
SERVER_SECRET = str(os.getenv("SERVER_SECRET", "DUCANH-SERVER-2026")).strip()

MB_USERNAME = str(os.getenv("MB_USERNAME", "")).strip()
MB_PASSWORD = str(os.getenv("MB_PASSWORD", "")).strip()
MB_ACCOUNT = str(os.getenv("MB_ACCOUNT", "0551112075555")).strip()
MB_DEVICE_ID = str(os.getenv("MB_DEVICE_ID", "")).strip()
MB_BASE = "https://online.mbbank.com.vn"
MB_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                 "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

DB_PATH = Path(os.getenv("SERVER_DB_PATH", "")
               or Path(__file__).with_name("license_server.db"))

POLL_SECONDS = 4            # nhịp cào lịch sử (đúng yêu cầu asyncio.sleep(4))
LOOKBACK_DAYS = 3           # số ngày lịch sử quét mỗi vòng

# 📐 MA TRẬN CƯỚC mới - KHỚP 100% với ``PREMIUM_PACKAGES`` trong ``main.py``.
PACKAGES: list[dict[str, Any]] = [
    {"id": "1M", "price": 200000, "days": 30, "label": "Gói 1 Tháng"},
    {"id": "3M", "price": 600000, "days": 90, "label": "Gói 3 Tháng"},
    {"id": "6M", "price": 1000000, "days": 210, "label": "Gói 6 Tháng (Tặng 1 tháng)"},
    {"id": "1Y", "price": 2400000, "days": 420, "label": "Gói 1 Năm (Tặng 2 tháng)"},
]
PACKAGE_BY_PRICE: dict[int, dict[str, Any]] = {int(p["price"]): p for p in PACKAGES}

# Định dạng Key: ``PREM-XXXX-XXXX`` (bỏ ký tự dễ nhầm 0/O/1/I).
KEY_PREFIX = "PREM"
KEY_GROUP = 4
KEY_GROUPS = 2
KEY_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
KICHHOAT_PATTERN = re.compile(r"KICHHOAT\s*([A-Za-z0-9\-]+)", re.IGNORECASE)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# =========================================================================== #
# CSDL SQLite của SERVER (lưu Key, giao dịch đã xử lý, liên kết Telegram)
# =========================================================================== #
_DB_LOCK = threading.Lock()


def _connect() -> sqlite3.Connection:
    """Mở kết nối SQLite (mỗi luồng 1 kết nối)."""
    connection = sqlite3.connect(str(DB_PATH), timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    """Tạo các bảng cần thiết (an toàn khi gọi nhiều lần)."""
    with _DB_LOCK, _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS keys (
                key_code         TEXT PRIMARY KEY,
                machine_id       TEXT NOT NULL DEFAULT '',
                package_id       TEXT NOT NULL DEFAULT '',
                price            INTEGER NOT NULL DEFAULT 0,
                days             INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT NOT NULL DEFAULT '',
                expires_at       TEXT NOT NULL DEFAULT '',
                activated_at     TEXT NOT NULL DEFAULT '',
                telegram_chat_id TEXT NOT NULL DEFAULT '',
                trans_ref        TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS orders (
                trans_ref  TEXT PRIMARY KEY,
                amount     INTEGER NOT NULL DEFAULT 0,
                content    TEXT NOT NULL DEFAULT '',
                machine_id TEXT NOT NULL DEFAULT '',
                package_id TEXT NOT NULL DEFAULT '',
                key_code   TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS bindings (
                machine_id       TEXT PRIMARY KEY,
                telegram_chat_id TEXT NOT NULL DEFAULT '',
                updated_at       TEXT NOT NULL DEFAULT ''
            );
            """
        )


def _now_text() -> str:
    """Mốc thời gian hiện tại (``YYYY-MM-DD HH:MM:SS``)."""
    return datetime.now().strftime(DATE_FORMAT)


def save_binding(machine_id: str, chat_id: str) -> None:
    """Liên kết MÃ MÁY <-> chat_id Telegram của khách (để gửi Key riêng tư)."""
    with _DB_LOCK, _connect() as connection:
        connection.execute(
            "INSERT INTO bindings (machine_id, telegram_chat_id, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT(machine_id) DO UPDATE SET "
            "telegram_chat_id = excluded.telegram_chat_id, "
            "updated_at = excluded.updated_at",
            (str(machine_id), str(chat_id), _now_text()))


def chat_id_for_machine(machine_id: str) -> str:
    """Lấy chat_id Telegram đã liên kết với mã máy (rỗng nếu chưa liên kết)."""
    with _DB_LOCK, _connect() as connection:
        row = connection.execute(
            "SELECT telegram_chat_id FROM bindings WHERE machine_id = ?",
            (str(machine_id),)).fetchone()
    return str(row["telegram_chat_id"]) if row else ""


def machine_for_chat(chat_id: str) -> str:
    """Lấy mã máy đã liên kết với 1 chat_id Telegram (rỗng nếu chưa có)."""
    with _DB_LOCK, _connect() as connection:
        row = connection.execute(
            "SELECT machine_id FROM bindings WHERE telegram_chat_id = ? "
            "ORDER BY updated_at DESC LIMIT 1", (str(chat_id),)).fetchone()
    return str(row["machine_id"]) if row else ""


def order_seen(trans_ref: str) -> bool:
    """Giao dịch này đã xử lý chưa (chống tạo Key trùng)."""
    with _DB_LOCK, _connect() as connection:
        row = connection.execute(
            "SELECT 1 FROM orders WHERE trans_ref = ?", (str(trans_ref),)).fetchone()
    return row is not None


def save_order(trans_ref: str, amount: int, content: str, machine_id: str,
               package_id: str, key_code: str) -> None:
    """Lưu 1 giao dịch đã xử lý + Key đã cấp cho nó."""
    with _DB_LOCK, _connect() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO orders (trans_ref, amount, content, machine_id, "
            "package_id, key_code, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (str(trans_ref), int(amount), str(content), str(machine_id),
             str(package_id), str(key_code), _now_text()))


def save_key(key_code: str, machine_id: str, package: dict[str, Any],
             chat_id: str = "", trans_ref: str = "") -> dict[str, Any]:
    """Lưu Key vừa đẻ (gắn mã máy + gói + HẠN dùng ``days`` kể từ lúc cấp)."""
    days = int(package.get("days") or 0)
    expires = (datetime.now() + timedelta(days=days)).strftime(DATE_FORMAT)
    record = {"key_code": str(key_code).strip().upper(), "machine_id": str(machine_id),
              "package_id": str(package.get("id") or ""),
              "price": int(package.get("price") or 0), "days": days,
              "created_at": _now_text(), "expires_at": expires,
              "telegram_chat_id": str(chat_id or ""), "trans_ref": str(trans_ref or "")}
    with _DB_LOCK, _connect() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO keys (key_code, machine_id, package_id, price, "
            "days, created_at, expires_at, telegram_chat_id, trans_ref) "
            "VALUES (:key_code, :machine_id, :package_id, :price, :days, :created_at, "
            ":expires_at, :telegram_chat_id, :trans_ref)", record)
    return record


def find_key(key_code: str) -> dict[str, Any] | None:
    """Đọc 1 Key trong CSDL (``None`` nếu không có)."""
    with _DB_LOCK, _connect() as connection:
        row = connection.execute(
            "SELECT * FROM keys WHERE key_code = ?",
            (str(key_code).strip().upper(),)).fetchone()
    return dict(row) if row else None


def mark_key_activated(key_code: str) -> None:
    """Đánh dấu Key vừa được app kích hoạt (chỉ để đối soát)."""
    with _DB_LOCK, _connect() as connection:
        connection.execute(
            "UPDATE keys SET activated_at = ? WHERE key_code = ?",
            (_now_text(), str(key_code).strip().upper()))


# =========================================================================== #
# SINH KEY + ĐỌC NỘI DUNG CHUYỂN KHOẢN
# =========================================================================== #
def make_key() -> str:
    """Sinh 1 Key ngẫu nhiên định dạng ``PREM-XXXX-XXXX``."""
    groups = ["".join(random.choice(KEY_ALPHABET) for _ in range(KEY_GROUP))
              for _ in range(KEY_GROUPS)]
    return KEY_PREFIX + "-" + "-".join(groups)


def parse_machine_id(content: Any) -> str:
    """Tách MÃ MÁY trong nội dung CK theo cú pháp ``KICHHOAT <mã máy>``."""
    match = KICHHOAT_PATTERN.search(str(content or ""))
    return match.group(1).upper() if match else ""


def match_package(amount: Any) -> dict[str, Any] | None:
    """Tìm GÓI khớp ĐÚNG số tiền chuyển (không khớp -> ``None``)."""
    try:
        value = int(round(float(amount)))
    except (TypeError, ValueError):
        return None
    return PACKAGE_BY_PRICE.get(value)


# =========================================================================== #
# CÀO LỊCH SỬ BIẾN ĐỘNG SỐ DƯ MB BANK (API nội bộ - MIỄN PHÍ, chỉ ĐỌC)
# =========================================================================== #
class MBBankScraper:
    """Đăng nhập Internet Banking MB Bank rồi đọc lịch sử giao dịch (read-only).

    ⚠️ Đây là API KHÔNG chính thức: MB Bank có thể đổi cấu trúc bất cứ lúc nào, nên
    TÊN ĐƯỜNG DẪN & trường dữ liệu được tách thành HẰNG SỐ để dễ chỉnh. MỌI lỗi đều
    được nuốt (trả ``[]``) để vòng lặp nền không bao giờ chết.
    """

    CAPTCHA_PATH = "/api/retail_web/internetbanking/getCaptcha"
    LOGIN_PATH = "/api/retail_web/internetbanking/login"
    HISTORY_PATH = "/api/retail_web/transactions/getHistTransaction"

    def __init__(self) -> None:
        self.username = MB_USERNAME
        self.password = MB_PASSWORD
        self.account = MB_ACCOUNT
        self.device_id = MB_DEVICE_ID or str(uuid.uuid4())
        self.session_id = ""
        self.client_id = str(uuid.uuid4())
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": MB_USER_AGENT,
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, text/plain, */*",
            "Origin": MB_BASE,
            "Referer": MB_BASE + "/",
        })

    @staticmethod
    def _ref_no() -> str:
        """Mã tham chiếu cho mỗi request (MB yêu cầu chuỗi dạng thời gian)."""
        return datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]

    def _rsa_encrypt(self, text: str) -> str:
        """Mã hoá mật khẩu bằng RSA công khai của MB (thiếu lib -> trả nguyên)."""
        try:
            import base64

            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import padding

            captcha = self.session.get(MB_BASE + self.CAPTCHA_PATH,
                                       timeout=15).json()
            key_b64 = captcha.get("rsa") or (captcha.get("data") or {}).get("rsa")
            if not key_b64:
                return text
            public_key = serialization.load_der_public_key(base64.b64decode(key_b64))
            encrypted = public_key.encrypt(text.encode("utf-8"), padding.PKCS1v15())
            return base64.b64encode(encrypted).decode("ascii")
        except Exception:  # noqa: BLE001 - thiếu lib / MB đổi khoá -> gửi thô
            return text

    def login(self) -> bool:
        """Đăng nhập MB Bank (lưu ``session_id`` để gọi lịch sử)."""
        payload = {
            "userId": self.username,
            "password": self._rsa_encrypt(self.password),
            "captcha": "",
            "ib": "i",
            "deviceId": self.device_id,
            "refNo": self._ref_no(),
            "deviceName": "Chrome",
            "clientId": self.client_id,
            "versionApp": "1.0.0",
        }
        response = self.session.post(MB_BASE + self.LOGIN_PATH, json=payload,
                                     timeout=20)
        data = response.json()
        self.session_id = str(data.get("sessionId")
                              or (data.get("data") or {}).get("sessionId") or "")
        return bool(self.session_id)

    def get_transactions(self, days: int = LOOKBACK_DAYS) -> list[dict[str, Any]]:
        """Danh sách giao dịch (mới nhất trước) trong ``days`` ngày gần đây."""
        if not self.session_id and not self.login():
            return []
        now = datetime.now()
        payload = {
            "accountNo": self.account,
            "fromDate": (now - timedelta(days=days)).strftime("%d/%m/%Y"),
            "toDate": now.strftime("%d/%m/%Y"),
            "sessionId": self.session_id,
            "refNo": self._ref_no(),
            "deviceId": self.device_id,
            "deviceName": "Chrome",
        }
        response = self.session.post(MB_BASE + self.HISTORY_PATH, json=payload,
                                     timeout=20)
        data = response.json()
        rows = (data.get("transactionHistoryList")
                or (data.get("data") or {}).get("transactionHistoryList") or [])
        return [dict(row) for row in rows]


def fetch_mb_transactions() -> list[dict[str, Any]]:
    """Cào lịch sử MB Bank an toàn (thiếu cấu hình / lỗi mạng -> ``[]``)."""
    if not (MB_USERNAME and MB_PASSWORD):
        return []
    try:
        return MBBankScraper().get_transactions()
    except Exception as error:  # noqa: BLE001
        print(f"[MB] lỗi cào lịch sử: {error}", flush=True)
        return []


def normalize_transaction(row: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hoá 1 giao dịch MB -> ``{ref, amount, content, is_in}`` (chỉ TIỀN VÀO)."""
    def pick(*names: str) -> Any:
        for name in names:
            value = row.get(name)
            if value not in (None, ""):
                return value
        return ""

    credit = pick("creditAmount", "credit", "amountIn")
    debit = pick("debitAmount", "debit", "amountOut")
    amount = 0
    if credit not in ("", None):
        try:
            amount = int(round(float(str(credit).replace(",", "").strip())))
        except (TypeError, ValueError):
            amount = 0
    content = str(pick("description", "addDescription", "remark",
                       "transactionDesc") or "")
    ref = str(pick("refNo", "transactionNumber", "transactionId", "id") or "")
    return {"ref": ref, "amount": amount, "content": content,
            "is_in": bool(amount > 0 and not debit)}


# =========================================================================== #
# TELEGRAM BOT (gửi Key RIÊNG TƯ cho khách + thông báo cho chủ shop)
# =========================================================================== #
def bot_configured() -> bool:
    """Đã dán token bot Telegram THẬT chưa."""
    token = str(TELEGRAM_BOT_TOKEN or "")
    return bool(token) and "DÁN_MÃ" not in token and ":" in token


def telegram_call(method: str, payload: dict[str, Any],
                  timeout: float = 15) -> dict[str, Any]:
    """Gọi 1 API Telegram Bot (nuốt mọi lỗi -> ``{"ok": False}``)."""
    if not bot_configured():
        return {"ok": False, "description": "chưa cấu hình TELEGRAM_BOT_TOKEN"}
    url = TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN, method=method)
    try:
        return requests.post(url, json=payload, timeout=timeout).json()
    except Exception as error:  # noqa: BLE001
        return {"ok": False, "description": str(error)}


def send_telegram(chat_id: Any, text: str) -> dict[str, Any]:
    """Gửi tin nhắn RIÊNG TƯ (Markdown) tới 1 ``chat_id``."""
    if not str(chat_id or "").strip():
        return {"ok": False, "description": "thiếu chat_id"}
    return telegram_call("sendMessage", {
        "chat_id": str(chat_id), "text": str(text),
        "parse_mode": "Markdown", "disable_web_page_preview": True})


def notify_admin(text: str) -> None:
    """Thông báo chủ shop (nếu đã cấu hình ``ADMIN_CHAT_ID``)."""
    if ADMIN_CHAT_ID:
        send_telegram(ADMIN_CHAT_ID, text)


# =========================================================================== #
# XỬ LÝ THANH TOÁN: KHỚP TIỀN + NỘI DUNG -> ĐẺ KEY -> GỬI TELEGRAM
# =========================================================================== #
def issue_key_for_payment(trans_ref: str, amount: int, content: str,
                          chat_id: str = "") -> dict[str, Any] | None:
    """Nếu giao dịch khớp 1 GÓI + ``KICHHOAT <mã máy>`` -> đẻ & gửi Key.

    Trả về ``{"key","machine_id","package","chat_id"}`` khi CẤP KEY THÀNH CÔNG;
    ``None`` khi bỏ qua (sai số tiền / thiếu cú pháp / giao dịch đã xử lý).
    """
    machine_id = parse_machine_id(content)
    package = match_package(amount)
    if not machine_id or package is None:
        return None
    if trans_ref and order_seen(trans_ref):
        return None
    key_code = make_key()
    target_chat = str(chat_id or chat_id_for_machine(machine_id) or "")
    save_key(key_code, machine_id, package, chat_id=target_chat, trans_ref=trans_ref)
    if trans_ref:
        save_order(trans_ref, amount, content, machine_id,
                   str(package.get("id")), key_code)
    money = f"{int(amount):,}".replace(",", ".")
    message = (
        "💎 *KÍCH HOẠT PREMIUM THÀNH CÔNG*\n"
        f"Mã máy: `{machine_id}`\n"
        f"Gói: *{package.get('label')}* ({package.get('days')} ngày)\n"
        f"Số tiền: {money}đ\n\n"
        f"🔑 MÃ KEY CỦA BẠN:\n`{key_code}`\n\n"
        "Dán mã này vào ô *Nhập mã Key Premium* trong app rồi bấm *Kích Hoạt*.\n"
        "Xin cảm ơn quý khách! 🙏")
    if target_chat:
        send_telegram(target_chat, message)
    notify_admin(f"✅ Đã bán {package.get('label')} cho máy {machine_id} "
                 f"({money}đ) - key {key_code}")
    return {"key": key_code, "machine_id": machine_id, "package": package,
            "chat_id": target_chat}


def process_transactions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Duyệt danh sách giao dịch THÔ -> cấp Key cho các giao dịch HỢP LỆ."""
    issued: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            transaction = normalize_transaction(row)
        except Exception:  # noqa: BLE001
            continue
        if not transaction.get("is_in"):
            continue
        try:
            result = issue_key_for_payment(
                transaction.get("ref") or str(uuid.uuid4()),
                int(transaction.get("amount") or 0),
                str(transaction.get("content") or ""))
        except Exception as error:  # noqa: BLE001
            print(f"[KEY] lỗi cấp key: {error}", flush=True)
            continue
        if result:
            issued.append(result)
    return issued


# =========================================================================== #
# VÒNG LẶP NỀN: CỨ 4 GIÂY CÀO LỊCH SỬ MB BANK 1 LẦN (asyncio.sleep(4))
# =========================================================================== #
async def poll_mbbank_forever() -> None:
    """Vòng lặp chạy nền 24/7: cào MB Bank -> cấp Key (lỗi KHÔNG làm chết vòng)."""
    print(f"[SERVER] Bắt đầu vòng cào MB Bank mỗi {POLL_SECONDS}s...", flush=True)
    while True:
        try:
            rows = await asyncio.to_thread(fetch_mb_transactions)
            issued = await asyncio.to_thread(process_transactions, rows)
            for item in issued:
                print(f"[KEY] đã cấp {item['key']} cho {item['machine_id']}",
                      flush=True)
        except Exception as error:  # noqa: BLE001
            print(f"[SERVER] vòng cào lỗi: {error}", flush=True)
        await asyncio.sleep(POLL_SECONDS)


# =========================================================================== #
# FASTAPI: ENDPOINT CHO APP POS + KHỞI ĐỘNG VÒNG LẶP NỀN
# =========================================================================== #
def create_app():
    """Tạo ứng dụng FastAPI (kèm vòng cào MB Bank chạy nền 24/7)."""
    if FastAPI is None:
        raise RuntimeError("Cần cài 'fastapi' để chạy server: "
                           "pip install fastapi uvicorn requests cryptography")

    app = FastAPI(title="Ducanh Store - Licence Server", version="1.0.0")

    @app.on_event("startup")
    async def _startup() -> None:
        """Mở CSDL + bật vòng cào MB Bank chạy nền."""
        init_db()
        asyncio.create_task(poll_mbbank_forever())
        print("[SERVER] Đã khởi động - chờ thanh toán MB Bank...", flush=True)

    @app.get("/")
    async def root() -> dict[str, Any]:
        """Kiểm tra server còn sống (dùng cho Render health check)."""
        return {"ok": True, "service": "licence-server", "time": _now_text(),
                "packages": [{"id": p["id"], "price": p["price"], "days": p["days"]}
                             for p in PACKAGES]}

    @app.post("/register")
    async def register(request: Request) -> Any:
        """App POS đăng ký mã máy + chat_id Telegram để nhận Key riêng tư."""
        data = await _json_body(request)
        machine_id = str(data.get("machine_id") or "").strip().upper()
        chat_id = str(data.get("telegram_chat_id") or "").strip()
        if not machine_id:
            return _json({"ok": False, "message": "Thiếu machine_id"}, 400)
        save_binding(machine_id, chat_id)
        return {"ok": True, "message": "Đã đăng ký máy với server."}

    @app.post("/verify_key")
    async def verify_key(request: Request) -> Any:
        """App POS gửi Key lên -> server đối chiếu CSDL trả valid True/False."""
        data = await _json_body(request)
        key_code = str(data.get("key") or "").strip().upper()
        machine_id = str(data.get("machine_id") or "").strip().upper()
        record = find_key(key_code)
        if not record:
            return {"valid": False, "days": 0, "expiry": "",
                    "message": "Key không tồn tại hoặc chưa được cấp."}
        owner = str(record.get("machine_id") or "").strip().upper()
        if machine_id and owner and machine_id != owner:
            return {"valid": False, "days": 0, "expiry": "",
                    "message": "Key này được cấp cho máy khác."}
        expires = _parse_dt(record.get("expires_at"))
        if expires is None or expires <= datetime.now():
            return {"valid": False, "days": 0,
                    "expiry": str(record.get("expires_at") or ""),
                    "message": "Key đã hết hạn."}
        mark_key_activated(key_code)
        return {"valid": True, "days": int(record.get("days") or 0),
                "expiry": str(record.get("expires_at") or ""),
                "message": f"Kích hoạt thành công gói {record.get('package_id')}!"}

    @app.post("/confirm_payment")
    async def confirm_payment(request: Request) -> Any:
        """DỰ PHÒNG: chủ shop tự đẻ Key khi cào MB lỗi (cần ``server_secret``)."""
        data = await _json_body(request)
        if str(data.get("secret") or "") != SERVER_SECRET:
            return _json({"ok": False, "message": "Sai server_secret"}, 403)
        machine_id = str(data.get("machine_id") or "").strip().upper()
        package = match_package(data.get("amount"))
        if not machine_id or package is None:
            return _json({"ok": False, "message": "Sai mã máy / số tiền gói"}, 400)
        key_code = make_key()
        chat_id = str(data.get("telegram_chat_id")
                      or chat_id_for_machine(machine_id) or "")
        save_key(key_code, machine_id, package, chat_id=chat_id,
                 trans_ref=str(data.get("trans_ref") or ""))
        if chat_id:
            send_telegram(chat_id, f"🔑 Key Premium của bạn: `{key_code}`")
        return {"ok": True, "key": key_code, "machine_id": machine_id,
                "days": package["days"]}

    @app.post("/webhook/telegram")
    async def telegram_webhook(request: Request) -> dict[str, Any]:
        """Bot nhận lệnh ``/start <MÃ MÁY>`` -> liên kết chat_id với mã máy."""
        update = await _json_body(request)
        message = (update.get("message") or update.get("edited_message") or {})
        chat_id = str((message.get("chat") or {}).get("id") or "")
        text = str(message.get("text") or "").strip()
        reply = "Gõ: /start <MÃ MÁY> để nhận Key khi bạn mua Premium."
        parts = text.split()
        if len(parts) >= 2 and parts[0].lower().startswith("/start"):
            machine_id = parts[1].strip().upper()
            if chat_id:
                save_binding(machine_id, chat_id)
                reply = (f"✅ Đã liên kết máy *{machine_id}* với Telegram này.\n"
                         "Khi bạn chuyển khoản đúng số tiền, Key Premium sẽ được "
                         "gửi ngay vào đây.")
        if chat_id:
            send_telegram(chat_id, reply)
        return {"ok": True}

    return app


def _json(payload: dict[str, Any], status: int = 200) -> Any:
    """Trả JSON kèm mã trạng thái (dùng ``JSONResponse`` nếu có FastAPI)."""
    if JSONResponse is not None:
        return JSONResponse(payload, status_code=status)
    return payload


async def _json_body(request: Any) -> dict[str, Any]:
    """Đọc body JSON của request (luôn trả ``dict``, không ném lỗi)."""
    try:
        data = await request.json()
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _parse_dt(text: Any) -> datetime | None:
    """Đọc mốc thời gian ``YYYY-MM-DD HH:MM:SS`` (``None`` nếu sai định dạng)."""
    try:
        return datetime.strptime(str(text or ""), DATE_FORMAT)
    except Exception:  # noqa: BLE001
        return None


# Ứng dụng ASGI cho uvicorn: ``uvicorn server_key:app --host 0.0.0.0 --port $PORT``
app = create_app() if FastAPI is not None else None


if __name__ == "__main__":
    init_db()
    if app is None:
        print("Chưa cài FastAPI -> chạy: "
              "pip install fastapi uvicorn requests cryptography")
    else:
        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))







