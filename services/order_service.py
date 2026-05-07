from __future__ import annotations

import time
import secrets
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from config import MAIN_TEXT_RATES, RATES
from utils.db import ensure_json_file, read_json, write_json_atomic


class OrderService:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.orders_path = self.base_dir / "orders.json"
        self.ads_path = self.base_dir / "ads.json"
        ensure_json_file(self.orders_path, {})
        ensure_json_file(self.ads_path, {})

    def load_orders(self) -> dict[str, Any]:
        return read_json(self.orders_path, {})

    def save_orders(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.orders_path, data)

    def load_ads(self) -> dict[str, Any]:
        return read_json(self.ads_path, {})

    def save_ads(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.ads_path, data)

    def next_order_id(self, orders: dict[str, Any]) -> str:
        max_id = 0
        for order_id in orders:
            if not order_id.startswith("order_"):
                continue
            suffix = order_id.split("_", 1)[1]
            if suffix.isdigit():
                max_id = max(max_id, int(suffix))
        return f"order_{max_id + 1}"

    def next_order_number(self, orders: dict[str, Any]) -> int:
        max_number = 0
        for order in orders.values():
            try:
                number = int(order.get("order_no", 0) or 0)
            except (TypeError, ValueError):
                number = 0
            if number > max_number:
                max_number = number
        return max_number + 1

    def next_order_code(self, orders: dict[str, Any], length: int = 7) -> str:
        alphabet = string.ascii_uppercase + string.digits
        existing = {str(order.get("order_code", "")).upper() for order in orders.values() if order.get("order_code")}
        while True:
            code = "".join(secrets.choice(alphabet) for _ in range(length))
            if code not in existing:
                return code

    def get_user_active_ad(self, ads_data: dict[str, Any], user_id: int) -> dict[str, Any] | None:
        ads = self.get_user_active_ads(ads_data, user_id)
        if ads:
            ads.sort(key=lambda item: item.get("created_at") or "", reverse=True)
            return ads[0]
        user_key = str(user_id)
        if isinstance(ads_data.get(user_key), dict):
            return ads_data[user_key]
        users_ads = ads_data.get("users", {})
        if isinstance(users_ads, dict) and isinstance(users_ads.get(user_key), dict):
            return users_ads[user_key]
        active_ad = ads_data.get("active_ad")
        if isinstance(active_ad, dict) and active_ad.get("user_id") == user_id:
            return active_ad
        return None

    def get_user_active_ads(self, ads_data: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for key, ad_data in ads_data.items():
            if key in {"main_text_sale", "active_ad", "users"}:
                continue
            if not isinstance(ad_data, dict):
                continue
            if key == str(ad_data.get("user_id")):
                continue
            if ad_data.get("user_id") != user_id:
                continue
            if ad_data.get("status") not in {"approved", "active"}:
                continue
            result.append(ad_data)
        return result

    def set_user_active_ad(self, ads_data: dict[str, Any], user_id: int, ad_data: dict[str, Any]) -> dict[str, Any]:
        user_key = str(user_id)
        canonical_key = ad_data.get("ad_key") or ad_data.get("order_id") or user_key
        ad_data["ad_key"] = canonical_key
        ads_data[canonical_key] = ad_data
        ads_data[user_key] = ad_data
        users_ads = ads_data.setdefault("users", {})
        if isinstance(users_ads, dict):
            users_ads[user_key] = ad_data
        ads_data["active_ad"] = ad_data
        return ads_data

    def approve_order(
        self,
        orders: dict[str, Any],
        ads_data: dict[str, Any],
        order_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        order = orders.get(order_id)
        if not order:
            return orders, ads_data, None

        if order.get("type") == "main_text":
            expires_at = (datetime.now() + timedelta(days=int(order.get("days") or 0))).strftime("%Y-%m-%d %H:%M:%S")
            main_text_data = {
                "user_id": order["user_id"],
                "order_id": order_id,
                "order_code": order.get("order_code"),
                "ad_key": "main_text_sale",
                "type": "main_text",
                "text": order.get("text", ""),
                "photo": order.get("photo"),
                "price": order.get("price", 0),
                "days": order.get("days", 0),
                "rate_name": order.get("rate_name") or MAIN_TEXT_RATES.get(int(order.get("days") or 0), {}).get("name"),
                "status": "active",
                "created_at": order.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "expires_at": expires_at,
                "order_id": order_id,
            }
            order["status"] = "approved"
            order["approved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order.setdefault("events", []).append({"at": order["approved_at"], "event": "approved"})
            ads_data["main_text_sale"] = main_text_data
            return orders, ads_data, main_text_data

        source_key = order.get("source_order_id") or order.get("item_key")
        current_ad = {}
        if source_key and isinstance(ads_data.get(source_key), dict):
            current_ad = ads_data.get(source_key) or {}
        elif order.get("type") in {"update_ad", "extend_ad", "renew_ad"}:
            current_ad = self.get_user_active_ad(ads_data, order["user_id"]) or {}

        ad_key = source_key or order_id
        expires_at = self._resolve_expiration(order, current_ad)
        extensions = list(current_ad.get("extensions", [])) if isinstance(current_ad.get("extensions"), list) else []
        if order.get("type") in {"extend_ad", "renew_ad"}:
            extensions.append(
                {
                    "order_id": order_id,
                    "days": int(order.get("days", 0) or 0),
                    "price": int(order.get("price", 0) or 0),
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        ad_data = {
            "order_id": current_ad.get("order_id") if order.get("type") in {"update_ad", "extend_ad", "renew_ad"} and current_ad.get("order_id") else order_id,
            "order_code": current_ad.get("order_code") if order.get("type") in {"update_ad", "extend_ad", "renew_ad"} and current_ad.get("order_code") else order.get("order_code"),
            "ad_key": ad_key,
            "type": current_ad.get("type") or order.get("type") or "new_ad",
            "user_id": order["user_id"],
            "text": order.get("text") if order.get("text") not in {None, ""} else current_ad.get("text", ""),
            "photo": order.get("photo") if order.get("photo") not in {None, ""} else current_ad.get("photo"),
            "price": order.get("price", current_ad.get("price", 0)),
            "days": order.get("days", current_ad.get("days", 0)),
            "rate_name": order.get("rate_name") or current_ad.get("rate_name"),
            "created_at": current_ad.get("created_at") or order.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at": expires_at,
            "extensions": extensions,
            "total_extension_days": sum(int(item.get("days", 0) or 0) for item in extensions),
            "status": "approved",
        }
        order["status"] = "approved"
        order["approved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order.setdefault("events", []).append({"at": order["approved_at"], "event": "approved"})
        self.set_user_active_ad(ads_data, order["user_id"], ad_data)
        return orders, ads_data, ad_data

    def rebuild_ads_from_orders(self, orders: dict[str, Any], existing_ads: dict[str, Any] | None = None) -> dict[str, Any]:
        ads_data: dict[str, Any] = {"users": {}, "active_ad": {}}
        if isinstance(existing_ads, dict):
            current_main = existing_ads.get("main_text_sale")
            if isinstance(current_main, dict):
                ads_data["main_text_sale"] = current_main

        approved_orders = sorted(
            [
                (order_id, order)
                for order_id, order in orders.items()
                if isinstance(order, dict) and order.get("status") == "approved"
            ],
            key=lambda item: item[1].get("approved_at") or item[1].get("created_at") or "",
        )

        for order_id, order in approved_orders:
            order_type = order.get("type")
            if order_type == "main_text":
                expires_at = order.get("expires_at")
                if not expires_at:
                    expires_at = (datetime.now() + timedelta(days=int(order.get("days") or 0))).strftime("%Y-%m-%d %H:%M:%S")
                ads_data["main_text_sale"] = {
                    "user_id": order["user_id"],
                    "order_id": order_id,
                    "order_code": order.get("order_code"),
                    "ad_key": "main_text_sale",
                    "type": "main_text",
                    "text": order.get("text", ""),
                    "photo": order.get("photo"),
                    "price": order.get("price", 0),
                    "days": order.get("days", 0),
                    "rate_name": order.get("rate_name") or MAIN_TEXT_RATES.get(int(order.get("days") or 0), {}).get("name"),
                    "status": "active",
                    "created_at": order.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "expires_at": expires_at,
                }
                continue

            if order_type not in {"new_ad", "update_ad", "extend_ad", "renew_ad"}:
                continue

            source_key = order.get("source_order_id") or order.get("item_key")
            current_ad = {}
            if source_key and isinstance(ads_data.get(source_key), dict):
                current_ad = ads_data.get(source_key) or {}
            ad_key = source_key or order_id
            ad_data = {
                "order_id": current_ad.get("order_id") if order_type in {"update_ad", "extend_ad", "renew_ad"} and current_ad.get("order_id") else order_id,
                "order_code": current_ad.get("order_code") if order_type in {"update_ad", "extend_ad", "renew_ad"} and current_ad.get("order_code") else order.get("order_code"),
                "ad_key": ad_key,
                "type": current_ad.get("type") or ("new_ad" if order_type in {"update_ad", "extend_ad", "renew_ad"} else order_type),
                "user_id": order["user_id"],
                "text": order.get("text") if order.get("text") not in {None, ""} else current_ad.get("text", ""),
                "photo": order.get("photo") if order.get("photo") not in {None, ""} else current_ad.get("photo"),
                "price": order.get("price", current_ad.get("price", 0)),
                "days": order.get("days", current_ad.get("days", 0)),
                "rate_name": order.get("rate_name") or current_ad.get("rate_name"),
                "created_at": current_ad.get("created_at") or order.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "expires_at": self._resolve_expiration(order, current_ad),
                "extensions": list(current_ad.get("extensions", [])) if isinstance(current_ad.get("extensions"), list) else [],
                "total_extension_days": current_ad.get("total_extension_days", 0),
                "status": "approved",
            }
            if order_type in {"extend_ad", "renew_ad"}:
                ad_data["extensions"].append(
                    {
                        "order_id": order_id,
                        "days": int(order.get("days", 0) or 0),
                        "price": int(order.get("price", 0) or 0),
                        "created_at": order.get("approved_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                ad_data["total_extension_days"] = sum(int(item.get("days", 0) or 0) for item in ad_data["extensions"])
            self.set_user_active_ad(ads_data, order["user_id"], ad_data)
        return ads_data

    def reject_order(self, orders: dict[str, Any], order_id: str) -> dict[str, Any]:
        order = orders.get(order_id)
        if order:
            order["status"] = "rejected"
            order["rejected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order.setdefault("events", []).append({"at": order["rejected_at"], "event": "rejected"})
        return orders

    def random_id(self) -> int:
        return abs(hash(f"{time.time()}_{time.perf_counter()}")) % (10**10)

    def get_main_text_status(self, ads_data: dict[str, Any]):
        return ads_data.get("main_text_sale", None)

    def can_buy_main_text(self, ads_data: dict[str, Any]):
        main_sale = self.get_main_text_status(ads_data)
        if not main_sale:
            return True, None
        expires_at = main_sale.get("expires_at")
        if expires_at:
            try:
                if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S") > datetime.now():
                    return False, f"Место занято до {expires_at}"
            except ValueError:
                pass
        return True, None

    def _resolve_expiration(self, order: dict[str, Any], current_ad: dict[str, Any]) -> str:
        if order.get("type") in {"extend_ad", "renew_ad"}:
            base_dt = datetime.now()
            current_exp = current_ad.get("expires_at")
            if current_exp:
                try:
                    current_exp_dt = datetime.strptime(current_exp, "%Y-%m-%d %H:%M:%S")
                    if current_exp_dt > base_dt:
                        base_dt = current_exp_dt
                except ValueError:
                    pass
            days = int(order.get("days") or 0)
            return (base_dt + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        if order.get("type") == "update_ad" and current_ad.get("expires_at"):
            return current_ad["expires_at"]
        days = int(order.get("days") or current_ad.get("days") or 0)
        return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
