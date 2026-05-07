from __future__ import annotations

import json
from typing import Any

from vk_api.keyboard import VkKeyboard, VkKeyboardColor


HISTORY_PAGE_SIZE = 5


def parse_message_payload(message: dict[str, Any], logger=None) -> dict[str, Any]:
    payload = message.get("payload")
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str) and payload.strip():
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            if logger:
                logger.warning(f"Не удалось разобрать payload: {payload}")
    return {}


def build_orders_list_keyboard(orders_data: dict[str, Any]) -> str | None:
    pending_ids = [order_id for order_id, order in orders_data.items() if order.get("status") == "pending"]
    if not pending_ids:
        return None
    keyboard = VkKeyboard(inline=True)
    for index, order_id in enumerate(pending_ids[:6], start=1):
        order_type = (orders_data.get(order_id, {}).get("type") or "").lower()
        order_code = orders_data.get(order_id, {}).get("order_code") or orders_data.get(order_id, {}).get("order_no", index)
        if order_type in {"extend_ad", "renew_ad"}:
            prefix = "Продл."
        elif order_type == "update_ad":
            prefix = "Изм."
        elif order_type == "main_text":
            prefix = "Осн."
        else:
            prefix = "Доп."
        keyboard.add_callback_button(
            f"{prefix} {order_code}",
            color=VkKeyboardColor.PRIMARY,
            payload={"command": "view_order", "order_id": order_id},
        )
        if index < len(pending_ids[:6]):
            keyboard.add_line()
    return keyboard.get_keyboard()


def build_orders_history_keyboard(orders_data: dict[str, Any], page: int = 0) -> str | None:
    history_ids = [order_id for order_id, order in orders_data.items() if order.get("status") in {"approved", "rejected", "deleted"}]
    history_ids = list(reversed(history_ids))
    if not history_ids:
        return None
    total_pages = max(1, (len(history_ids) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * HISTORY_PAGE_SIZE
    page_ids = history_ids[start:start + HISTORY_PAGE_SIZE]

    keyboard = VkKeyboard(inline=True)
    for index, order_id in enumerate(page_ids, start=1):
        order = orders_data.get(order_id, {})
        status = (order.get("status") or "").lower()
        order_code = order.get("order_code") or order.get("order_no", start + index)
        if status == "approved":
            prefix = "Одобр."
            color = VkKeyboardColor.POSITIVE
        elif status == "rejected":
            prefix = "Откл."
            color = VkKeyboardColor.NEGATIVE
        else:
            prefix = "Удал."
            color = VkKeyboardColor.SECONDARY
        keyboard.add_callback_button(
            f"{prefix} {order_code}",
            color=color,
            payload={"command": "view_order", "order_id": order_id, "readonly": True, "history_page": page},
        )
        if index < len(page_ids):
            keyboard.add_line()

    if total_pages > 1:
        keyboard.add_line()
        if page > 0:
            keyboard.add_callback_button("⬅️", color=VkKeyboardColor.SECONDARY, payload={"command": "history_page", "page": page - 1})
        if page < total_pages - 1:
            keyboard.add_callback_button("➡️", color=VkKeyboardColor.SECONDARY, payload={"command": "history_page", "page": page + 1})
    return keyboard.get_keyboard()


def build_order_action_keyboard(
    order_id: str,
    status: str | None = None,
    has_check: bool = False,
    *,
    readonly: bool = False,
    history_page: int = 0,
) -> str:
    keyboard = VkKeyboard(inline=True)
    if not readonly and status not in {"approved", "rejected"}:
        keyboard.add_callback_button("✅ Одобрить", color=VkKeyboardColor.POSITIVE, payload={"command": "approve_order", "order_id": order_id})
        keyboard.add_callback_button("❌ Отклонить", color=VkKeyboardColor.NEGATIVE, payload={"command": "reject_order", "order_id": order_id})
    if has_check:
        if not readonly or status not in {"approved", "rejected"}:
            keyboard.add_line()
        keyboard.add_callback_button("📎 Показать чек", color=VkKeyboardColor.SECONDARY, payload={"command": "show_check_order", "order_id": order_id})
    if readonly:
        keyboard.add_line()
        keyboard.add_callback_button("❌ Выйти", color=VkKeyboardColor.SECONDARY, payload={"command": "history_page", "page": history_page})
    return keyboard.get_keyboard()


def render_orders_list_text(orders_data: dict[str, Any]) -> str:
    pending_count = sum(1 for order in orders_data.values() if order.get("status") == "pending")
    if pending_count == 0:
        return "ЗАЯВКИ НА МОДЕРАЦИЮ\n\nСтатус: пусто\nОжидающих проверку заявок нет."
    lines = [
        "ЗАЯВКИ НА МОДЕРАЦИЮ",
        "",
        f"Ожидает проверки: {pending_count}",
        "Выберите заявку из списка ниже.",
    ]
    if pending_count > 6:
        lines.extend(["", "Показаны первые 6 заявок."])
    return "\n".join(lines)


def render_orders_history_text(orders_data: dict[str, Any], page: int = 0) -> str:
    history_orders = [order for order in orders_data.values() if order.get("status") in {"approved", "rejected", "deleted"}]
    if not history_orders:
        return "ИСТОРИЯ ЗАКАЗОВ\n\nИстория пока пуста."
    total_pages = max(1, (len(history_orders) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    lines = [
        "ИСТОРИЯ ЗАКАЗОВ",
        "",
        f"Всего обработано: {len(history_orders)}",
        "Выберите заказ из списка ниже.",
    ]
    if total_pages > 1:
        lines.extend(["", f"Страница: {page + 1}/{total_pages}"])
    return "\n".join(lines)


def _order_type_label(order_data: dict[str, Any]) -> str:
    return "Основное" if (order_data.get("type") or "").lower() == "main_text" else "Дополнительное"


def _order_status_label(status: str | None) -> str:
    status_map = {
        "pending": "На модерации",
        "approved": "Одобрено",
        "rejected": "Отклонено",
        "deleted": "Удалено",
    }
    return status_map.get((status or "").lower(), status or "Неизвестно")


def render_order_details(order_id: str, order_data: dict[str, Any] | None, user_label: str) -> str:
    if not order_data:
        return "❌ Заявка не найдена."
    text_value = (order_data.get("text") or "").strip() or "Не указан"
    return (
        "ЗАЯВКА НА РАССЫЛКУ\n\n"
        f"Код заказа: {order_data.get('order_code') or order_data.get('order_no', '-')}\n"
        f"Тип: {_order_type_label(order_data)}\n"
        f"Покупатель: {user_label}\n"
        f"Статус: {_order_status_label(order_data.get('status'))}\n"
        f"Сумма: {order_data.get('price', 0)} ₽\n"
        f"Срок: {order_data.get('days', 0)} дн.\n"
        f"Создан: {order_data.get('created_at', 'неизвестно')}\n"
        f"ID заказа: {order_id}\n\n"
        f"Текст рекламы:\n{text_value}"
    )


def render_order_result(order_id: str, order_data: dict[str, Any] | None, user_label: str) -> str:
    if not order_data:
        return "❌ Заявка не найдена."
    title = "✅ ЗАЯВКА ОДОБРЕНА" if order_data.get("status") == "approved" else "❌ ЗАЯВКА ОТКЛОНЕНА"
    text_value = (order_data.get("text") or "").strip() or "Не указан"
    return (
        f"{title}\n\n"
        f"Код заказа: {order_data.get('order_code') or order_data.get('order_no', '-')}\n"
        f"Тип: {_order_type_label(order_data)}\n"
        f"Покупатель: {user_label}\n"
        f"Сумма: {order_data.get('price', 0)} ₽\n"
        f"Срок: {order_data.get('days', 0)} дн.\n"
        f"ID заказа: {order_id}\n\n"
        f"Текст рекламы:\n{text_value}"
    )
