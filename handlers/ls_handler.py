from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from config import MAIN_TEXT_RATES, PAYMENT_DETAILS, RATES, SUBSCRIPTION_CHANNEL_LINK, group_id
from menu import generate_main_menu, generate_main_text_rates_menu, generate_rates_menu
from services.order_service import OrderService


USER_STATES: dict[str, dict[str, Any]] = {}
PERSISTENT_CALLBACK_COMMANDS = {
    "view_user_ad",
    "start_extend",
    "contact_support",
    "add_check",
    "submit_order",
    "reupload_check",
    "edit_ad_text",
    "edit_ad_photo",
    "add_ad_photo",
    "edit_main_text",
    "edit_main_photo",
    "add_main_photo",
}


WELCOME_TEXT = (
    "👋 Добро пожаловать!\n"
    "Здесь вы можете заказать продвижение своего канала или аккаунта.\n\n"
    "Выберите действие:"
)

HELP_TEXT = (
    "🛠 ДОСТУПНЫЕ КОМАНДЫ (ЛС):\n\n"
    "🔹 .начать — главное меню\n"
    "🔹 .купить — заказать рекламу\n"
    "🔹 .продлить — продлить рекламу\n"
    "🔹 .сообщение — посмотреть свои объявления\n"
    "🔹 .помощь — этот список"
)

INFO_TEXT = (
    "🏢 О НАС\n\n"
    "Мы Klinoff | PIAR занимаемся продвижением каналов и аккаунтов.\n"
    "📊 Рассылаем сообщения в 150+ чатов\n"
    "💰 Доступные тарифы от 70 ₽\n"
    "⚡ Быстрое одобрение заявок\n\n"
    "По вопросам связи: @klinoffshop"
)

PAYMENT_TEXT = (
    "💳 ОПЛАТА ЗАКАЗА\n\n"
    "Переведите сумму по реквизитам:\n"
    "• Телефон: +7 902 840-96-27\n"
    "• Банк: ВТБ\n"
    "• Получатель: Максим Анатольевич К.\n\n"
    "После оплаты нажмите «📎 Добавить чек» и отправьте скриншот или файл."
)


def _normalize_command(raw: str) -> str:
    value = (raw or "").strip().lower()
    if value.startswith("."):
        value = value[1:]
    return value


def _parse_payload(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("payload")
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str) and payload.strip():
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return {}


def _answer_callback(vk, event_object: dict[str, Any] | None, *, text: str | None = None, link: str | None = None, logger=None) -> None:
    if not event_object:
        return
    event_data = {"type": "open_link", "link": link} if link else {"type": "show_snackbar", "text": text or "Открываю..."}
    try:
        vk.messages.sendMessageEventAnswer(
            event_id=event_object["event_id"],
            user_id=event_object["user_id"],
            peer_id=event_object["peer_id"],
            event_data=json.dumps(event_data, ensure_ascii=False),
        )
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка ответа на callback: {exc}")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _extract_attachment(message: dict[str, Any]) -> dict[str, Any] | None:
    attachments = message.get("attachments") or []
    if not attachments:
        return None
    raw = attachments[0]
    kind = raw.get("type")
    if kind not in {"photo", "video", "doc"}:
        return None
    item = raw[kind]
    ref = f"{kind}{item['owner_id']}_{item['id']}"
    if item.get("access_key"):
        ref = f"{ref}_{item['access_key']}"
    return {"type": kind, "vk_attachment": ref, "payload": raw}


def _send(vk, user_id: int, text: str, service: OrderService, *, keyboard: str | None = None, attachment: str | None = None, logger=None):
    params = {"user_id": user_id, "message": text or " ", "random_id": service.random_id()}
    if keyboard:
        params["keyboard"] = keyboard
    if attachment:
        params["attachment"] = attachment
    try:
        return vk.messages.send(**params)
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка отправки ЛС пользователю {user_id}: {exc}")
        return None


def _delete_pm_message(vk, peer_id: int, event_object: dict[str, Any] | None, logger=None) -> None:
    if not event_object:
        return
    try:
        cmid = event_object.get("conversation_message_id")
        if cmid:
            vk.messages.delete(peer_id=peer_id, cmids=cmid, delete_for_all=1)
            return
    except Exception:
        pass
    try:
        message_id = event_object.get("message_id")
        if message_id:
            vk.messages.delete(message_ids=message_id, delete_for_all=1)
    except Exception as exc:
        if logger:
            logger.warning(f"Не удалось удалить ЛС-сообщение {event_object}: {exc}")


def _check_subscription(vk, user_id: int, logger=None) -> bool:
    try:
        return bool(vk.groups.isMember(group_id=group_id, user_id=user_id))
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка проверки подписки: {exc}")
        return False


def _rate_days_from_text(text: str) -> int | None:
    for days, data in RATES.items():
        if text == data.get("name") or text == f"{data['name']} — {data['price']} ₽":
            return int(days)
    return None


def _get_user_name(vk, user_id: int) -> str:
    try:
        user = vk.users.get(user_ids=user_id)[0]
        return f"{user['first_name']} {user['last_name']}"
    except Exception:
        return f"Пользователь {user_id}"


def _next_order_id(orders_data: dict[str, Any]) -> str:
    order_id = f"order_{int(time.time())}"
    while order_id in orders_data:
        order_id = f"order_{int(time.time() * 1000)}"
    return order_id


def _next_order_no(service: OrderService, orders_data: dict[str, Any]) -> int:
    return service.next_order_number(orders_data)


def _next_order_code(service: OrderService, orders_data: dict[str, Any]) -> str:
    return service.next_order_code(orders_data)


def _append_photo_attachment(existing: str | None, new_attachment: str | None) -> str | None:
    if not new_attachment:
        return existing
    parts = [part.strip() for part in str(existing or "").split(",") if part.strip()]
    if new_attachment not in parts:
        parts.append(new_attachment)
    return ",".join(parts) if parts else None


def _find_pending_edit_order(
    orders_data: dict[str, Any],
    *,
    user_id: int,
    item_key: str,
    order_type: str,
) -> tuple[str | None, dict[str, Any] | None]:
    for order_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        if order.get("status") != "pending":
            continue
        if order.get("user_id") != user_id:
            continue
        if order.get("type") != order_type:
            continue
        if order.get("item_key") != item_key:
            continue
        return order_id, order
    return None, None


def _upsert_pending_edit_order(
    service: OrderService,
    orders_data: dict[str, Any],
    *,
    user_id: int,
    item_key: str,
    order_type: str,
    text: str,
    photo: str | None,
    price: int,
    days: int,
    rate_name: str | None,
    base_item: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_id, order = _find_pending_edit_order(
        orders_data,
        user_id=user_id,
        item_key=item_key,
        order_type=order_type,
    )
    created = False
    if order is None or order_id is None:
        order_id = _next_order_id(orders_data)
        order = {
            "order_no": _next_order_no(service, orders_data),
            "order_code": _next_order_code(service, orders_data),
            "type": order_type,
            "status": "pending",
            "user_id": user_id,
            "created_at": now,
            "events": [],
        }
        orders_data[order_id] = order
        created = True

    order["item_key"] = item_key
    if base_item:
        order["source_order_id"] = base_item.get("order_id")
        order["source_order_code"] = base_item.get("order_code")
    order["text"] = text
    order["photo"] = photo
    order["price"] = price
    order["days"] = days
    order["rate_name"] = rate_name
    order["updated_at"] = now
    order.setdefault("events", []).append({"at": now, "event": "created" if created else "updated"})
    return order_id, created


def _get_effective_active_ads(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, ad_data in ads_data.items():
        if key in {"main_text_sale", "active_ad", "users"}:
            continue
        if not isinstance(ad_data, dict):
            continue
        if ad_data.get("user_id") != user_id:
            continue
        expires_at = _parse_dt(ad_data.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now():
            continue
        if ad_data.get("status") not in {"approved", "active", None}:
            continue
        normalized = dict(ad_data)
        normalized["ad_key"] = ad_data.get("ad_key") or key
        items.append(normalized)
    items.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return items


def _count_user_extra_ads(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> int:
    return len(_get_effective_active_ads(service, ads_data, orders_data, user_id))


def _active_items(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for idx, active in enumerate(_get_effective_active_ads(service, ads_data, orders_data, user_id), start=1):
        items.append(
            {
                "key": active.get("ad_key") or f"extra_{idx}",
                "order_id": active.get("order_id"),
                "order_code": active.get("order_code"),
                "title": f"✅ Активная рассылка #{idx}",
                "button_title": f"🟢 Доп. реклама #{idx}",
                "extend_title": f"🔁 Продлить доп. рекламу #{idx}",
                "type": "extra",
                "text": active.get("text", "") or "Текст не указан",
                "photo": active.get("photo"),
                "price": active.get("price", 0),
                "days": active.get("days", 0),
                "rate_name": active.get("rate_name"),
                "expires_at": active.get("expires_at", "не указано"),
            }
        )
    main_text_sale = ads_data.get("main_text_sale")
    if isinstance(main_text_sale, dict) and main_text_sale.get("user_id") == user_id:
        expires_at = _parse_dt(main_text_sale.get("expires_at"))
        if expires_at is None or expires_at >= datetime.now():
            items.append(
                {
                    "key": "main_text_sale",
                    "order_id": main_text_sale.get("order_id"),
                    "order_code": main_text_sale.get("order_code"),
                    "title": "💎 Основная реклама",
                    "button_title": "💎 Осн. реклама",
                    "extend_title": "🔁 Продлить осн. рекламу",
                    "type": "main",
                    "text": main_text_sale.get("text", "") or "Текст не указан",
                    "photo": main_text_sale.get("photo"),
                    "price": main_text_sale.get("price", 0),
                    "days": main_text_sale.get("days", 0),
                    "rate_name": main_text_sale.get("rate_name"),
                    "expires_at": main_text_sale.get("expires_at", "не указано"),
                }
            )
    return items


def _main_menu(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> str:
    items = _active_items(service, ads_data, orders_data, user_id)
    has_active_ad = bool(items)
    return generate_main_menu(has_active_ad=has_active_ad, has_messages=has_active_ad)


def _kb_cancel() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _kb_yes_skip() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Да", color=VkKeyboardColor.POSITIVE, payload={"command": "add_media_yes"})
    kb.add_callback_button("➡️ Пропустить", color=VkKeyboardColor.PRIMARY, payload={"command": "skip_media"})
    return kb.get_keyboard()


def _kb_buy_menu() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("📢 Купить доп. рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_extra_ad_info"})
    kb.add_line()
    kb.add_callback_button("💎 Купить осн. рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_main_ad_info"})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_ready_back(ready_command: str) -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Готов купить", color=VkKeyboardColor.POSITIVE, payload={"command": ready_command})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "buy_menu"})
    return kb.get_keyboard()


def _kb_payment() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("📎 Добавить чек", color=VkKeyboardColor.POSITIVE, payload={"command": "add_check"})
    kb.add_line()
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _kb_check_confirm() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Да, отправить", color=VkKeyboardColor.POSITIVE, payload={"command": "submit_order"})
    kb.add_line()
    kb.add_callback_button("❌ Нет, изменить", color=VkKeyboardColor.NEGATIVE, payload={"command": "reupload_check"})
    return kb.get_keyboard()


def _kb_info(can_buy: bool) -> str:
    kb = VkKeyboard(inline=True)
    if can_buy:
        kb.add_callback_button("💰 Купить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_menu"})
        kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_active_items(items: list[dict[str, Any]]) -> str:
    kb = VkKeyboard(inline=True)
    for index, item in enumerate(items, start=1):
        kb.add_callback_button(item["button_title"], color=VkKeyboardColor.PRIMARY, payload={"command": "view_user_ad", "item_key": item["key"]})
        if index < len(items):
            kb.add_line()
    return kb.get_keyboard()


def _kb_extend_items(items: list[dict[str, Any]]) -> str:
    kb = VkKeyboard(inline=True)
    for index, item in enumerate(items, start=1):
        kb.add_callback_button(item["extend_title"], color=VkKeyboardColor.POSITIVE, payload={"command": "start_extend", "item_key": item["key"]})
        if index < len(items):
            kb.add_line()
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_active_item_actions(item: dict[str, Any]) -> str:
    kb = VkKeyboard(inline=True)
    if item.get("type") == "extra":
        kb.add_callback_button("✏️ Редактировать текст", color=VkKeyboardColor.PRIMARY, payload={"command": "edit_ad_text", "item_key": item["key"]})
        kb.add_callback_button("🖼️ Редактировать фото", color=VkKeyboardColor.SECONDARY, payload={"command": "edit_ad_photo", "item_key": item["key"]})
        kb.add_line()
        kb.add_callback_button("📎 Добавить фото", color=VkKeyboardColor.SECONDARY, payload={"command": "add_ad_photo", "item_key": item["key"]})
        kb.add_line()
    if item.get("type") == "main":
        kb.add_callback_button("✏️ Редактировать текст", color=VkKeyboardColor.PRIMARY, payload={"command": "edit_main_text", "item_key": item["key"]})
        kb.add_callback_button("🖼️ Редактировать фото", color=VkKeyboardColor.SECONDARY, payload={"command": "edit_main_photo", "item_key": item["key"]})
        kb.add_line()
        kb.add_callback_button("📎 Добавить фото", color=VkKeyboardColor.SECONDARY, payload={"command": "add_main_photo", "item_key": item["key"]})
        kb.add_line()
    kb.add_callback_button("💰 Продлить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "start_extend", "item_key": item["key"]})
    kb.add_line()
    kb.add_callback_button("🆘 Связаться с поддержкой", color=VkKeyboardColor.NEGATIVE, payload={"command": "contact_support", "item_key": item["key"]})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".сообщение"})
    return kb.get_keyboard()


def _render_active_item(item: dict[str, Any]) -> str:
    return (
        f"{item['title']}\n\n"
        f"{item.get('text', '') or 'Текст не указан'}\n"
        f"Срок до: {item.get('expires_at', 'не указано')}\n"
        f"Тариф: {item.get('rate_name') or '-'}"
    )


def _extra_ad_description() -> str:
    return (
        "📢 ДОПОЛНИТЕЛЬНАЯ РЕКЛАМА\n\n"
        "Ваш текст попадёт в общую рассылку по чатам.\n"
        "Такие тексты отправляются в случайном порядке вместе с другими заказами.\n"
        "Во все чаты сообщение всё равно будет отправлено.\n\n"
        "Если всё подходит, нажмите «Готов купить»."
    )


def _main_ad_description(can_buy: bool, reason: str | None) -> str:
    if not can_buy:
        return (
            "💎 ОСНОВНАЯ РЕКЛАМА\n\n"
            "Основной покупаемый текст отправляется отдельным блоком в конце рассылки.\n\n"
            f"⚠️ Сейчас место занято.\n{reason}"
        )
    return (
        "💎 ОСНОВНАЯ РЕКЛАМА\n\n"
        "Этот текст отправляется отдельным основным блоком в конце рассылки.\n"
        "Он выделяется сильнее обычной дополнительной рекламы.\n\n"
        "Если всё подходит, нажмите «Готов купить»."
    )


def _notify_admin(vk, message_config: dict[str, Any], service: OrderService, *, extension_only: bool = False, logger=None):
    admin_chat = message_config.get("admin_chat")
    if not admin_chat:
        return
    text = "📥 НОВАЯ ЗАЯВКА НА ПРОДЛЕНИЕ\nИспользуйте .заказы для просмотра" if extension_only else "📥 НОВАЯ ЗАЯВКА\nИспользуйте .заказы для просмотра"
    try:
        vk.messages.send(peer_id=admin_chat, message=text, random_id=service.random_id())
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка отправки уведомления в админ-чат: {exc}")


def _notify_support_request(vk, message_config: dict[str, Any], service: OrderService, user_id: int, item: dict[str, Any], logger=None):
    admin_chat = message_config.get("admin_chat")
    if not admin_chat:
        return
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("💬 Перейти в чат", color=VkKeyboardColor.PRIMARY, payload={"command": "support_open_chat", "user_id": user_id})
    kb.add_line()
    kb.add_callback_button("✅ Закрыто", color=VkKeyboardColor.POSITIVE, payload={"command": "support_close", "user_id": user_id})
    try:
        vk.messages.send(
            peer_id=admin_chat,
            message=(
                "🆘 Пользователь связался со службой поддержки\n\n"
                f"Покупатель: [id{user_id}|Пользователь]\n"
                f"Реклама: {item.get('title', 'не указано')}\n"
                f"Текст: {(item.get('text', '') or 'Текст не указан')[:300]}\n"
                f"Причина: {item.get('support_reason', 'не указана')}"
            ),
            keyboard=kb.get_keyboard(),
            random_id=service.random_id(),
        )
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка отправки заявки в поддержку в админ-чат: {exc}")


def _reset_state(state: dict[str, Any]) -> None:
    flow = state.get("flow")
    support_mode = state.get("support_mode")
    admin_order_edit = state.get("admin_order_edit")
    state.clear()
    if support_mode:
        state["support_mode"] = support_mode
    if admin_order_edit:
        state["admin_order_edit"] = admin_order_edit
    if flow and flow.get("persistent"):
        state["flow"] = flow


def _find_item(items: list[dict[str, Any]], item_key: str | None) -> dict[str, Any] | None:
    for item in items:
        if item.get("key") == item_key:
            return item
    return None


def handle_personal_message(
    vk,
    event,
    user_id: int,
    ads_data: dict[str, Any],
    orders_data: dict[str, Any],
    message_config: dict[str, Any],
    service: OrderService,
    save_ads,
    save_orders,
    logger=None,
):
    message = event.obj.message
    payload = _parse_payload(message)
    text = (message.get("text") or "").strip()
    raw_command = payload.get("command") or text
    command = _normalize_command(raw_command)
    event_object = message.get("_event_object")
    attachment = _extract_attachment(message)

    if event_object:
        _answer_callback(vk, event_object, logger=logger)
        if command not in PERSISTENT_CALLBACK_COMMANDS:
            _delete_pm_message(vk, user_id, event_object, logger=logger)

    state = USER_STATES.setdefault(str(user_id), {"menu_sent": False})
    flow = state.get("flow")

    if state.get("support_mode"):
        return

    if command == "cancel":
        _reset_state(state)
        _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, text="Отменено", logger=logger)
        return

    if command in {"начать", "start"}:
        _reset_state(state)
        _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "помощь":
        _send(vk, user_id, HELP_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "инфо_о_нас":
        can_buy, reason = service.can_buy_main_text(ads_data)
        msg = INFO_TEXT if can_buy else f"{INFO_TEXT}\n\n⚠️ Место основного текста занято.\n{reason}"
        _send(vk, user_id, msg, service, keyboard=_kb_info(can_buy), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "сообщение":
        items = _active_items(service, ads_data, orders_data, user_id)
        if not items:
            _send(vk, user_id, "📭 У вас пока нет активной рекламы.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, "📨 ВАШИ АКТИВНЫЕ ЗАКАЗЫ\n\nВыберите рекламу для просмотра и управления:", service, keyboard=_kb_active_items(items), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "продлить":
        items = _active_items(service, ads_data, orders_data, user_id)
        if not items:
            _send(vk, user_id, "📭 У вас пока нет активной рекламы для продления.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, "💰 Выберите рекламу, которую хотите продлить:", service, keyboard=_kb_extend_items(items), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command in {"купить", "buy_menu"}:
        _reset_state(state)
        if _count_user_extra_ads(service, ads_data, orders_data, user_id) >= 3:
            _send(
                vk,
                user_id,
                "⚠️ У вас уже максимум дополнительных реклам.\n\nМожно купить не больше 3 доп. текстов одновременно.",
                service,
                keyboard=_main_menu(service, ads_data, orders_data, user_id),
                logger=logger,
            )
            return
        _send(vk, user_id, "Выберите тип рекламы:", service, keyboard=_kb_buy_menu(), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "buy_extra_ad_info":
        _send(vk, user_id, _extra_ad_description(), service, keyboard=_kb_ready_back("ready_buy_extra"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "buy_main_ad_info":
        can_buy, reason = service.can_buy_main_text(ads_data)
        _send(vk, user_id, _main_ad_description(can_buy, reason), service, keyboard=_kb_ready_back("ready_buy_main" if can_buy else "buy_menu"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "ready_buy_extra":
        _send(vk, user_id, "📅 Выберите срок рассылки:", service, keyboard=generate_rates_menu(), logger=logger)
        return

    if command == "ready_buy_main":
        _send(vk, user_id, "📅 Выберите срок основной рекламы:", service, keyboard=generate_main_text_rates_menu(), logger=logger)
        return

    items = _active_items(service, ads_data, orders_data, user_id)

    if command == "view_user_ad":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, _render_active_item(item), service, keyboard=_kb_active_item_actions(item), attachment=item.get("photo"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "start_extend":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        state["flow"] = {"step": "wait_extend_rate", "base_item": item, "item_key": item["key"], "mode": "renew_main_text" if item.get("type") == "main" else "extend"}
        if item.get("type") == "main":
            _send(vk, user_id, "📅 Выберите срок продления основной рекламы:", service, keyboard=generate_main_text_rates_menu(), logger=logger)
        else:
            _send(vk, user_id, "📅 Выберите срок продления:", service, keyboard=generate_rates_menu(), logger=logger)
        return

    if command == "contact_support":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        state["flow"] = {"step": "wait_support_reason", "base_item": item, "item_key": item["key"]}
        _send(vk, user_id, "✍️ Перед вызовом администратора напишите причину обращения следующим сообщением.", service, keyboard=_kb_cancel(), logger=logger)
        return

    if command in {"edit_ad_text", "edit_ad_photo", "add_ad_photo", "edit_main_text", "edit_main_photo", "add_main_photo"}:
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        if command == "edit_ad_text":
            state["flow"] = {"step": "wait_update_text", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "rate_name": item.get("rate_name")}
            _send(vk, user_id, "✏️ Пришлите новый текст для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_main_text":
            state["flow"] = {"step": "wait_update_main_text", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name")}
            _send(vk, user_id, "✏️ Пришлите новый текст для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_ad_photo":
            state["flow"] = {"step": "wait_update_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "rate_name": item.get("rate_name"), "append_photo": False}
            _send(vk, user_id, "🖼️ Пришлите новое фото для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_main_photo":
            state["flow"] = {"step": "wait_update_main_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name"), "append_photo": False}
            _send(vk, user_id, "🖼️ Пришлите новое фото для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "add_ad_photo":
            state["flow"] = {"step": "wait_update_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "rate_name": item.get("rate_name"), "append_photo": True}
            _send(vk, user_id, "📎 Пришлите дополнительное фото для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "add_main_photo":
            state["flow"] = {"step": "wait_update_main_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name"), "append_photo": True}
            _send(vk, user_id, "📎 Пришлите дополнительное фото для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return

    if flow and flow.get("step") == "wait_support_reason" and text and not text.startswith("."):
        item = dict(flow.get("base_item") or {})
        item["support_reason"] = text
        _notify_support_request(vk, message_config, service, user_id, item, logger=logger)
        _send(vk, user_id, "✅ Заявка в поддержку отправлена. Ожидайте администратора.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_extend_rate":
        selected_days = payload.get("days")
        if selected_days is None:
            selected_days = _rate_days_from_text(text)
        if selected_days is not None:
            selected_days = int(selected_days)
            base_item = flow.get("base_item") or {}
            rates_map = MAIN_TEXT_RATES if base_item.get("type") == "main" else RATES
            rate = rates_map[selected_days]
            flow.update(
                {
                    "step": "payment",
                    "days": selected_days,
                    "price": rate["price"],
                    "rate_name": rate["name"],
                    "text": base_item.get("text", ""),
                    "photo": base_item.get("photo"),
                }
            )
            _send(vk, user_id, PAYMENT_TEXT + f"\n\nСумма к оплате: {flow['price']} ₽", service, keyboard=_kb_payment(), logger=logger)
            return

    if command == "rate_select":
        selected_days = payload.get("days")
        if selected_days is not None:
            selected_days = int(selected_days)
            rate = RATES[selected_days]
            state["flow"] = {"step": "wait_text", "days": selected_days, "price": rate["price"], "rate_name": rate["name"], "text": "", "photo": None, "mode": "new_ad"}
            _send(vk, user_id, "✍️ Пришлите текст для рассылки", service, keyboard=_kb_cancel(), logger=logger)
            return

    if command == "buy_main_text":
        selected_days = payload.get("days")
        if selected_days is not None:
            selected_days = int(selected_days)
            rate = MAIN_TEXT_RATES[selected_days]
            state["flow"] = {"step": "wait_text", "days": selected_days, "price": rate["price"], "rate_name": rate["name"], "text": "", "photo": None, "mode": "main_text"}
            _send(vk, user_id, "✍️ Пришлите текст для основной рекламы", service, keyboard=_kb_cancel(), logger=logger)
            return

    if flow and flow.get("step") == "wait_text" and text and not text.startswith("."):
        flow["text"] = text
        flow["step"] = "choose_media"
        _send(vk, user_id, f"📄 Ваш текст:\n\n{text}\n\nДобавить фото?", service, keyboard=_kb_yes_skip(), logger=logger)
        return

    if flow and flow.get("step") == "choose_media":
        if command == "add_media_yes":
            flow["step"] = "wait_media"
            _send(vk, user_id, "📎 Пришлите фото", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "skip_media":
            flow["step"] = "payment"
            _send(vk, user_id, PAYMENT_TEXT + f"\n\nСумма к оплате: {flow['price']} ₽", service, keyboard=_kb_payment(), logger=logger)
            return

    if flow and flow.get("step") == "wait_media" and attachment and attachment["type"] in {"photo", "video"}:
        flow["photo"] = attachment["vk_attachment"]
        flow["step"] = "payment"
        _send(vk, user_id, PAYMENT_TEXT + f"\n\nСумма к оплате: {flow['price']} ₽", service, keyboard=_kb_payment(), logger=logger)
        return

    if flow and flow.get("step") == "wait_update_text" and text and not text.startswith(".") and flow.get("base_item") is not None:
        flow["text"] = text
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="update_ad",
            text=flow.get("text", ""),
            photo=flow.get("photo"),
            price=0,
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение текста отправлена на модерацию!" if created else "✅ Существующая заявка на изменение текста обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_photo" and attachment and attachment["type"] in {"photo", "video"} and flow.get("base_item") is not None:
        photo_value = attachment["vk_attachment"]
        if flow.get("append_photo"):
            photo_value = _append_photo_attachment(flow.get("photo"), photo_value)
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="update_ad",
            text=flow.get("text", ""),
            photo=photo_value,
            price=0,
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение фото отправлена на модерацию!" if created else "✅ Существующая заявка на изменение фото обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_main_text" and text and not text.startswith(".") and flow.get("base_item") is not None:
        flow["text"] = text
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="main_text",
            text=flow.get("text", ""),
            photo=flow.get("photo"),
            price=flow.get("price", 0),
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение текста основной рекламы отправлена на модерацию!" if created else "✅ Существующая заявка на изменение основной рекламы обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_main_photo" and attachment and attachment["type"] in {"photo", "video"} and flow.get("base_item") is not None:
        photo_value = attachment["vk_attachment"]
        if flow.get("append_photo"):
            photo_value = _append_photo_attachment(flow.get("photo"), photo_value)
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="main_text",
            text=flow.get("text", ""),
            photo=photo_value,
            price=flow.get("price", 0),
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение фото основной рекламы отправлена на модерацию!" if created else "✅ Существующая заявка на изменение фото основной рекламы обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "payment" and command == "add_check":
        flow["step"] = "wait_check"
        _send(vk, user_id, "📎 Отправьте скриншот чека или файл.", service, keyboard=_kb_cancel(), logger=logger)
        return

    if flow and flow.get("step") == "wait_check" and attachment and attachment["type"] in {"photo", "doc"}:
        flow["attachment"] = attachment
        flow["step"] = "confirm_check"
        _send(vk, user_id, "✅ Чек получен. Всё верно?", service, keyboard=_kb_check_confirm(), logger=logger)
        return

    if flow and flow.get("step") == "confirm_check":
        if command == "reupload_check":
            flow["step"] = "wait_check"
            _send(vk, user_id, "📎 Отправьте новый чек.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "submit_order":
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order_id = _next_order_id(orders_data)
            order_type = "main_text" if flow.get("mode") in {"main_text", "renew_main_text"} else ("extend_ad" if flow.get("mode") == "extend" else "new_ad")
            orders_data[order_id] = {
                "order_no": _next_order_no(service, orders_data),
                "order_code": _next_order_code(service, orders_data),
                "type": order_type,
                "status": "pending",
                "user_id": user_id,
                "text": flow.get("text", ""),
                "photo": flow.get("photo"),
                "price": flow.get("price", 0),
                "days": flow.get("days", 0),
                "rate_name": flow.get("rate_name"),
                "created_at": now,
                "attachment": flow.get("attachment"),
                "events": [{"at": now, "event": "created"}],
            }
            base_item = flow.get("base_item")
            if base_item:
                orders_data[order_id]["item_key"] = base_item.get("key")
                orders_data[order_id]["source_order_id"] = base_item.get("order_id")
                orders_data[order_id]["source_order_code"] = base_item.get("order_code")
            save_orders()
            _notify_admin(vk, message_config, service, extension_only=(order_type == "extend_ad"), logger=logger)
            if order_type == "main_text":
                done_text = "📨 Заявка на покупку основной рекламы отправлена на модерацию!\nОжидайте подтверждения."
            elif order_type == "extend_ad":
                done_text = "📨 Заявка на продление отправлена на модерацию!\nОжидайте подтверждения."
            else:
                done_text = "📨 Ваша заявка отправлена на модерацию!\nОжидайте подтверждения."
            _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            _reset_state(state)
            return

    _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
