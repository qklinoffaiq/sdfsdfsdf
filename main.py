from __future__ import annotations

import json
import random
import re
import threading
import time
from urllib.request import urlopen
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.upload import VkUpload

from config import MESSAGE_CONFIG, SUBSCRIPTION_CHANNEL_LINK, admin_ids, cd_min, dev_ids, group_id, group_token, interval_sec
from handlers.chat_handler import build_order_action_keyboard, build_orders_history_keyboard, build_orders_list_keyboard, parse_message_payload, render_order_details, render_order_result, render_orders_history_text, render_orders_list_text
from handlers.ls_handler import USER_STATES, handle_personal_message
from menu import generate_main_menu
from services.order_service import OrderService
from utils.db import ensure_json_file, read_json, write_json_atomic
from utils.logger import get_logger


logger = get_logger()
OWNER_ID = 574393629
BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data.json"
USERS_DB_PATH = BASE_DIR / "users_db.json"
INCOMES_PATH = BASE_DIR / "incomes.json"

ensure_json_file(DATA_PATH, MESSAGE_CONFIG)
ensure_json_file(USERS_DB_PATH, {})
ensure_json_file(INCOMES_PATH, [])

order_service = OrderService(BASE_DIR)
runtime_data = read_json(DATA_PATH, MESSAGE_CONFIG)
chat_ids = runtime_data.get("chat_ids", MESSAGE_CONFIG.get("chat_ids", []))
admin_chat = runtime_data.get("admin_chat", MESSAGE_CONFIG.get("admin_chat"))
message_text = runtime_data.get("message_text", runtime_data.get("text", MESSAGE_CONFIG.get("text", "")))
message_photo_path = runtime_data.get("photo_path", MESSAGE_CONFIG.get("photo_path"))

vk_session = vk_api.VkApi(token=group_token)
vk = vk_session.get_api()
longpoll = VkBotLongPoll(vk_session, group_id)
longpoll_enabled = True
vk_upload = VkUpload(vk_session)

ads_data = order_service.load_ads()
orders_data = order_service.load_orders()
broadcast_thread: threading.Thread | None = None
broadcast_lock = threading.Lock()
uploaded_message_photo: str | None = None
expiration_thread: threading.Thread | None = None
auto_broadcast_thread: threading.Thread | None = None
EXPIRY_WARNING_DAY = "expiry_warn_1d_at"
EXPIRY_WARNING_HOUR = "expiry_warn_1h_at"
EXPIRY_REMOVED_AT = "expired_removed_at"
last_broadcast_time = time.time()


def generate_random_id() -> int:
    return int(time.time() * 1000000) % (2**31 - 1)


def load_json_file(path: str | Path, default: Any) -> Any:
    return read_json(path, default)


def load_users() -> dict[str, Any]:
    return read_json(USERS_DB_PATH, {})


def save_users(data: dict[str, Any]) -> None:
    write_json_atomic(USERS_DB_PATH, data)


def get_role(user_id: int) -> str | None:
    if user_id in dev_ids:
        return "dev"
    if user_id in admin_ids:
        return "admin"
    users = load_users()
    return users.get(str(user_id), {}).get("role")


def has_permission(user_id: int, level: str) -> bool:
    role = get_role(user_id)
    if level == "dev":
        return role == "dev"
    if level == "admin":
        return role in {"admin", "dev"}
    return False


def update_user_stats(user_id: int, action: str) -> None:
    users = load_users()
    user = users.setdefault(
        str(user_id),
        {
            "role": "user",
            "osn_photo_count": 0,
            "osn_text_count": 0,
            "total_messages": 0,
            "last_message": "",
            "stats": {},
        },
    )
    stats = user.setdefault("stats", {})
    stats[action] = int(stats.get(action, 0) or 0) + 1
    stats["last_activity"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if action == "osn_photo":
        user["osn_photo_count"] = int(user.get("osn_photo_count", 0) or 0) + 1
    elif action == "osn_text":
        user["osn_text_count"] = int(user.get("osn_text_count", 0) or 0) + 1
    elif action == "command":
        user["total_messages"] = int(user.get("total_messages", 0) or 0) + 1
    user["last_message"] = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {action}"
    save_users(users)


def save_ads() -> None:
    order_service.save_ads(ads_data)


def save_orders() -> None:
    order_service.save_orders(orders_data)


def append_income(user_id: int, amount: int, description: str) -> None:
    incomes = load_json_file(INCOMES_PATH, [])
    incomes.append({"user_id": user_id, "amount": amount, "description": description, "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    write_json_atomic(INCOMES_PATH, incomes)


def save_runtime_data() -> None:
    runtime_data["chat_ids"] = chat_ids
    runtime_data["admin_chat"] = admin_chat
    runtime_data["message_text"] = message_text
    runtime_data["photo_path"] = message_photo_path
    runtime_data["additional_texts"] = runtime_data.get("additional_texts", [])
    runtime_data["additional_photos_by_text"] = runtime_data.get("additional_photos_by_text", {})
    write_json_atomic(DATA_PATH, runtime_data)


def remove_chat_from_broadcast_list(chat_id: int, reason: str) -> None:
    if chat_id == admin_chat:
        return
    if chat_id in chat_ids:
        chat_ids.remove(chat_id)
        save_runtime_data()
        logger.info(f"Chat {chat_id}: удалён из списка рассылки ({reason})")


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def is_user_mirror_ad_key(key: str, ad_data: dict[str, Any]) -> bool:
    user_id = ad_data.get("user_id")
    return str(key).isdigit() and user_id is not None and str(key) == str(user_id)


def get_help_text(role: str | None) -> str:
    user_commands = (
        "📋 Основные команды:\n"
        "🔹 .пинг — проверить, работает ли бот\n"
        "🔹 .стата — посмотреть свою статистику\n"
    )

    admin_commands = (
        "🔸 Административные команды:\n"
        "🔹 .редтекст [номер] [текст] — редактировать дополнительный текст\n"
        "🔹 .рассылка — запустить рассылку\n"
        "🔹 .список — показать количество чатов\n"
        "🔹 .ид — узнать ID текущего чата\n"
        "🔹 .инфо — показать текущие настройки\n"
        "🔹 .хелп — показать это сообщение\n"
        "🔹 .тест — отправить тестовое сообщение\n"
        "🔹 .списокрекламы — показать список действующих заказов рекламы\n"
        "🔹 .допсписок — показать список дополнительных текстов\n"
        "🔹 .заказы — показать заявки на модерации\n"
        "🔹 .историязаказов — показать обработанные заказы\n"
        "🔹 .уст — добавить текущий чат в рассылку\n"
        "🔹 .инфочат — получить информацию о чате\n"
        "🔹 .добид [число] — добавить указанное количество ID в список\n"
        "🔹 .делид [число] — удалить указанное количество ID с конца списка\n"
        "🔹 .добфото [номер] — добавить фото к дополнительному тексту\n"
        "🔹 .удфото [номер] — удалить фото у дополнительного текста\n"
    )

    dev_commands = (
        "🔧 Команды разработчика:\n"
        "🔹 .админ [id/@] — выдать или снять права администратора\n"
        "🔹 .разраб [id/@/ответ] — выдать или снять права разработчика\n"
        "🔹 .настройки [cd_min|interval_sec] [число] — изменить тайминги\n"
        "🔹 .редоснтекст [текст] — изменить основной текст рассылки\n"
        "🔹 .редоснфото — изменить основное фото рассылки\n"
        "🔹 .gzov — разослать только основное сообщение бота во все чаты\n"
        "🔹 .стафф — показать состав персонала\n"
        "🔹 .админчат — установить текущий чат как административный\n"
        "🔹 .доходы — посмотреть статистику доходов\n"
        "🔹 .добзаказ @username [дни] [текст] — добавить заказ вручную\n"
        "🔹 .удзаказ @username — удалить активный заказ пользователя\n"
        "🔹 .измзаказ <order_id|номер> <поле> <значение> — изменить заказ\n"
    )

    full_text = user_commands
    if role in {"admin", "dev"}:
        full_text += "\n\n" + admin_commands
    if role == "dev":
        full_text += "\n\n" + dev_commands
    return full_text


def get_user_display_name(user_id: int) -> str:
    try:
        user_info = vk.users.get(user_ids=user_id)[0]
        return f"{user_info['first_name']} {user_info['last_name']}"
    except Exception:
        return f"Пользователь {user_id}"


def render_user_stats(user_id: int) -> str:
    users = load_users()
    user = users.get(str(user_id), {})
    stats = user.get("stats", {})
    total_commands = int(user.get("total_messages", stats.get("command", 0)) or 0)
    return (
        "📊 Ваша статистика\n\n"
        f"ID: {user_id}\n"
        f"Команд использовано: {total_commands}\n"
        f"Изменений основного фото: {int(user.get('osn_photo_count', 0) or 0)}\n"
        f"Изменений основного текста: {int(user.get('osn_text_count', 0) or 0)}\n"
        f"Последняя активность: {stats.get('last_activity', user.get('last_message', 'нет данных'))}"
    )


def render_user_stats_detailed(user_id: int) -> str:
    users = load_users()
    user = users.get(str(user_id), {})
    stats = user.get("stats", {})
    total_commands = int(user.get("total_messages", stats.get("command", 0)) or 0)
    role_names = {"user": "Пользователь", "admin": "Администратор", "dev": "Разработчик"}
    role_display = role_names.get(user.get("role", "user"), "Пользователь")
    return (
        f"👤 Информация о пользователе:\n\n"
        f"🔹 Имя: {get_user_display_name(user_id)}\n"
        f"🔹 Роль: {role_display}\n"
        f"🔹 Изменения текста/фото: {int(user.get('osn_text_count', 0) or 0) + int(user.get('osn_photo_count', 0) or 0)}\n"
        f"🔹 Всего сообщений для бота: {total_commands}\n"
        f"🔹 Последнее сообщение: {user.get('last_message', 'Неизвестно') or 'Неизвестно'}"
    )


def render_staff_detailed() -> str:
    users = load_users()
    devs = []
    admins = []
    try:
        owner_info = vk.users.get(user_ids=OWNER_ID)[0]
        owner_name = f"{owner_info['first_name']} {owner_info['last_name']}"
        devs.append(f"• [id{OWNER_ID}|{owner_name}]")
    except Exception:
        devs.append(f"• [id{OWNER_ID}|Разработчик]")

    for uid, data in users.items():
        if data.get("role") == "admin":
            try:
                info = vk.users.get(user_ids=int(uid))[0]
                name = f"{info['first_name']} {info['last_name']}"
                admins.append(f"• [id{uid}|{name}]")
            except Exception:
                admins.append(f"• [id{uid}|Администратор]")

    return "🔧 Список персонала бота:\n\nРазработчик:\n" + "\n".join(devs) + "\n\nАдминистраторы:\n" + ("\n".join(admins) if admins else "Нет назначенных администраторов")


def render_runtime_info() -> str:
    return (
        "ℹ️ Текущие настройки\n\n"
        f"Чатов в рассылке: {len(chat_ids)}\n"
        f"Админ-чат: {admin_chat or 'не задан'}\n"
        f"Интервал между сообщениями: {interval_sec}\n"
        f"Авторассылка каждые: {cd_min} мин.\n"
        f"Основной текст: {len(message_text or '')} символов"
    )


def render_runtime_info_legacy() -> str:
    additional_texts = get_additional_texts()
    info_parts = [
        "📊 ИНФОРМАЦИЯ О НАСТРОЙКАХ",
        "",
        f"⏱️ Интервал между рассылками: *{cd_min}* минут",
        f"⚡ Интервал отправки сообщений: *{interval_sec}* секунд",
        "",
        "📝 ТЕКСТ РАССЫЛКИ:",
        "" if not (message_text or "").strip() else message_text,
        "",
    ]
    if additional_texts:
        info_parts.append("📎 ДОПОЛНИТЕЛЬНЫЕ ТЕКСТЫ:")
        for i, add_text in enumerate(additional_texts, 1):
            if add_text.strip():
                info_parts.append(f"{i}. {add_text.strip()}")
        info_parts.append("")
    return "\n".join(info_parts)


def save_config_value(name: str, value_repr: str) -> None:
    config_path = BASE_DIR / "config.py"
    lines = config_path.read_text(encoding="utf-8").splitlines()
    updated = False
    new_lines: list[str] = []
    for line in lines:
        if line.startswith(f"{name} ="):
            new_lines.append(f"{name} = {value_repr}")
            updated = True
        else:
            new_lines.append(line)
    if updated:
        config_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def render_staff() -> str:
    users = load_users()
    dynamic_admins = [int(uid) for uid, data in users.items() if data.get("role") == "admin"]
    dynamic_devs = [int(uid) for uid, data in users.items() if data.get("role") == "dev"]
    all_admins = sorted(set(admin_ids + dynamic_admins))
    all_devs = sorted(set(dev_ids + dynamic_devs))
    admin_list = ", ".join(f"id{item}" for item in all_admins) or "нет"
    dev_list = ", ".join(f"id{item}" for item in all_devs) or "нет"
    return f"👥 Состав персонала\n\nАдминистраторы: {admin_list}\nРазработчики: {dev_list}"


def get_additional_texts() -> list[str]:
    value = runtime_data.get("additional_texts", [])
    return value if isinstance(value, list) else []


def get_additional_photos() -> dict[str, list[str]]:
    value = runtime_data.get("additional_photos_by_text", {})
    return value if isinstance(value, dict) else {}


def save_additional_texts(texts: list[str], photos_map: dict[str, list[str]] | None = None) -> None:
    runtime_data["additional_texts"] = texts
    if photos_map is not None:
        runtime_data["additional_photos_by_text"] = photos_map
    save_runtime_data()


def send_message(chat_id: int, text: str, attachment: str | None = None, keyboard: str | None = None) -> Any:
    params = {"peer_id": chat_id, "message": text or " ", "random_id": generate_random_id()}
    if attachment:
        params["attachment"] = attachment
    if keyboard:
        params["keyboard"] = keyboard
    try:
        return vk.messages.send(**params)
    except Exception as exc:
        error_text = str(exc)
        error_lower = error_text.lower()
        if "the user was kicked out of the conversation" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "участник исключён из беседы")
            return None
        if "you are restricted to write to a chat" in error_lower or "code 983" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "ограничение на запись")
            return None
        if "ошибка доступа к чату" in error_lower or "you don't have access to this chat" in error_lower or "access denied" in error_lower:
            logger.warning(f"Chat {chat_id}: ошибка доступа. Рассылка приостановлена.")
            return "access_error"
        logger.error(f"Ошибка отправки в чат {chat_id}: {exc}")
        return None


def build_extend_specific_keyboard(item_key: str, is_main: bool) -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "💰 Продлить рекламу",
        color=VkKeyboardColor.POSITIVE,
        payload={"command": "start_extend", "item_key": item_key},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📨 Мои сообщения",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": ".сообщение"},
    )
    return keyboard.get_keyboard()


def upload_message_photo() -> str | None:
    global uploaded_message_photo
    if uploaded_message_photo:
        return uploaded_message_photo
    if not message_photo_path:
        return None
    if isinstance(message_photo_path, str) and message_photo_path.startswith(("photo", "doc", "video")):
        uploaded_message_photo = message_photo_path
        return uploaded_message_photo

    photo_path = Path(message_photo_path)
    if not photo_path.is_absolute():
        photo_path = BASE_DIR / photo_path
    if not photo_path.exists():
        logger.warning(f"Файл фото для основной рассылки не найден: {photo_path}")
        return None

    try:
        photo = vk_upload.photo_messages(str(photo_path))[0]
        uploaded_message_photo = f"photo{photo['owner_id']}_{photo['id']}"
        return uploaded_message_photo
    except Exception as exc:
        logger.error(f"Не удалось загрузить фото основной рассылки: {exc}")
        return None


def safe_send_pm(user_id: int, text: str, keyboard: str | None = None) -> Any:
    try:
        params = {"user_id": user_id, "message": text or " ", "random_id": generate_random_id()}
        if keyboard:
            params["keyboard"] = keyboard
        return vk.messages.send(**params)
    except Exception as exc:
        error_text = str(exc)
        if "[901]" in error_text or "Can't send messages for users without permission" in error_text:
            logger.warning(f"Не удалось отправить ЛС {user_id}: пользователь не разрешил сообщения сообщества.")
            return None
        logger.error(f"Ошибка отправки ЛС {user_id}: {exc}")
        return None

def answer_callback_event(
    event_object: dict[str, Any],
    text: str = "Открываю...",
    *,
    link: str | None = None,
) -> None:
    try:
        event_data = {"type": "open_link", "link": link} if link else {"type": "show_snackbar", "text": text}
        vk.messages.sendMessageEventAnswer(
            event_id=event_object["event_id"],
            user_id=event_object["user_id"],
            peer_id=event_object["peer_id"],
            event_data=json.dumps(event_data, ensure_ascii=False),
        )
    except Exception as exc:
        logger.warning(f"Не удалось отправить callback-ответ: {exc}")

        return None


def is_subscribed(user_id: int) -> bool:
    try:
        return bool(vk.groups.isMember(group_id=group_id, user_id=user_id))
    except Exception as exc:
        logger.warning(f"Ошибка проверки подписки {user_id}: {exc}")
        return True


def render_income_stats() -> str:
    incomes = load_json_file(INCOMES_PATH, [])
    now = datetime.now()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    def parse_created_at(item: dict[str, Any]) -> datetime | None:
        try:
            return datetime.strptime(item["created_at"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    day_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= day_start))
    week_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= week_ago))
    month_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= month_ago))
    total_income = sum(int(i.get("amount", 0) or 0) for i in incomes)
    return (
        "📊 Статистика доходов разработчика\n\n"
        f"📅 За сегодня: {day_income} ₽\n"
        f"📆 За неделю: {week_income} ₽\n"
        f"🗓 За месяц: {month_income} ₽\n"
        f"💎 За всё время: {total_income} ₽"
    )


def build_buy_ad_keyboard() -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("💰 Купить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_main_text_dm"})
    return keyboard.get_keyboard()


def extract_attachment_reference(message: dict[str, Any]) -> str | None:
    attachments = message.get("attachments") or []
    if not attachments:
        return None
    raw = attachments[0]
    kind = raw.get("type")
    if kind not in {"photo", "video", "doc"}:
        return None
    item = raw[kind]
    reference = f"{kind}{item['owner_id']}_{item['id']}"
    if item.get("access_key"):
        reference = f"{reference}_{item['access_key']}"
    return reference


def attachment_reference_from_order(order: dict[str, Any] | None) -> str | None:
    if not order:
        return None
    if order.get("photo"):
        return str(order["photo"])
    return check_attachment_reference_from_order(order)


def check_attachment_reference_from_order(order: dict[str, Any] | None) -> str | None:
    if not order:
        return None
    attachment = order.get("attachment")
    if isinstance(attachment, dict):
        if attachment.get("vk_attachment"):
            return str(attachment["vk_attachment"])
        kind = attachment.get("type")
        if kind in {"photo", "doc", "video"}:
            nested = attachment.get(kind)
            if isinstance(nested, dict) and nested.get("owner_id") and nested.get("id"):
                reference = f"{kind}{nested['owner_id']}_{nested['id']}"
                if nested.get("access_key"):
                    reference = f"{reference}_{nested['access_key']}"
                return reference
            if attachment.get("owner_id") and attachment.get("id"):
                reference = f"{kind}{attachment['owner_id']}_{attachment['id']}"
                if attachment.get("access_key"):
                    reference = f"{reference}_{attachment['access_key']}"
                return reference
    return None


def extract_attachment_references(message: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for raw in message.get("attachments") or []:
        kind = raw.get("type")
        if kind not in {"photo", "video", "doc"}:
            continue
        item = raw[kind]
        reference = f"{kind}{item['owner_id']}_{item['id']}"
        if item.get("access_key"):
            reference = f"{reference}_{item['access_key']}"
        refs.append(reference)
    return refs


def get_active_random_orders() -> list[dict[str, Any]]:
    current_ads = order_service.load_ads()
    active_items: list[dict[str, Any]] = []
    for user_key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if user_key in {"main_text_sale", "active_ad", "users"}:
            continue
        if is_user_mirror_ad_key(user_key, ad_data):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue
        expires_at = ad_data.get("expires_at")
        if expires_at:
            try:
                if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S") < datetime.now():
                    continue
            except ValueError:
                pass
        active_items.append(ad_data)
    random.shuffle(active_items)
    return active_items


def send_broadcast_to_chat(chat_id: int) -> None:
    global ads_data
    ads_data = order_service.load_ads()
    logger.info(f"Рассылка: отправка в чат {chat_id}")
    for ad in get_active_random_orders():
        result = send_message(chat_id, ad.get("text", ""), attachment=ad.get("photo"))
        if result == "access_error":
            logger.warning(f"Рассылка остановлена: потерян доступ к чату {chat_id}")
            return "access_error"
        time.sleep(interval_sec)

    if message_text:
        result = send_message(
            chat_id,
            message_text,
            attachment=upload_message_photo(),
            keyboard=build_buy_ad_keyboard(),
        )
        if result == "access_error":
            logger.warning(f"Рассылка остановлена на основном сообщении бота: потерян доступ к чату {chat_id}")
            return "access_error"
        time.sleep(interval_sec)

    main_text_sale = ads_data.get("main_text_sale", {})
    if main_text_sale and main_text_sale.get("status") == "active":
        expires_at = main_text_sale.get("expires_at")
        is_active = True
        if expires_at:
            try:
                is_active = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S") > datetime.now()
            except ValueError:
                is_active = True
        if is_active:
            result = send_message(
                chat_id,
                main_text_sale.get("text", ""),
                attachment=main_text_sale.get("photo"),
            )
            if result == "access_error":
                logger.warning(f"Рассылка остановлена на основном продаваемом тексте: потерян доступ к чату {chat_id}")
                return "access_error"
            time.sleep(interval_sec)
    return None

def iter_active_ads() -> list[tuple[str, dict[str, Any]]]:
    current_ads = order_service.load_ads()
    result: list[tuple[str, dict[str, Any]]] = []
    for key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"active_ad", "users"}:
            continue
        if is_user_mirror_ad_key(key, ad_data):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue
        expires_at = ad_data.get("expires_at")
        if expires_at:
            try:
                if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S") < datetime.now():
                    continue
            except ValueError:
                pass
        result.append((key, ad_data))
    return result


def mark_related_order_expired(order_id: str | None) -> None:
    if not order_id or order_id not in orders_data:
        return
    order = orders_data[order_id]
    if order.get("status") not in {"approved", "active"}:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order["status"] = "deleted"
    order["deleted_at"] = timestamp
    order.setdefault("events", []).append({"at": timestamp, "event": "expired"})


def process_expiring_ads() -> None:
    global ads_data, orders_data
    ads_data = order_service.load_ads()
    orders_data = order_service.load_orders()
    now = datetime.now()
    changed = False

    for item_key, ad_data in list(ads_data.items()):
        if item_key in {"active_ad", "users"}:
            continue
        if not isinstance(ad_data, dict):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue

        expires_dt = parse_dt(ad_data.get("expires_at"))
        if not expires_dt:
            continue

        user_id = int(ad_data.get("user_id") or 0)
        is_main = item_key == "main_text_sale" or ad_data.get("type") == "main_text"
        keyboard = build_extend_specific_keyboard(item_key, is_main)
        title = "основная реклама" if is_main else "дополнительная реклама"
        order_code = (
            str(ad_data.get("order_code", "")).upper()
            or get_order_display_code(ad_data.get("order_id"))
            or "без кода"
        )

        remaining = expires_dt - now
        if timedelta(hours=23) <= remaining <= timedelta(days=1, hours=1) and not ad_data.get(EXPIRY_WARNING_DAY):
            safe_send_pm(
                user_id,
                f"⚠️ Напоминание: через 1 день закончится {title}.\nКод заказа: {order_code}\nПродлите её заранее.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_WARNING_DAY] = now.strftime("%Y-%m-%d %H:%M:%S")
            changed = True

        if timedelta(minutes=30) <= remaining <= timedelta(hours=1, minutes=30) and not ad_data.get(EXPIRY_WARNING_HOUR):
            safe_send_pm(
                user_id,
                f"⏰ Напоминание: через 1 час закончится {title}.\nКод заказа: {order_code}\nНажмите кнопку ниже, чтобы продлить именно это объявление.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_WARNING_HOUR] = now.strftime("%Y-%m-%d %H:%M:%S")
            changed = True

        if expires_dt <= now and not ad_data.get(EXPIRY_REMOVED_AT):
            safe_send_pm(
                user_id,
                f"❌ Срок рекламы истёк.\n{title.capitalize()} с кодом {order_code} удалена из активных объявлений.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_REMOVED_AT] = now.strftime("%Y-%m-%d %H:%M:%S")
            ad_data["status"] = "expired"
            mark_related_order_expired(ad_data.get("order_id") or (item_key if item_key in orders_data else None))
            if item_key == "main_text_sale":
                ads_data.pop("main_text_sale", None)
            else:
                ads_data.pop(item_key, None)
                user_key = str(user_id)
                if ads_data.get(user_key) is ad_data:
                    ads_data.pop(user_key, None)
                users_ads = ads_data.get("users")
                if isinstance(users_ads, dict) and users_ads.get(user_key) is ad_data:
                    users_ads.pop(user_key, None)
                active_ad = ads_data.get("active_ad")
                if active_ad is ad_data:
                    ads_data.pop("active_ad", None)
            changed = True

    if changed:
        save_orders()
        save_ads()


def expiration_monitor() -> None:
    while True:
        try:
            process_expiring_ads()
        except Exception as exc:
            logger.error(f"Ошибка обработки истечения рекламы: {exc}")
        time.sleep(60)


def render_additional_texts_list() -> str:
    additional_texts = get_additional_texts()
    additional_photos = get_additional_photos()
    if not additional_texts:
        return "Список дополнительных текстов пуст."
    lines = ["📋 Дополнительные тексты:"]
    for index, text_value in enumerate(additional_texts, start=1):
        photo_count = len(additional_photos.get(str(index - 1), []))
        preview = text_value if len(text_value) <= 160 else f"{text_value[:160]}..."
        lines.append(f"{index}. {preview}")
        lines.append(f"   Фото: {photo_count}")
    return "\n".join(lines)


def render_user_mention(user_id: int) -> str:
    try:
        user = vk.users.get(user_ids=user_id)[0]
        full_name = f"{user['first_name']} {user['last_name']}"
        return f"[id{user_id}|{full_name}]"
    except Exception:
        return f"id{user_id}"


def get_order_display_code(order_id: str, order: dict[str, Any] | None = None) -> str:
    order = order or orders_data.get(order_id) or {}
    code = order.get("order_code")
    if code:
        return str(code).upper()
    return order_id


def normalize_order_lookup_code(value: str) -> str:
    return str(value or "").upper().replace("O", "0")


def find_best_order_for_ad(ad_data: dict[str, Any]) -> str | None:
    user_id = ad_data.get("user_id")
    ad_text = (ad_data.get("text") or "").strip()
    ad_type = ad_data.get("type")
    ad_created = ad_data.get("created_at") or ""
    ad_days = int(ad_data.get("days", 0) or 0)
    candidates: list[tuple[str, dict[str, Any]]] = []
    for candidate_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        if order.get("user_id") != user_id:
            continue
        if order.get("status") not in {"approved", "pending"}:
            continue
        if ad_type == "main_text" and order.get("type") != "main_text":
            continue
        if ad_type != "main_text" and order.get("type") == "main_text":
            continue
        if (order.get("text") or "").strip() != ad_text:
            continue
        if int(order.get("days", 0) or 0) != ad_days:
            continue
        candidates.append((candidate_id, order))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[1].get("approved_at") or item[1].get("created_at") or ad_created, reverse=True)
    return candidates[0][0]


def resolve_order_reference(order_ref: str) -> str | None:
    if order_ref in orders_data:
        return order_ref
    upper_ref = order_ref.upper()
    normalized_ref = normalize_order_lookup_code(order_ref)
    for candidate_id, order in orders_data.items():
        candidate_code = str(order.get("order_code", "")).upper()
        if candidate_code == upper_ref or normalize_order_lookup_code(candidate_code) == normalized_ref:
            return candidate_id
    current_ads = order_service.load_ads()
    for ad_key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if ad_key in {"users", "active_ad"}:
            continue
        if is_user_mirror_ad_key(ad_key, ad_data):
            continue
        ad_code = str(ad_data.get("order_code", "")).upper()
        if ad_code != upper_ref and normalize_order_lookup_code(ad_code) != normalized_ref:
            continue
        related_order_id = ad_data.get("order_id")
        if related_order_id and related_order_id in orders_data:
            return related_order_id
        if ad_key in orders_data:
            return ad_key
        inferred_order_id = find_best_order_for_ad(ad_data)
        if inferred_order_id:
            ad_data["order_id"] = inferred_order_id
            ad_data["order_code"] = orders_data[inferred_order_id].get("order_code")
            ad_data["ad_key"] = ad_key
            ad_data["type"] = ad_data.get("type") or orders_data[inferred_order_id].get("type")
            try:
                order_service.save_ads(current_ads)
            except Exception as exc:
                logger.warning(f"Не удалось сохранить привязку рекламы к заказу: {exc}")
            return inferred_order_id
    return None


def sync_order_codes() -> None:
    global orders_data, ads_data
    changed_orders = False
    changed_ads = False

    for order_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        code = str(order.get("order_code", "") or "").upper()
        if not code or code.startswith("ORDER_") or code.isdigit():
            order["order_code"] = order_service.next_order_code(orders_data)
            changed_orders = True

    for key, ad_data in ads_data.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"users", "active_ad"}:
            continue
        if is_user_mirror_ad_key(key, ad_data):
            continue
        related_order_id = ad_data.get("order_id")
        if related_order_id and related_order_id in orders_data:
            desired = orders_data[related_order_id].get("order_code")
            if desired and ad_data.get("order_code") != desired:
                ad_data["order_code"] = desired
                changed_ads = True
            if ad_data.get("type") != orders_data[related_order_id].get("type"):
                ad_data["type"] = orders_data[related_order_id].get("type")
                changed_ads = True
            if ad_data.get("ad_key") != ("main_text_sale" if key == "main_text_sale" else key):
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
        elif key in orders_data:
            ad_data["order_id"] = key
            desired = orders_data[key].get("order_code")
            if desired and ad_data.get("order_code") != desired:
                ad_data["order_code"] = desired
                changed_ads = True
            if ad_data.get("type") != orders_data[key].get("type"):
                ad_data["type"] = orders_data[key].get("type")
                changed_ads = True
            if ad_data.get("ad_key") != ("main_text_sale" if key == "main_text_sale" else key):
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
        else:
            inferred_order_id = find_best_order_for_ad(ad_data)
            if inferred_order_id:
                ad_data["order_id"] = inferred_order_id
                ad_data["order_code"] = orders_data[inferred_order_id].get("order_code")
                ad_data["type"] = orders_data[inferred_order_id].get("type")
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
                continue
            code = str(ad_data.get("order_code", "") or "").upper()
            if not code or code.startswith("ORDER_") or code.isdigit():
                ad_data["order_code"] = order_service.next_order_code(orders_data)
                changed_ads = True

    if changed_orders:
        save_orders()
    if changed_ads:
        save_ads()


def save_main_photo_from_message(message: dict[str, Any]) -> tuple[bool, str]:
    global message_photo_path, uploaded_message_photo
    attachments = message.get("attachments") or []
    if not attachments:
        return False, "Прикрепите фото к сообщению с командой .редоснфото"
    attachment_ref = extract_attachment_reference(message)
    if attachment_ref and attachment_ref.startswith("photo"):
        message_photo_path = attachment_ref
        uploaded_message_photo = attachment_ref
        save_runtime_data()
        return True, "✅ Основное фото бота успешно обновлено."
    photo_attachment = None
    for item in attachments:
        if item.get("type") == "photo":
            photo_attachment = item["photo"]
            break
    if not photo_attachment:
        return False, "Поддерживается только фото."

    sizes = photo_attachment.get("sizes") or []
    if not sizes:
        return False, "Не удалось получить размеры фото."
    best = max(sizes, key=lambda x: x.get("width", 0) * x.get("height", 0))
    url = best.get("url")
    if not url:
        return False, "Не удалось получить ссылку на фото."

    photos_dir = BASE_DIR / "photos"
    photos_dir.mkdir(parents=True, exist_ok=True)
    target = photos_dir / "main_photo.jpg"
    try:
        with urlopen(url) as response:
            target.write_bytes(response.read())
    except Exception as exc:
        return False, f"Не удалось сохранить фото: {exc}"

    message_photo_path = "photos/main_photo.jpg"
    uploaded_message_photo = None
    save_runtime_data()
    return True, "✅ Основное фото бота успешно обновлено."


def broadcast_message(notify_chat_id: int | None = None) -> None:
    target_chats = [
        chat_id for chat_id in chat_ids
        if admin_chat is not None and chat_id != admin_chat and len(str(chat_id)) == 10 and str(chat_id).startswith("2")
    ]
    logger.info(f"Рассылка запущена. Целевых чатов: {len(target_chats)}")
    if not target_chats:
        if notify_chat_id:
            send_message(notify_chat_id, "⚠️ Рассылка не запущена: список чатов пуст.")
        return
    sent_count = 0
    try:
        interrupted = False
        for chat_id in target_chats:
            result = send_broadcast_to_chat(chat_id)
            if result == "access_error":
                interrupted = True
                break
            sent_count += 1
        if interrupted:
            logger.warning(f"Рассылка приостановлена. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"⚠️ Рассылка остановлена из-за ошибки доступа.\nОбработано чатов: {sent_count}")
        else:
            logger.info(f"Рассылка завершена. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"✅ Рассылка завершена.\nОбработано чатов: {sent_count}")
    except Exception as exc:
        logger.exception(f"Критическая ошибка рассылки: {exc}")
        if notify_chat_id:
            send_message(notify_chat_id, f"❌ Ошибка рассылки: {exc}")


def start_broadcast(notify_chat_id: int | None = None) -> None:
    global broadcast_thread
    with broadcast_lock:
        if broadcast_thread and broadcast_thread.is_alive():
            logger.info("Попытка повторно запустить уже активную рассылку.")
            return
        logger.info("Создаю поток рассылки.")
        broadcast_thread = threading.Thread(target=broadcast_message, kwargs={"notify_chat_id": notify_chat_id}, daemon=True)
        broadcast_thread.start()


def send_gzov_to_chat(chat_id: int) -> Any:
    logger.info(f"GZOV: отправка в чат {chat_id}")
    return send_message(
        chat_id,
        message_text,
        attachment=upload_message_photo(),
        keyboard=build_buy_ad_keyboard(),
    )


def broadcast_gzov(notify_chat_id: int | None = None) -> None:
    target_chats = [
        chat_id for chat_id in chat_ids
        if admin_chat is not None and chat_id != admin_chat and len(str(chat_id)) == 10 and str(chat_id).startswith("2")
    ]
    logger.info(f"GZOV запущен. Целевых чатов: {len(target_chats)}")
    if not target_chats:
        if notify_chat_id:
            send_message(notify_chat_id, "⚠️ GZOV не запущен: список чатов пуст.")
        return
    sent_count = 0
    try:
        interrupted = False
        for chat_id in target_chats:
            result = send_gzov_to_chat(chat_id)
            if result == "access_error":
                interrupted = True
                break
            sent_count += 1
            time.sleep(interval_sec)
        if interrupted:
            logger.warning(f"GZOV остановлен. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"⚠️ GZOV остановлен из-за ошибки доступа.\nОбработано чатов: {sent_count}")
        else:
            logger.info(f"GZOV завершён. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"✅ GZOV завершён.\nОбработано чатов: {sent_count}")
    except Exception as exc:
        logger.exception(f"Критическая ошибка GZOV: {exc}")
        if notify_chat_id:
            send_message(notify_chat_id, f"❌ Ошибка GZOV: {exc}")


def start_gzov(notify_chat_id: int | None = None) -> None:
    thread = threading.Thread(target=broadcast_gzov, kwargs={"notify_chat_id": notify_chat_id}, daemon=True)
    thread.start()


def auto_broadcast_loop() -> None:
    global last_broadcast_time
    while True:
        try:
            if time.time() - last_broadcast_time >= cd_min * 60:
                start_broadcast()
                last_broadcast_time = time.time()
        except Exception as exc:
            logger.error(f"Ошибка авторассылки по таймеру: {exc}")
        time.sleep(5)


def handle_group_info(chat_id: int) -> None:
    send_message(
        chat_id,
        "ℹ️ ИНФОРМАЦИЯ О НАС\n\nМы занимаемся продвижением каналов.\n📊 150+ чатов для рассылки\n\nНажмите кнопку ниже, чтобы перейти к покупке рекламы.",
        keyboard=build_buy_ad_keyboard(),
    )


def resolve_user_id(token: str) -> int | None:
    token = token.strip()
    mention_match = re.match(r"\[id(\d+)\|", token)
    if mention_match:
        return int(mention_match.group(1))
    direct_id = re.match(r"id(\d+)$", token, re.IGNORECASE)
    if direct_id:
        return int(direct_id.group(1))
    if token.startswith("@"):
        token = token[1:]
    try:
        user = vk.users.get(user_ids=token)
        if user:
            return int(user[0]["id"])
    except Exception as exc:
        logger.warning(f"Не удалось разрешить пользователя {token}: {exc}")
    return None


def extract_target_user(message: dict[str, Any], text: str) -> int | None:
    reply_message = message.get("reply_message")
    if isinstance(reply_message, dict) and reply_message.get("from_id"):
        return int(reply_message["from_id"])

    for raw_part in (text or "").replace("\n", " ").split():
        part = raw_part.strip(",")
        mention_match = re.match(r"\[id(\d+)\|", part)
        if mention_match:
            return int(mention_match.group(1))
        if part.startswith("@"):
            resolved = resolve_user_id(part[1:])
            if resolved:
                return resolved
        direct_id = re.match(r"id(\d+)$", part, re.IGNORECASE)
        if direct_id:
            return int(direct_id.group(1))
        if "vk.com/" in part or "vk.ru/" in part:
            tail = part.rstrip("/").split("/")[-1]
            resolved = resolve_user_id(tail)
            if resolved:
                return resolved
    return None


def create_direct_order(user_id: int, days: int, text_value: str, photo: str | None, admin_user_id: int) -> tuple[str, dict[str, Any]]:
    global ads_data
    order_id = order_service.next_order_id(orders_data)
    order_no = order_service.next_order_number(orders_data)
    order_code = order_service.next_order_code(orders_data)
    now = datetime.now()
    expires_at = (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    order = {
        "order_no": order_no,
        "order_code": order_code,
        "type": "new_ad",
        "status": "approved",
        "user_id": user_id,
        "text": text_value,
        "photo": photo,
        "price": 0,
        "days": days,
        "rate_name": f"{days} дн.",
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "approved_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "approved_by": admin_user_id,
        "events": [
            {"at": now.strftime("%Y-%m-%d %H:%M:%S"), "event": "created_by_admin"},
            {"at": now.strftime("%Y-%m-%d %H:%M:%S"), "event": "approved"},
        ],
    }
    orders_data[order_id] = order
    ad_data = {
        "ad_key": order_id,
        "user_id": user_id,
        "text": text_value,
        "photo": photo,
        "price": 0,
        "days": days,
        "rate_name": f"{days} дн.",
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": expires_at,
        "status": "approved",
    }
    ads_data[order_id] = ad_data
    save_orders()
    save_ads()
    return order_id, ad_data


def build_delete_order_keyboard(target_user_id: int, requester_id: int) -> str | None:
    keyboard = VkKeyboard(inline=True)
    has_items = False

    user_ads = order_service.get_user_active_ads(ads_data, target_user_id)
    for ad_data in user_ads:
        item_key = ad_data.get("ad_key") or next((key for key, value in ads_data.items() if value is ad_data), "extra_active")
        preview = (ad_data.get("text") or "Активная рассылка").strip()
        label = preview[:30] + ("..." if len(preview) > 30 else "")
        keyboard.add_callback_button(
            f"🗑 Рассылка: {label}",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "delete_user_order", "target_user_id": target_user_id, "requester_id": requester_id, "item_key": item_key},
        )
        keyboard.add_line()
        has_items = True

    main_sale = ads_data.get("main_text_sale")
    if isinstance(main_sale, dict) and main_sale.get("user_id") == target_user_id:
        preview = (main_sale.get("text") or "Основная реклама").strip()
        label = preview[:30] + ("..." if len(preview) > 30 else "")
        keyboard.add_callback_button(
            f"🗑 Осн. реклама: {label}",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "delete_user_order", "target_user_id": target_user_id, "requester_id": requester_id, "item_key": "main_text_sale"},
        )
        has_items = True

    return keyboard.get_keyboard() if has_items else None


def build_single_order_keyboard(order_id: str, order: dict[str, Any], requester_id: int, has_check: bool = False, message_id: int | None = None) -> str:
    keyboard = VkKeyboard(inline=True)
    item_key = "main_text_sale" if order.get("type") == "main_text" else (order.get("ad_key") or order_id)
    keyboard.add_callback_button(
        "✏️ Редактировать текст",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_order_text", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_callback_button(
        "🖼 Удалить фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "remove_order_photo", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📎 Добавить фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "add_order_photo", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "⏳ Изменить срок",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_order_days", "order_id": order_id, "requester_id": requester_id},
    )
    if has_permission(requester_id, "dev"):
        keyboard.add_callback_button(
            "🗑 Удалить рекламу",
            color=VkKeyboardColor.NEGATIVE,
            payload={
                "command": "delete_user_order",
                "order_id": order_id,
                "target_user_id": int(order.get("user_id") or 0),
                "requester_id": requester_id,
                "item_key": item_key,
            },
        )
    if has_check:
        keyboard.add_line()
        keyboard.add_callback_button(
            "📎 Показать чек",
            color=VkKeyboardColor.SECONDARY,
            payload={"command": "show_check_order", "order_id": order_id, "requester_id": requester_id},
        )
    if message_id:
        keyboard.add_line()
        keyboard.add_callback_button(
            "❌ Выйти",
            color=VkKeyboardColor.SECONDARY,
            payload={"command": "exit_order_view", "message_id": message_id, "requester_id": requester_id},
        )
    return keyboard.get_keyboard()


def delete_direct_order(user_id: int, item_key: str | None = None) -> bool:
    global ads_data
    removed = False
    user_key = str(user_id)

    if item_key in {None, "extra_active"} or (item_key and item_key != "main_text_sale"):
        if item_key and item_key not in {None, "extra_active", "main_text_sale"}:
            ad_data = ads_data.get(item_key)
            if isinstance(ad_data, dict) and ad_data.get("user_id") == user_id:
                ads_data.pop(item_key, None)
                removed = True
                if isinstance(ads_data.get(user_key), dict) and ads_data[user_key].get("order_id") == ad_data.get("order_id"):
                    ads_data.pop(user_key, None)
                    removed = True
                users_ads = ads_data.get("users")
                if isinstance(users_ads, dict):
                    user_ad = users_ads.get(user_key)
                    if isinstance(user_ad, dict) and user_ad.get("order_id") == ad_data.get("order_id"):
                        users_ads.pop(user_key, None)
                        removed = True
                active_ad = ads_data.get("active_ad")
                if isinstance(active_ad, dict) and active_ad.get("order_id") == ad_data.get("order_id"):
                    ads_data.pop("active_ad", None)
                    removed = True
        else:
            if user_key in ads_data:
                ads_data.pop(user_key, None)
                removed = True
            users_ads = ads_data.get("users")
            if isinstance(users_ads, dict):
                if users_ads.pop(user_key, None) is not None:
                    removed = True
            active_ad = ads_data.get("active_ad")
            if isinstance(active_ad, dict) and active_ad.get("user_id") == user_id:
                ads_data.pop("active_ad", None)
                removed = True
        candidate_orders = [
            (order_id, order)
            for order_id, order in orders_data.items()
            if order.get("user_id") == user_id and order.get("status") == "approved" and order.get("type") in {"new_ad", "update_ad", "extend_ad", "renew_ad"}
        ]
        if item_key and item_key not in {None, "extra_active", "main_text_sale"}:
            candidate_orders = [(order_id, order) for order_id, order in candidate_orders if order_id == item_key]
        if candidate_orders:
            candidate_orders.sort(key=lambda pair: pair[1].get("approved_at") or pair[1].get("created_at") or "")
            order = candidate_orders[-1][1]
            order["status"] = "deleted"
            order["deleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order.setdefault("events", []).append({"at": order["deleted_at"], "event": "deleted"})
            removed = True

    if item_key in {None, "main_text_sale"}:
        main_sale = ads_data.get("main_text_sale")
        if isinstance(main_sale, dict) and main_sale.get("user_id") == user_id:
            ads_data.pop("main_text_sale", None)
            removed = True
        main_orders = [
            order
            for order in orders_data.values()
            if order.get("user_id") == user_id and order.get("status") == "approved" and order.get("type") == "main_text"
        ]
        if main_orders:
            main_orders.sort(key=lambda item: item.get("approved_at") or item.get("created_at") or "")
            order = main_orders[-1]
            order["status"] = "deleted"
            order["deleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order.setdefault("events", []).append({"at": order["deleted_at"], "event": "deleted"})
            removed = True

    if removed:
        save_orders()
        save_ads()
    return removed


def get_order_item_key(order_id: str, order: dict[str, Any] | None) -> str:
    if not order:
        return order_id
    if order.get("type") == "main_text":
        return "main_text_sale"
    return str(order.get("ad_key") or order_id)


def get_ads_record_for_order(order_id: str, order: dict[str, Any] | None) -> dict[str, Any] | None:
    if not order:
        return None
    item_key = get_order_item_key(order_id, order)
    ad_data = ads_data.get(item_key)
    return ad_data if isinstance(ad_data, dict) else None


def update_order_and_ad(order_id: str, updater) -> bool:
    order = orders_data.get(order_id)
    if not order:
        return False
    updater(order)
    order["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order.setdefault("events", []).append({"at": order["updated_at"], "event": "updated"})

    ad_data = get_ads_record_for_order(order_id, order)
    if ad_data is not None:
        updater(ad_data)
        ad_data["updated_at"] = order["updated_at"]
    save_orders()
    save_ads()
    return True


def apply_order_text_change(order_id: str, text_value: str) -> bool:
    clean_text = text_value.strip()
    if not clean_text:
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("text", clean_text))


def apply_order_days_change(order_id: str, days: int) -> bool:
    if days <= 0:
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("days", days))


def apply_order_photo_remove(order_id: str) -> bool:
    order = orders_data.get(order_id)
    if not order:
        return False

    def clear_photo(data: dict[str, Any]) -> None:
        data["photo"] = None
        if "attachment" in data:
            data["attachment"] = None

    return update_order_and_ad(order_id, clear_photo)


def apply_order_photo_add(order_id: str, attachment_ref: str) -> bool:
    if not attachment_ref:
        return False

    def merge_photo(data: dict[str, Any]) -> None:
        current_parts = [part.strip() for part in str(data.get("photo") or "").split(",") if part.strip()]
        if attachment_ref not in current_parts:
            current_parts.append(attachment_ref)
        data["photo"] = ",".join(current_parts) if current_parts else attachment_ref

    return update_order_and_ad(order_id, merge_photo)


def handle_admin_order_edit_input(chat_id: int, user_id: int, text: str, message: dict[str, Any] | None = None) -> bool:
    state = USER_STATES.get(str(user_id)) or {}
    pending = state.get("admin_order_edit")
    if not isinstance(pending, dict):
        return False
    if pending.get("chat_id") != chat_id:
        return False

    order_id = str(pending.get("order_id") or "")
    mode = pending.get("mode")
    attachments = (message or {}).get("attachments") or []
    if order_id not in orders_data:
        state.pop("admin_order_edit", None)
        send_message(chat_id, "❌ Заказ не найден.")
        return True

    if mode == "text":
        if not apply_order_text_change(order_id, text):
            send_message(chat_id, "❌ Отправьте новый текст для заказа обычным сообщением.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Текст заказа {get_order_display_code(order_id)} обновлён.")
        return True

    if mode == "days":
        try:
            days = int(text.strip())
        except ValueError:
            send_message(chat_id, "❌ Отправьте срок числом, например: 30")
            return True
        if not apply_order_days_change(order_id, days):
            send_message(chat_id, "❌ Срок должен быть положительным числом.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Срок заказа {get_order_display_code(order_id)} обновлён: {days} дн.")
        return True

    if mode == "photo":
        if not attachments:
            send_message(chat_id, "❌ Отправьте фото следующим сообщением.")
            return True
        raw = attachments[0]
        if raw.get("type") != "photo":
            send_message(chat_id, "❌ Нужно отправить именно фото.")
            return True
        photo = raw.get("photo") or {}
        owner_id = photo.get("owner_id")
        photo_id = photo.get("id")
        if not owner_id or not photo_id:
            send_message(chat_id, "❌ Не удалось получить вложение фото.")
            return True
        attachment_ref = f"photo{owner_id}_{photo_id}"
        if photo.get("access_key"):
            attachment_ref = f"{attachment_ref}_{photo['access_key']}"
        if not apply_order_photo_add(order_id, attachment_ref):
            send_message(chat_id, "❌ Не удалось добавить фото к заказу.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Фото добавлено к заказу {get_order_display_code(order_id)}.")
        return True

    state.pop("admin_order_edit", None)
    return False


def delete_chat_message(peer_id: int, message_id: int) -> None:
    try:
        vk.messages.delete(message_ids=message_id, delete_for_all=1)
    except Exception:
        try:
            vk.messages.delete(peer_id=peer_id, cmids=message_id, delete_for_all=1)
        except Exception as exc:
            logger.warning(f"Не удалось удалить сообщение {message_id} в чате {peer_id}: {exc}")


def delete_chat_message_by_cmid(peer_id: int, cmid: int) -> None:
    try:
        vk.messages.delete(peer_id=peer_id, cmids=cmid, delete_for_all=1)
    except Exception as exc:
        logger.warning(f"Не удалось удалить сообщение с conversation_message_id={cmid} в чате {peer_id}: {exc}")


def handle_admin_text_command(chat_id: int, user_id: int, text: str, message: dict[str, Any]) -> bool:
    global ads_data
    if not has_permission(user_id, "admin"):
        return False

    if text.startswith(".добзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        parts = text.split(" ", 3)
        if len(parts) < 4:
            send_message(chat_id, "Использование: .добзаказ @username [дни] [текст]")
            return True
        target_token = parts[1]
        try:
            days = int(parts[2])
        except ValueError:
            send_message(chat_id, "Дни должны быть числом.")
            return True
        text_value = parts[3].strip()
        if not text_value:
            send_message(chat_id, "Укажите текст заказа.")
            return True
        target_user_id = resolve_user_id(target_token)
        if not target_user_id:
            send_message(chat_id, "Не удалось определить пользователя.")
            return True
        photo = extract_attachment_reference(message)
        order_id, ad_data = create_direct_order(target_user_id, days, text_value, photo, user_id)
        safe_send_pm(target_user_id, f"✅ Ваш заказ добавлен администратором.\nРассылка активирована до: {ad_data['expires_at']}")
        send_message(chat_id, f"✅ Заказ {get_order_display_code(order_id)} добавлен для id{target_user_id}.")
        return True

    if text.startswith(".удзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        parts = text.split(" ", 1)
        if len(parts) < 2:
            send_message(chat_id, "Использование: .удзаказ @username")
            return True
        target_user_id = resolve_user_id(parts[1].strip())
        if not target_user_id:
            send_message(chat_id, "Не удалось определить пользователя.")
            return True
        keyboard = build_delete_order_keyboard(target_user_id, user_id)
        if not keyboard:
            send_message(chat_id, "Активный заказ для этого пользователя не найден.")
            return True
        send_message(chat_id, f"Выберите рекламу пользователя {render_user_mention(target_user_id)}, которую нужно удалить:", keyboard=keyboard)
        return True

    if text == ".заказ" or text.startswith(".заказ "):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_message(chat_id, "❌ Формат: .заказ [order_code]")
            return True
        order_ref = parts[1].strip()
        resolved = resolve_order_reference(order_ref)
        if not resolved or resolved not in orders_data:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        order = orders_data[resolved]
        user_label = render_user_mention(int(order.get("user_id"))) if order.get("user_id") else "неизвестно"
        has_check = bool(order.get("attachment"))
        message_id = send_message(chat_id, render_order_details(resolved, order, user_label), attachment=attachment_reference_from_order(order))
        send_message(
            chat_id,
            "Действия по заказу:",
            keyboard=build_single_order_keyboard(
                resolved,
                order,
                user_id,
                has_check=has_check,
                message_id=message_id if isinstance(message_id, int) else None,
            ),
        )
        return True

    if text.startswith(".измзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        parts = text.split(maxsplit=3)
        if len(parts) < 4:
            send_message(chat_id, "❌ Формат: .измзаказ <order_id|номер> <поле> <значение>")
            return True
        order_ref = parts[1].strip()
        field = parts[2].strip()
        raw_value = parts[3].strip()
        resolved = None
        if order_ref in orders_data:
            resolved = order_ref
        else:
            for candidate_id, order in orders_data.items():
                if str(order.get("order_no")) == order_ref or str(order.get("order_code", "")).upper() == order_ref.upper():
                    resolved = candidate_id
                    break
        if not resolved or resolved not in orders_data:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        if field not in {"text", "price", "days", "rate_name", "status", "type", "photo"}:
            send_message(chat_id, "❌ Нельзя менять это поле.")
            return True
        value: Any = raw_value
        if field in {"price", "days"}:
            try:
                value = int(raw_value)
            except ValueError:
                send_message(chat_id, "❌ Для price/days нужно число.")
                return True
        orders_data[resolved][field] = value
        orders_data[resolved]["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_orders()
        send_message(chat_id, f"✅ Заказ {resolved} обновлён: {field} = {value}")
        return True

    return False


def handle_admin_order_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    global ads_data
    command = payload.get("command")
    order_id = payload.get("order_id")
    if command not in {"view_order", "approve_order", "reject_order", "delete_user_order", "show_check_order", "support_open_chat", "support_close", "exit_order_view", "edit_order_text", "remove_order_photo", "add_order_photo", "edit_order_days", "history_page"}:
        return False
    if not has_permission(user_id, "admin"):
        send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
        return True
    if command == "support_open_chat":
        target_user_id = int(payload.get("user_id") or 0)
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить покупателя.")
            return True
        safe_send_pm(target_user_id, "✅ Администратор с вами! Объясните ситуацию.")
        USER_STATES.setdefault(str(target_user_id), {"menu_sent": True})["support_mode"] = True
        try:
            event_object = payload.get("_event_object")
            if isinstance(event_object, dict):
                answer_callback_event(
                    event_object,
                    link=f"https://vk.com/gim{group_id}?sel={target_user_id}",
                )
        except Exception as exc:
            logger.warning(f"Не удалось открыть чат поддержки: {exc}")
        send_message(chat_id, f"✅ Открываю чат с {render_user_mention(target_user_id)} и отправляю уведомление пользователю.")
        return True
    if command == "support_close":
        target_user_id = int(payload.get("user_id") or 0)
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить покупателя.")
            return True
        state = USER_STATES.setdefault(str(target_user_id), {"menu_sent": True})
        state.pop("support_mode", None)
        state.pop("flow", None)
        safe_send_pm(target_user_id, "✅ Обращение закрыто. Бот снова доступен.")
        event_object = payload.get("_event_object")
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Обращение закрыто.")
        send_message(chat_id, f"✅ Вопрос пользователя {render_user_mention(target_user_id)} закрыт. Бот снова отвечает в ЛС.")
        return True
    if command == "delete_user_order":
        requester_id = int(payload.get("requester_id") or 0)
        target_user_id = int(payload.get("target_user_id") or 0)
        item_key = payload.get("item_key")
        if requester_id != user_id:
            send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
            return True
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Удаление рекламы доступно только разработчику.")
            return True
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить пользователя для удаления.")
            return True
        if delete_direct_order(target_user_id, item_key):
            safe_send_pm(target_user_id, "❌ Ваш активный заказ был удалён администратором.")
            send_message(chat_id, f"✅ Выбранная реклама пользователя {render_user_mention(target_user_id)} удалена.")
        else:
            send_message(chat_id, "Активный заказ для этого пользователя не найден.")
        return True
    if command == "exit_order_view":
        requester_id = int(payload.get("requester_id") or 0)
        if requester_id and requester_id != user_id:
            send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
            return True
        target_message_id = int(payload.get("message_id") or 0)
        if target_message_id:
            delete_chat_message(chat_id, target_message_id)
        event_object = payload.get("_event_object")
        if isinstance(event_object, dict):
            cmid = int(event_object.get("conversation_message_id") or 0)
            if cmid:
                delete_chat_message_by_cmid(chat_id, cmid)
        return True
    if command == "history_page":
        page = int(payload.get("page") or 0)
        history_keyboard = build_orders_history_keyboard(orders_data, page=page)
        send_message(chat_id, render_orders_history_text(orders_data, page=page), keyboard=history_keyboard)
        return True
    order = orders_data.get(order_id)
    user_label = render_user_mention(int(order.get("user_id"))) if order and order.get("user_id") else "неизвестно"
    requester_id = int(payload.get("requester_id") or 0)
    if command in {"edit_order_text", "remove_order_photo", "add_order_photo", "edit_order_days", "show_check_order"} and requester_id and requester_id != user_id:
        send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
        return True
    if command == "view_order":
        readonly = bool(payload.get("readonly"))
        history_page = int(payload.get("history_page") or 0)
        send_message(
            chat_id,
            render_order_details(order_id, order, user_label),
            keyboard=build_order_action_keyboard(
                order_id,
                order.get("status") if order else None,
                has_check=bool(order and order.get("attachment")),
                readonly=readonly,
                history_page=history_page,
            ),
            attachment=attachment_reference_from_order(order),
        )
        return True
    if command == "edit_order_text":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "text", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"✏️ Отправьте новый текст для заказа {get_order_display_code(order_id)} следующим сообщением.")
        return True
    if command == "remove_order_photo":
        if not order:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        if apply_order_photo_remove(order_id):
            send_message(chat_id, f"✅ Фото у заказа {get_order_display_code(order_id)} удалено.")
        else:
            send_message(chat_id, "❌ Не удалось удалить фото.")
        return True
    if command == "add_order_photo":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "photo", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"📎 Отправьте новое фото для заказа {get_order_display_code(order_id)} следующим сообщением.")
        return True
    if command == "edit_order_days":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "days", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"⏳ Отправьте новый срок в днях для заказа {get_order_display_code(order_id)}.")
        return True
    if command == "show_check_order":
        attachment = check_attachment_reference_from_order(order)
        if not attachment:
            send_message(chat_id, "❌ У этой заявки нет прикреплённого чека.")
            return True
        send_message(chat_id, f"📎 Чек по заявке {order_id}", attachment=attachment)
        return True
    if command == "approve_order" and order:
        _, ads_data, _ = order_service.approve_order(orders_data, ads_data, order_id)
        save_orders()
        save_ads()
        append_income(order["user_id"], int(order.get("price", 0) or 0), f"{order.get('type')} - {order.get('rate_name')}")
        safe_send_pm(order["user_id"], "✅ Ваш заказ одобрен!\nРассылка активирована.")
        send_message(chat_id, render_order_result(order_id, order, user_label))
        return True
    if command == "reject_order" and order:
        order_service.reject_order(orders_data, order_id)
        save_orders()
        safe_send_pm(order["user_id"], "❌ Ваша заявка отклонена.")
        send_message(chat_id, render_order_result(order_id, order, user_label))
        return True
    return True


def main() -> None:
    global ads_data, orders_data, admin_chat, message_text, cd_min, interval_sec, expiration_thread, auto_broadcast_thread, last_broadcast_time
    logger.info("Бот запущен")
    last_broadcast_time = time.time()
    sync_order_codes()
    process_expiring_ads()
    if expiration_thread is None or not expiration_thread.is_alive():
        expiration_thread = threading.Thread(target=expiration_monitor, daemon=True)
        expiration_thread.start()
    if auto_broadcast_thread is None or not auto_broadcast_thread.is_alive():
        auto_broadcast_thread = threading.Thread(target=auto_broadcast_loop, daemon=True)
        auto_broadcast_thread.start()

    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_EVENT:
            event_object = event.object
            payload = event_object.get("payload") or {}
            proxy_message = {
                "peer_id": event_object.get("peer_id"),
                "from_id": event_object.get("user_id"),
                "text": "",
                "payload": payload,
                "attachments": [],
                "_event_object": event_object,
            }
            event = type("CallbackEventProxy", (), {"obj": type("CallbackObj", (), {"message": proxy_message})()})()
        elif event.type != VkBotEventType.MESSAGE_NEW:
            continue

        message = event.obj.message
        chat_id = message.get("peer_id")
        user_id = message.get("from_id")
        text = (message.get("text") or "").strip()
        payload = parse_message_payload(message, logger=logger)
        command = (payload.get("command") or text).strip().lower()
        is_personal_chat = chat_id == user_id

        ads_data = order_service.load_ads()
        orders_data = order_service.load_orders()

        if chat_id != admin_chat and not is_personal_chat and not has_permission(user_id, "admin"):
            if not is_subscribed(user_id):
                send_message(chat_id, f"⚠️ Подпишитесь на нашу группу для использования команд:\n{SUBSCRIPTION_CHANNEL_LINK}")
                continue

        if is_personal_chat:
            handle_personal_message(vk, event, user_id, ads_data, orders_data, runtime_data, order_service, save_ads, save_orders, logger=logger)
            continue

        if handle_admin_order_action(chat_id, user_id, payload):
            continue

        if handle_admin_order_edit_input(chat_id, user_id, text, message):
            continue

        if handle_admin_text_command(chat_id, user_id, text, message):
            continue

        if text == ".пинг":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            start_time = time.time()
            send_message(chat_id, "Проверка пинга...")
            end_time = time.time()
            ping_time = int((end_time - start_time) * 1000)
            send_message(chat_id, f"Пинг: {ping_time}ms")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".стата"):
            target_user_id = extract_target_user(message, text) or user_id
            send_message(chat_id, render_user_stats_detailed(target_user_id))
            update_user_stats(user_id, "command")
            continue

        if text == ".хелп":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            role = get_role(user_id)
            send_message(chat_id, get_help_text(role))
            update_user_stats(user_id, "command")
            continue

        if text == ".список" and chat_id == admin_chat and has_permission(user_id, "admin"):
            total_chats = len(chat_ids)
            chat_list = "\n".join(str(cid) for cid in chat_ids if cid != admin_chat)
            send_message(chat_id, f"Количество чатов для рассылки: {total_chats}\nСписок чатов:\n{chat_list}")
            update_user_stats(user_id, "command")
            continue

        if text == ".ид" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, f"✅ ID этой беседы: {chat_id}")
            update_user_stats(user_id, "command")
            continue

        if text == ".инфо" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, render_runtime_info_legacy())
            update_user_stats(user_id, "command")
            continue

        if text == ".допсписок" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, render_additional_texts_list())
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".редтекст") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(" ", 2)
            if len(parts) < 3:
                send_message(chat_id, "Использование: .редтекст [номер] [текст]")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть положительным числом.")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            additional_texts[index] = parts[2].strip()
            save_additional_texts(additional_texts)
            send_message(chat_id, f"✅ Дополнительный текст №{text_number} обновлён.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".добфото"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2:
                send_message(chat_id, "Укажите номер текста. Пример: .добфото 1")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть числом. Пример: .добфото 1")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            attachments = extract_attachment_references(message)
            if not attachments:
                send_message(chat_id, "❌ Прикрепите фото к сообщению с командой .добфото")
                continue
            additional_photos = get_additional_photos()
            additional_photos.setdefault(str(index), [])
            additional_photos[str(index)].extend(attachments)
            save_additional_texts(additional_texts, additional_photos)
            send_message(chat_id, f"✅ Фото успешно добавлены к доп. тексту №{text_number}", attachment=",".join(attachments))
            update_user_stats(user_id, "osn_photo")
            continue

        if text.startswith(".удфото"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2:
                send_message(chat_id, "Укажите номер текста. Пример: .удфото 1")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть числом. Пример: .удфото 1")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            additional_photos = get_additional_photos()
            if str(index) in additional_photos:
                del additional_photos[str(index)]
                save_additional_texts(additional_texts, additional_photos)
                send_message(chat_id, f"🗑️ Все фото для доп. текста №{text_number} удалены")
            else:
                send_message(chat_id, f"ℹ️ У доп. текста №{text_number} не было прикреплённых фото")
            update_user_stats(user_id, "command")
            continue

        if text == ".списокрекламы" and chat_id == admin_chat and has_permission(user_id, "admin"):
            active_ads = iter_active_ads()
            if not active_ads:
                send_message(chat_id, "Активных заказов рекламы сейчас нет.")
                continue
            for index, (ad_key, ad_data) in enumerate(active_ads, start=1):
                user_label = render_user_mention(int(ad_data.get("user_id"))) if ad_data.get("user_id") else ad_key
                expires_at = ad_data.get("expires_at", "не указано")
                text_value = ad_data.get("text", "") or "Текст не указан"
                preview = text_value if len(text_value) <= 700 else f"{text_value[:700]}..."
                related_order_id = ad_data.get("order_id") or (ad_key if ad_key in orders_data else None)
                order_code = (
                    str(ad_data.get("order_code", "") or "").upper()
                    or (get_order_display_code(related_order_id) if related_order_id else "")
                    or "НЕИЗВЕСТНО"
                )
                ad_type = "Основное" if ad_key == "main_text_sale" or ad_data.get("type") == "main_text" else "Дополнительное"
                send_message(
                    chat_id,
                    f"📢 Реклама #{index}\nКод: {order_code}\nТип: {ad_type}\nПользователь: {user_label}\nАктивна до: {expires_at}\n\n{preview}",
                    attachment=ad_data.get("photo"),
                )
            send_message(chat_id, f"Всего активных реклам: {len(active_ads)}")
            update_user_stats(user_id, "command")
            continue

        if text == ".уст" and has_permission(user_id, "admin"):
            if admin_chat is None:
                send_message(chat_id, "Администратор не установлен.")
                continue
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только из административного чата.")
                continue
            if len(str(chat_id)) == 10 and str(chat_id).startswith("2"):
                if chat_id not in chat_ids:
                    chat_ids.append(chat_id)
                    save_runtime_data()
                    send_message(chat_id, "Этот чат добавлен в список для рассылки сообщений.")
                else:
                    send_message(chat_id, "❌ Этот чат уже в списке рассылки.")
            else:
                send_message(chat_id, "Невозможно добавить этот чат: это не беседа.")
            update_user_stats(user_id, "command")
            continue

        if text == ".инфочат" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(
                chat_id,
                "📋 Информация о чате:\n"
                "\n"
                f"🔹 ID чата: {chat_id}\n"
                f"🔹 ID отправителя: {user_id}\n"
                f"🔹 Режим Long Poll: {'Включён' if longpoll_enabled else 'Отключён или не настроен'}\n"
                f"🔹 Текущий административный чат: {admin_chat if admin_chat else 'Не установлен'}\n"
                "▫️ Версия бота: 2.0\n",
            )
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".добид") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_message(chat_id, "Использование: .добид [число]")
                continue
            try:
                count = int(parts[1])
                if count <= 0:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Количество должно быть положительным числом.")
                continue
            bot_chat_ids = [cid for cid in chat_ids if str(cid).startswith("2") and len(str(cid)) == 10]
            next_chat_id = max(bot_chat_ids, default=2000000000) + 1
            for _ in range(count):
                chat_ids.append(next_chat_id)
                next_chat_id += 1
            save_runtime_data()
            send_message(chat_id, f"✅ Добавлено {count} чатов в список.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".делид") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_message(chat_id, "Использование: .делид [число]")
                continue
            try:
                count = int(parts[1])
                if count <= 0:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Количество должно быть положительным числом.")
                continue
            bot_chat_ids = [cid for cid in chat_ids if str(cid).startswith("2") and len(str(cid)) == 10]
            removed = 0
            for _ in range(min(count, len(bot_chat_ids))):
                if bot_chat_ids:
                    chat_to_remove = bot_chat_ids.pop()
                    if chat_to_remove in chat_ids:
                        chat_ids.remove(chat_to_remove)
                        removed += 1
            if removed:
                save_runtime_data()
            send_message(chat_id, f"✅ Удалено {removed} чатов с конца списка.")
            update_user_stats(user_id, "command")
            continue

        if text == ".доходы" and has_permission(user_id, "dev"):
            send_message(chat_id, render_income_stats())
            update_user_stats(user_id, "command")
            continue

        if text == ".тест" and has_permission(user_id, "admin"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            send_broadcast_to_chat(chat_id)
            update_user_stats(user_id, "command")
            continue

        if text == ".рассылка" and has_permission(user_id, "admin"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            start_broadcast(notify_chat_id=chat_id)
            last_broadcast_time = time.time()
            send_message(chat_id, "✅ Рассылка запущена и таймер сброшен.")
            update_user_stats(user_id, "command")
            continue

        if text == ".gzov" and has_permission(user_id, "dev"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            start_gzov(notify_chat_id=chat_id)
            send_message(chat_id, "✅ GZOV запущен.")
            update_user_stats(user_id, "command")
            continue

        if text == ".заказы" and has_permission(user_id, "admin"):
            orders_keyboard = build_orders_list_keyboard(orders_data)
            send_message(chat_id, render_orders_list_text(orders_data), keyboard=orders_keyboard)
            continue

        if text == ".историязаказов" and has_permission(user_id, "admin"):
            history_keyboard = build_orders_history_keyboard(orders_data, page=0)
            send_message(chat_id, render_orders_history_text(orders_data, page=0), keyboard=history_keyboard)
            continue

        if text == ".инфо_о_нас" or payload.get("command") == ".инфо_о_нас":
            handle_group_info(chat_id)
            continue

        if command == "buy_main_text_dm":
            safe_send_pm(user_id, "✉️ Вы перешли в личные сообщения бота.\n\nДля покупки рекламы используйте команду Начать.")
            continue

        if text == ".меню":
            active_ad = order_service.get_user_active_ad(ads_data, user_id)
            has_messages = any(order.get("user_id") == user_id and order.get("type") != "main_text" for order in orders_data.values())
            send_message(
                chat_id,
                "Главное меню",
                keyboard=generate_main_menu(has_active_ad=bool(active_ad), has_messages=has_messages or bool(active_ad)),
            )
            continue

        if text == ".админчат":
            if not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            if admin_chat == chat_id:
                send_message(chat_id, "⚠️ Этот чат уже является административным.")
            else:
                admin_chat = chat_id
                save_runtime_data()
                send_message(chat_id, "Административный чат установлен.")
            continue

        if text == ".стафф":
            if user_id == OWNER_ID:
                send_message(chat_id, render_staff_detailed())
            else:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
            continue

        if text.startswith(".редоснтекст") and has_permission(user_id, "dev"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Неверный формат команды. Используйте: .редоснтекст [текст]")
                continue
            message_text = parts[1].strip()
            save_runtime_data()
            send_message(chat_id, message_text, attachment=upload_message_photo())
            update_user_stats(user_id, "osn_text")
            continue

        if text.startswith(".настройки") and chat_id == admin_chat and has_permission(user_id, "dev"):
            parts = text.split()
            if len(parts) != 3:
                send_message(chat_id, "❌ Неверный формат. Используйте: .настройки [cd_min|interval_sec] [число]")
                continue
            key = parts[1]
            try:
                value = float(parts[2])
            except ValueError:
                send_message(chat_id, "❌ Значение должно быть числом.")
                continue
            if key == "cd_min":
                if value < 1 or value > 1440:
                    send_message(chat_id, "❌ КД должно быть от 1 до 1440 минут.")
                    continue
                cd_min = int(value)
                save_config_value("cd_min", str(cd_min))
                send_message(chat_id, f"✅ Установлено: cd_min = {cd_min} мин")
            elif key == "interval_sec":
                if value < 0 or value > 60:
                    send_message(chat_id, "❌ Интервал должен быть от 0 до 60 секунд.")
                    continue
                interval_sec = float(value)
                save_config_value("interval_sec", str(interval_sec))
                send_message(chat_id, f"✅ Установлено: interval_sec = {interval_sec} сек")
            else:
                send_message(chat_id, "❌ Доступные ключи: cd_min, interval_sec")
                continue
            update_user_stats(user_id, "command")
            continue

        if text == ".редоснфото":
            if user_id == OWNER_ID:
                if chat_id != admin_chat:
                    send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                    continue
                ok, result_text = save_main_photo_from_message(message)
                send_message(chat_id, result_text if ok else ("❌ Прикрепите именно фото." if "Поддерживается только фото" in result_text else result_text))
                if ok:
                    update_user_stats(user_id, "osn_photo")
            else:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
            continue

        if text.startswith(".админ") and has_permission(user_id, "dev"):
            target_user_id = extract_target_user(message, text)
            if not target_user_id:
                send_message(chat_id, "❌ Укажите пользователя: ответом, @ или ссылкой.")
                continue
            if user_id == target_user_id:
                send_message(chat_id, "❌ Нельзя снимать права администратора с себя через эту команду.")
                continue
            users = load_users()
            user_entry = users.setdefault(
                str(target_user_id),
                {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""},
            )
            current_role = user_entry.get("role")
            if current_role == "admin":
                user_entry["role"] = "user"
                save_users(users)
                send_message(chat_id, f"❌ Права администратора сняты у пользователя {target_user_id}.")
            else:
                user_entry["role"] = "admin"
                save_users(users)
                send_message(chat_id, f"✅ Пользователь {target_user_id} назначен администратором.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".разраб"):
            if user_id != OWNER_ID:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            target_user_id = extract_target_user(message, text)
            if not target_user_id:
                send_message(chat_id, "❌ Укажите пользователя: ответом, @ или ссылкой.")
                continue
            users = load_users()
            user_key = str(target_user_id)
            current_role = users.get(user_key, {}).get("role", "user")
            if current_role == "dev":
                users.setdefault(user_key, {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""})
                users[user_key]["role"] = "user"
                save_users(users)
                try:
                    user_info = vk.users.get(user_ids=target_user_id)[0]
                    full_name = f"{user_info['first_name']} {user_info['last_name']}"
                    send_message(chat_id, f"❌ Права разработчика сняты у [id{target_user_id}|{full_name}]")
                except Exception as e:
                    send_message(chat_id, f"❌ Права разработчика сняты у пользователя {target_user_id}. Произошла ошибка при получении имени: {e}")
            else:
                users.setdefault(user_key, {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""})
                users[user_key]["role"] = "dev"
                save_users(users)
                try:
                    user_info = vk.users.get(user_ids=target_user_id)[0]
                    full_name = f"{user_info['first_name']} {user_info['last_name']}"
                    send_message(chat_id, f"✅ [id{target_user_id}|{full_name}] назначен(а) разработчиком.")
                except Exception as e:
                    send_message(chat_id, f"✅ Пользователь {target_user_id} назначен разработчиком. Произошла ошибка при получении имени: {e}")
            continue

if __name__ == "__main__":
    main()
