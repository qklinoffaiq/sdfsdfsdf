from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from config import MAIN_TEXT_RATES


def generate_main_menu(has_active_ad: bool = False, has_messages: bool = False):
    keyboard = VkKeyboard(inline=True)

    if has_messages:
        keyboard.add_callback_button("📨 Управление рекламой", color=VkKeyboardColor.PRIMARY, payload={"command": ".сообщение"})
        if has_active_ad:
            keyboard.add_callback_button("🔁 Продлить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": ".продлить"})
        keyboard.add_line()
        keyboard.add_callback_button("🛒 Купить новую рекламу", color=VkKeyboardColor.SECONDARY, payload={"command": ".купить"})
        return keyboard.get_keyboard()

    keyboard.add_callback_button("📢 Купить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": ".купить"})
    keyboard.add_line()
    keyboard.add_callback_button("❓ Помощь", color=VkKeyboardColor.SECONDARY, payload={"command": ".помощь"})
    keyboard.add_callback_button("ℹ️ Информация о нас", color=VkKeyboardColor.SECONDARY, payload={"command": ".инфо_о_нас"})
    return keyboard.get_keyboard()


def generate_main_text_rates_menu():
    keyboard = VkKeyboard(inline=True)
    items = list(MAIN_TEXT_RATES.items())
    for index, (days, data) in enumerate(items, start=1):
        keyboard.add_callback_button(
            f"{data['name']} — {data['price']} ₽",
            color=VkKeyboardColor.POSITIVE,
            payload={"command": "buy_main_text", "days": days},
        )
        if index < len(items):
            keyboard.add_line()
    return keyboard.get_keyboard()


def generate_rates_menu():
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("3 дня — 70 ₽", color=VkKeyboardColor.POSITIVE, payload={"command": "rate_select", "days": 3})
    keyboard.add_callback_button("1 неделя — 150 ₽", color=VkKeyboardColor.POSITIVE, payload={"command": "rate_select", "days": 7})
    keyboard.add_line()
    keyboard.add_callback_button("месяц — 450 ₽", color=VkKeyboardColor.POSITIVE, payload={"command": "rate_select", "days": 30})
    return keyboard.get_keyboard()


def generate_order_details_kb():
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("✅ Одобрить", color=VkKeyboardColor.POSITIVE, payload={"command": "approve_ui"})
    keyboard.add_callback_button("❌ Отклонить", color=VkKeyboardColor.NEGATIVE, payload={"command": "reject_ui"})
    keyboard.add_line()
    keyboard.add_callback_button("📎 Показать чек", color=VkKeyboardColor.SECONDARY, payload={"command": "show_check_ui"})
    keyboard.add_callback_button("🔙 Выйти", color=VkKeyboardColor.SECONDARY, payload={"command": "exit_ui"})
    return keyboard.get_keyboard()


def generate_exit_kb():
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("🔙 Выйти", color=VkKeyboardColor.SECONDARY, payload={"command": "exit_ui"})
    return keyboard.get_keyboard()
