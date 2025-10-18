import os
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.error import BadRequest
from dotenv import load_dotenv
import logging
import asyncio
import re
import requests
from bs4 import BeautifulSoup
import tempfile
import time
from functools import wraps
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def rate_limit(limit_seconds=2):
    """Декоратор для защиты от множественных нажатий"""
    def decorator(func):
        last_called = {}
        
        @wraps(func)
        async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # Получаем user_id в зависимости от типа update
            if hasattr(update, 'effective_user'):
                user_id = update.effective_user.id
            elif hasattr(update, 'from_user'):  # Для CallbackQuery
                user_id = update.from_user.id
            elif hasattr(update, 'message') and update.message:
                user_id = update.message.from_user.id
            else:
                # Если не можем определить пользователя, пропускаем проверку
                return await func(self, update, context, *args, **kwargs)
                
            current_time = time.time()
            
            if user_id in last_called:
                time_passed = current_time - last_called[user_id]
                if time_passed < limit_seconds:
                    try:
                        if hasattr(update, 'callback_query'):
                            await update.callback_query.answer(
                                f"⏳ Подождите {limit_seconds - int(time_passed)} секунд", 
                                show_alert=False
                            )
                        else:
                            await update.message.reply_text(
                                f"⏳ Подождите {limit_seconds - int(time_passed)} секунд перед следующим действием"
                            )
                    except:
                        pass
                    return
            
            last_called[user_id] = current_time
            return await func(self, update, context, *args, **kwargs)
        
        return wrapper
    return decorator

class ScheduleBot:
    def __init__(self):
        self.token = os.getenv('BOT_TOKEN')
        self.excel_file = os.getenv('EXCEL_FILE_PATH')
        self.user_data = {}
        self.week_info_cache = None
        self.df_cache = None
        self.last_download_time = None
        self.data_loaded = False
        self.last_action_time = {}
        
    def download_schedule_from_website(self):
        """Скачать расписание с сайта ktmu-sutd.ru"""
        try:
            logger.info("🔄 Загрузка расписания...")
            
            timetable_url = "https://ktmu-sutd.ru/timetable.html"
            
            session = requests.Session()
            # КРИТИЧЕСКИ ВАЖНО: отключаем использование системного прокси
            session.trust_env = False
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Быстрая загрузка страницы
            response = session.get(timetable_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Быстрый поиск ссылок
            schedule_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                text = link.get_text(strip=True)
                
                if href.startswith('/'):
                    full_url = f"https://ktmu-sutd.ru{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                url_lower = full_url.lower()
                
                # Быстрая проверка форматов
                if any(ext in url_lower for ext in ['.xlsx', '.xls']):
                    schedule_links.append((full_url, text, "EXCEL"))
                elif any(domain in url_lower for domain in ['docs.google.com', 'drive.google.com']):
                    schedule_links.append((full_url, text, "GOOGLE_DOCS"))
            
            if not schedule_links:
                return self.download_schedule_alternative()
            
            # Пробуем скачать файлы
            for file_url, file_name, file_type in schedule_links[:3]:  # Ограничиваем количество попыток
                try:
                    download_url = file_url
                    if file_type == "GOOGLE_DOCS":
                        excel_url = self.convert_google_docs_to_excel(file_url)
                        if excel_url:
                            download_url = excel_url
                        else:
                            continue
                    
                    file_response = session.get(download_url, headers=headers, timeout=30)
                    
                    if file_response.status_code == 200 and len(file_response.content) > 10000:
                        # Быстрая проверка файла
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                            tmp_file.write(file_response.content)
                            temp_excel_file = tmp_file.name
                        
                        # Проверяем только наличие нужных листов
                        try:
                            excel_file = pd.ExcelFile(temp_excel_file)
                            sheet_names = excel_file.sheet_names
                            
                            if any(any(keyword in sheet.lower() for keyword in ['1 поток', '1_поток', 'kr', 'крд']) 
                                  for sheet in sheet_names):
                                logger.info(f"✅ Найден файл с листом 1 потока")
                                self.excel_file = temp_excel_file
                                self.df_cache = None
                                self.week_info_cache = None
                                self.data_loaded = True
                                return True
                            else:
                                os.unlink(temp_excel_file)
                                continue
                                
                        except Exception:
                            try:
                                os.unlink(temp_excel_file)
                            except:
                                pass
                            continue
                            
                except Exception:
                    continue
            
            return self.download_schedule_alternative()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return self.download_schedule_alternative()

    def convert_google_docs_to_excel(self, google_docs_url):
        """Быстрое преобразование Google Docs ссылки"""
        try:
            google_docs_url = google_docs_url.replace('gooogle', 'google')
            
            if '/spreadsheets/d/' in google_docs_url:
                match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', google_docs_url)
                if match:
                    return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/export?format=xlsx"
            
            elif '/file/d/' in google_docs_url or 'drive.google.com' in google_docs_url:
                patterns = [r'/file/d/([a-zA-Z0-9-_]+)', r'id=([a-zA-Z0-9-_]+)', r'/d/([a-zA-Z0-9-_]+)']
                for pattern in patterns:
                    match = re.search(pattern, google_docs_url)
                    if match:
                        return f"https://drive.google.com/uc?export=download&id={match.group(1)}"
            
            return None
        except:
            return None

    def download_schedule_alternative(self):
        """Быстрая альтернативная загрузка"""
        try:
            known_links = [
                "https://docs.google.com/spreadsheets/d/1zyuQ2Z1tXrTh3mU3JX4bZMonwsQFruf3/export?format=xlsx",
            ]
            
            session = requests.Session()
            # Отключаем прокси и для альтернативной загрузки
            session.trust_env = False
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            for link in known_links:
                try:
                    response = session.get(link, headers=headers, timeout=15)
                    if response.status_code == 200:
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                            tmp_file.write(response.content)
                            self.excel_file = tmp_file.name
                        
                        self.df_cache = None
                        self.week_info_cache = None
                        self.data_loaded = True
                        return True
                except:
                    continue
            
            return False
        except:
            return False

    def get_dataframe(self, force_download=False):
        """Оптимизированное кэширование DataFrame"""
        try:
            if force_download or not self.data_loaded:
                logger.info("🔄 Принудительная загрузка данных...")
                success = self.download_schedule_from_website()
                if not success:
                    logger.warning("⚠️ Не удалось загрузить данные")
                    # Пробуем загрузить локальный файл, если есть
                    if self.excel_file and os.path.exists(self.excel_file):
                        logger.info("🔄 Используем локальный файл...")
                        try:
                            excel_file = pd.ExcelFile(self.excel_file)
                            sheet_names = excel_file.sheet_names
                            
                            target_sheet = None
                            for sheet in sheet_names:
                                if any(keyword in sheet.lower() for keyword in ['1 поток', '1_поток', 'kr', 'крд']):
                                    target_sheet = sheet
                                    break
                            
                            if not target_sheet and sheet_names:
                                target_sheet = sheet_names[0]
                            
                            if target_sheet:
                                self.df_cache = pd.read_excel(self.excel_file, sheet_name=target_sheet, header=None)
                                self.data_loaded = True
                                logger.info(f"✅ DataFrame загружен с локального файла: {target_sheet}")
                                return self.df_cache
                        except Exception as e:
                            logger.error(f"❌ Ошибка загрузки локального файла: {e}")
                    return None
            
            if self.df_cache is None and self.excel_file and os.path.exists(self.excel_file):
                # Быстрая загрузка с определением листа
                excel_file = pd.ExcelFile(self.excel_file)
                sheet_names = excel_file.sheet_names
                
                target_sheet = None
                for sheet in sheet_names:
                    if any(keyword in sheet.lower() for keyword in ['1 поток', '1_поток', 'kr', 'крд']):
                        target_sheet = sheet
                        break
                
                if not target_sheet and sheet_names:
                    target_sheet = sheet_names[0]
                
                if target_sheet:
                    self.df_cache = pd.read_excel(self.excel_file, sheet_name=target_sheet, header=None)
                    logger.info(f"✅ DataFrame загружен с листа: {target_sheet}")
            
            return self.df_cache
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки DataFrame: {e}")
            return None

    def is_data_loaded(self):
        """Проверка, загружены ли данные"""
        return self.data_loaded and self.df_cache is not None

    def get_current_academic_week(self):
        """Получить текущую учебную неделю из расписания"""
        try:
            week_info = self.get_week_info()
            if not week_info:
                return "1"  # По умолчанию первая неделя
            
            # Получаем текущую дату
            today = pd.Timestamp.now().normalize()
            
            # Ищем неделю, которая содержит текущую дату
            for week_num, info in week_info.items():
                week_desc = info.get('description', '').lower()
                
                # Парсим даты из описания недели
                dates = re.findall(r'\d{1,2}\.\d{1,2}\.\d{4}', week_desc)
                if len(dates) >= 2:
                    try:
                        start_date = pd.to_datetime(dates[0], format='%d.%m.%Y')
                        end_date = pd.to_datetime(dates[1], format='%d.%m.%Y')
                        
                        if start_date <= today <= end_date:
                            return week_num
                    except:
                        continue
            
            # Если не нашли, возвращаем первую неделю
            return sorted(week_info.keys())[0]
            
        except Exception as e:
            logger.error(f"❌ Ошибка определения учебной недели: {e}")
            return "1"

    def get_monday_date(self, week_number):
        """Получить дату понедельника для указанной недели"""
        try:
            df = self.get_dataframe()
            if df is None:
                return None
            
            week_info = self.get_week_info()
            if not week_info or week_number not in week_info:
                return None
            
            week_data = week_info[week_number]
            monday_col = week_data['columns'][0]  # Первый столбец - понедельник
            
            # Ищем дату в строке 4 (индекс 3) для понедельника
            date_cell = df.iloc[3, monday_col]
            if pd.notna(date_cell):
                date_str = str(date_cell).strip()
                # Пытаемся извлечь дату
                date_match = re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', date_str)
                if date_match:
                    try:
                        return datetime.strptime(date_match.group(0), '%d.%m.%Y')
                    except:
                        pass
            
            # Если не нашли дату в ячейке, пытаемся извлечь из описания недели
            week_desc = week_data.get('description', '')
            dates = re.findall(r'\d{1,2}\.\d{1,2}\.\d{4}', week_desc)
            if dates:
                try:
                    return datetime.strptime(dates[0], '%d.%m.%Y')
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения даты понедельника: {e}")
            return None

    def get_day_date(self, week_number, day_index):
        """Получить дату для конкретного дня недели"""
        try:
            # Получаем дату понедельника
            monday_date = self.get_monday_date(week_number)
            if not monday_date:
                return ""
            
            # Вычисляем дату для текущего дня (добавляем дни к понедельнику)
            target_date = monday_date + timedelta(days=day_index)
            return target_date.strftime('%d.%m.%Y')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения даты дня: {e}")
            return ""

    def get_current_week_and_day(self):
        """Получить текущую неделю и день с правильной логикой"""
        week_number = self.get_current_academic_week()
        
        # Текущий день недели (0-понедельник, 6-воскресенье)
        current_day = pd.Timestamp.now().dayofweek
        if current_day >= 6:  # Воскресенье
            current_day = 0   # Показываем понедельник
            # Если воскресенье, переходим к следующей неделе
            week_info = self.get_week_info()
            if week_info:
                weeks = sorted(week_info.keys(), key=int)
                current_index = weeks.index(week_number)
                if current_index < len(weeks) - 1:
                    week_number = weeks[current_index + 1]
        
        return week_number, current_day

    def get_week_info(self):
        """Оптимизированное получение информации о неделях"""
        if self.week_info_cache is not None:
            return self.week_info_cache
            
        try:
            df = self.get_dataframe()
            if df is None or len(df.columns) == 0:
                return {}
            
            week_info = {}
            # Быстрый поиск недель в строке 4
            for col in range(min(50, len(df.columns))):  # Ограничиваем поиск
                cell_value = df.iloc[3, col]
                if pd.notna(cell_value) and 'неделя' in str(cell_value).lower():
                    week_text = str(cell_value)
                    week_num, week_type, date_range = self._parse_week_info(week_text)
                    
                    if week_num:
                        week_columns = self._find_week_columns_simple(col, week_num)
                        week_info[week_num] = {
                            'type': week_type,
                            'description': week_text,
                            'date_range': date_range,
                            'columns': week_columns,
                            'header_column': col
                        }
            
            self.week_info_cache = week_info
            logger.info(f"✅ Загружена информация о {len(week_info)} неделях")
            return week_info
        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о неделях: {e}")
            return {}

    def _parse_week_info(self, week_text):
        """Быстрый парсинг информации о неделе с датами"""
        week_text_lower = week_text.lower()
        
        week_type = 'Нечетная' if 'нечетная' in week_text_lower or 'числитель' in week_text_lower else \
                   'Четная' if 'четная' in week_text_lower or 'знаменатель' in week_text_lower else 'Неизвестно'
        
        # Ищем номер недели
        bracket_match = re.search(r'\((\d+)\)', week_text)
        if bracket_match:
            week_num = bracket_match.group(1)
        else:
            numbers = re.findall(r'\d+', week_text)
            week_num = numbers[0] if numbers else None
        
        # Ищем даты
        dates = re.findall(r'\d{1,2}\.\d{1,2}\.\d{4}', week_text)
        date_range = ""
        if len(dates) >= 2:
            date_range = f"{dates[0]} - {dates[1]}"
        
        return week_num, week_type, date_range

    def _find_week_columns_simple(self, header_col, week_number):
        """Быстрый поиск столбцов недели"""
        try:
            df = self.get_dataframe()
            if df is None:
                return []
            
            return [header_col + i for i in range(6) if header_col + i < len(df.columns)]
        except:
            return []

    async def safe_edit_message(self, query, text, reply_markup=None, parse_mode='HTML'):
        """Безопасное редактирование сообщения"""
        try:
            await query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            return True
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"❌ Ошибка редактирования: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            return False

    async def setup_commands(self, application):
        """Настройка меню команд"""
        commands = [
            BotCommand("start", "🚀 Запустить бота"),
            BotCommand("menu", "📋 Главное меню"),
            BotCommand("refresh", "🔄 Обновить расписание"),
            BotCommand("week", "📅 Выбрать неделю"),
            BotCommand("today", "📆 Расписание на сегодня"),
            BotCommand("tomorrow", "📆 Расписание на завтра"),
            BotCommand("monday", "📆 Понедельник"),
            BotCommand("tuesday", "📆 Вторник"),
            BotCommand("wednesday", "📆 Среда"),
            BotCommand("thursday", "📆 Четверг"),
            BotCommand("friday", "📆 Пятница"),
            BotCommand("saturday", "📆 Суббота"),
            BotCommand("debug", "🐛 Отладочная информация"),
        ]
        
        await application.bot.set_my_commands(commands)
        await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    @rate_limit(limit_seconds=2)
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start с автоматическим обновлением"""
        # Отправляем сообщение о начале загрузки
        loading_message = await update.message.reply_text(
            "🔄 <b>Загружаю актуальное расписание...</b>\n"
            "<i>Это займет несколько секунд</i>", 
            parse_mode='HTML'
        )
        
        # Принудительно обновляем расписание
        success = await asyncio.get_event_loop().run_in_executor(None, self.download_schedule_from_website)
        
        if success:
            await loading_message.edit_text(
                "✅ <b>Расписание успешно загружено!</b>\n"
                "Теперь вы можете просматривать актуальное расписание.",
                parse_mode='HTML'
            )
            await asyncio.sleep(1)
            # Показываем главное меню после запуска
            await self.show_main_menu(update, context)
        else:
            await loading_message.edit_text(
                "❌ <b>Не удалось загрузить расписание</b>\n"
                "Используется старая версия. Попробуйте обновить позже.",
                parse_mode='HTML'
            )
            await asyncio.sleep(1)
            await self.show_main_menu(update, context)

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text=None):
        """Показать главное меню"""
        keyboard = [
            [InlineKeyboardButton("📅 Выбрать неделю", callback_data="select_week")],
            [InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Обновить расписание", callback_data="refresh_schedule")],
            [InlineKeyboardButton("🐛 Отладка", callback_data="debug_weeks")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status = "✅ Актуальное" if self.data_loaded else "⚠️ Старое"
        
        text = message_text or (
            f"👋 <b>Бот расписания 1-КРД-6</b>\n\n"
            f"📊 <b>Статус данных:</b> {status}\n\n"
            "🚀 <b>Доступные команды:</b>\n"
            "• /menu - Главное меню\n"
            "• /refresh - Обновить расписание\n"
            "• /week - Выбрать неделю\n"
            "• /today - Сегодня\n• /tomorrow - Завтра\n"
            "• /monday - Пн\n• /tuesday - Вт\n• /wednesday - Ср\n"
            "• /thursday - Чт\n• /friday - Пт\n• /saturday - Сб\n\n"
            "👇 <i>Или используйте кнопки ниже:</i>"
        )
        
        if update.message:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

    @rate_limit(limit_seconds=2)
    async def menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /menu"""
        await self.show_main_menu(update, context)

    @rate_limit(limit_seconds=3)
    async def refresh(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /refresh"""
        message = await update.message.reply_text("🔄 Обновляю расписание...")
        
        success = await asyncio.get_event_loop().run_in_executor(None, self.download_schedule_from_website)
        
        if success:
            await message.edit_text("✅ Расписание обновлено!")
            await asyncio.sleep(1)
            # Показываем главное меню после обновления
            await self.show_main_menu(update, context)
        else:
            await message.edit_text("❌ Не удалось обновить расписание")

    @rate_limit(limit_seconds=2)
    async def week(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /week"""
        # Проверяем, загружены ли данные
        if not self.is_data_loaded():
            await update.message.reply_text(
                "❌ Данные не загружены. Сначала используйте /start или /refresh",
                parse_mode='HTML'
            )
            return
        
        # Показываем выбор недели
        await self.show_week_selection_standalone(update, context)

    @rate_limit(limit_seconds=2)
    async def today(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /today - расписание на сегодня"""
        if not self.is_data_loaded():
            await update.message.reply_text(
                "❌ Данные не загружены. Сначала используйте /start или /refresh",
                parse_mode='HTML'
            )
            return
        
        week_number, day_idx = self.get_current_week_and_day()
        await self.show_day_schedule_standalone(update, week_number, day_idx, "сегодня")

    @rate_limit(limit_seconds=2)
    async def tomorrow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /tomorrow - расписание на завтра"""
        if not self.is_data_loaded():
            await update.message.reply_text(
                "❌ Данные не загружены. Сначала используйте /start или /refresh",
                parse_mode='HTML'
            )
            return
        
        week_number, current_day = self.get_current_week_and_day()
        
        # Определяем день для завтра
        tomorrow_idx = current_day + 1
        week_change = False
        
        if tomorrow_idx >= 6:  # Если завтра воскресенье или после субботы
            tomorrow_idx = 0
            week_change = True
        
        # Если переходим на следующую неделю
        if week_change:
            week_info = self.get_week_info()
            if week_info:
                weeks = sorted(week_info.keys(), key=int)
                current_index = weeks.index(week_number)
                if current_index < len(weeks) - 1:
                    week_number = weeks[current_index + 1]
        
        await self.show_day_schedule_standalone(update, week_number, tomorrow_idx, "завтра")

    @rate_limit(limit_seconds=2)
    async def monday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /monday - понедельник"""
        await self.show_day_by_name(update, 0, "понедельник")

    @rate_limit(limit_seconds=2)
    async def tuesday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /tuesday - вторник"""
        await self.show_day_by_name(update, 1, "вторник")

    @rate_limit(limit_seconds=2)
    async def wednesday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /wednesday - среда"""
        await self.show_day_by_name(update, 2, "среду")

    @rate_limit(limit_seconds=2)
    async def thursday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /thursday - четверг"""
        await self.show_day_by_name(update, 3, "четверг")

    @rate_limit(limit_seconds=2)
    async def friday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /friday - пятница"""
        await self.show_day_by_name(update, 4, "пятницу")

    @rate_limit(limit_seconds=2)
    async def saturday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /saturday - суббота"""
        await self.show_day_by_name(update, 5, "субботу")

    async def show_day_by_name(self, update: Update, day_idx: int, day_name: str):
        """Показать расписание по названию дня"""
        if not self.is_data_loaded():
            await update.message.reply_text(
                "❌ Данные не загружены. Сначала используйте /start или /refresh",
                parse_mode='HTML'
            )
            return
        
        # Для дней недели используем текущую учебную неделю
        week_number = self.get_current_academic_week()
        await self.show_day_schedule_standalone(update, week_number, day_idx, day_name)

    async def show_day_schedule_standalone(self, update: Update, week_number: str, day_idx: int, day_name: str):
        """Показать расписание дня как отдельное сообщение"""
        loading_message = await update.message.reply_text(f"⏳ Загружаю расписание на {day_name}...")
        
        schedule = await asyncio.get_event_loop().run_in_executor(
            None, self.get_1krd6_schedule, week_number, day_idx
        )
        
        # Добавляем информацию о неделе
        week_info = self.get_week_info()
        week_data = week_info.get(week_number, {})
        week_type = week_data.get('type', '')
        
        full_schedule = f"📅 <b>Неделя {week_number}</b> ({week_type})\n{schedule}"
        
        keyboard = [
            [InlineKeyboardButton("📆 Другой день", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await loading_message.edit_text(full_schedule, reply_markup=reply_markup, parse_mode='HTML')

    async def show_week_selection_standalone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать выбор недели как отдельное сообщение"""
        week_info = await asyncio.get_event_loop().run_in_executor(None, self.get_week_info)
        
        if not week_info:
            await update.message.reply_text("❌ Не удалось загрузить информацию о неделях")
            return
        
        keyboard = []
        for week_num, info in sorted(week_info.items(), key=lambda x: int(x[0])):
            button_text = f"Неделя {week_num} ({info['type']})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"week_{week_num}")])
        
        keyboard.append([InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")])
        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="refresh_schedule")])
        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📆 <b>Выберите неделю:</b>",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

    @rate_limit(limit_seconds=2)
    async def debug(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /debug"""
        debug_info = await asyncio.get_event_loop().run_in_executor(None, self.debug_weeks_info)
        await update.message.reply_text(f"<pre>{debug_info}</pre>", parse_mode='HTML')

    @rate_limit(limit_seconds=2)
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback'ов"""
        query = update.callback_query
        
        try:
            # Всегда отвечаем на callback_query чтобы убрать "часики" в интерфейсе
            await query.answer()
        except Exception as e:
            logger.warning(f"⚠️ Ошибка ответа на callback: {e}")
        
        try:
            if query.data == "select_week":
                await self.show_week_selection(query, context)
            elif query.data == "refresh_schedule":
                await self.handle_refresh(query, context)
            elif query.data == "quick_days":
                await self.show_quick_days(query, context)
            elif query.data == "debug_weeks":
                await self.handle_debug(query, context)
            elif query.data == "back_to_menu":
                await self.show_main_menu(update, context)
            elif query.data.startswith("week_"):
                await self.handle_week_selection(query, context, query.data.replace("week_", ""))
            elif query.data.startswith("day_"):
                await self.handle_day_selection(query, context, query.data)
            elif query.data.startswith("all_days_"):
                await self.handle_all_days(query, context, query.data.replace("all_days_", ""))
            elif query.data.startswith("quick_day_"):
                await self.handle_quick_day_selection(query, context, query.data.replace("quick_day_", ""))
            # Обработка быстрых команд из меню дней
            elif query.data == "quick_today":
                await self.handle_quick_today(query, context)
            elif query.data == "quick_tomorrow":
                await self.handle_quick_tomorrow(query, context)
            elif query.data == "quick_monday":
                await self.handle_quick_day(query, context, 0, "понедельник")
            elif query.data == "quick_tuesday":
                await self.handle_quick_day(query, context, 1, "вторник")
            elif query.data == "quick_wednesday":
                await self.handle_quick_day(query, context, 2, "среду")
            elif query.data == "quick_thursday":
                await self.handle_quick_day(query, context, 3, "четверг")
            elif query.data == "quick_friday":
                await self.handle_quick_day(query, context, 4, "пятницу")
            elif query.data == "quick_saturday":
                await self.handle_quick_day(query, context, 5, "субботу")
                
        except Exception as e:
            logger.error(f"❌ Ошибка callback: {e}")
            try:
                await query.edit_message_text("❌ Произошла ошибка. Попробуйте еще раз.")
            except Exception as edit_error:
                logger.error(f"❌ Ошибка редактирования сообщения: {edit_error}")

    async def handle_quick_today(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик быстрой команды 'Сегодня'"""
        # Проверяем загрузку данных с правильным сообщением
        df = self.get_dataframe()
        if df is None or not self.is_data_loaded():
            await self.safe_edit_message(
                query, 
                "❌ Данные не загружены. Используйте /start или /refresh для загрузки расписания."
            )
            return
        
        week_number, day_idx = self.get_current_week_and_day()
        await self.show_quick_day_schedule(query, week_number, day_idx, "сегодня")

    async def handle_quick_tomorrow(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик быстрой команды 'Завтра'"""
        df = self.get_dataframe()
        if df is None or not self.is_data_loaded():
            await self.safe_edit_message(
                query, 
                "❌ Данные не загружены. Используйте /start или /refresh для загрузки расписания."
            )
            return
        
        week_number, current_day = self.get_current_week_and_day()
        
        # Определяем день для завтра
        tomorrow_idx = current_day + 1
        week_change = False
        
        if tomorrow_idx >= 6:  # Если завтра воскресенье или после субботы
            tomorrow_idx = 0
            week_change = True
        
        # Если переходим на следующую неделю
        if week_change:
            week_info = self.get_week_info()
            if week_info:
                weeks = sorted(week_info.keys(), key=int)
                current_index = weeks.index(week_number)
                if current_index < len(weeks) - 1:
                    week_number = weeks[current_index + 1]
        
        await self.show_quick_day_schedule(query, week_number, tomorrow_idx, "завтра")

    async def handle_quick_day(self, query, context: ContextTypes.DEFAULT_TYPE, day_idx: int, day_name: str):
        """Обработчик быстрой команды дня недели"""
        df = self.get_dataframe()
        if df is None or not self.is_data_loaded():
            await self.safe_edit_message(
                query, 
                "❌ Данные не загружены. Используйте /start или /refresh для загрузки расписания."
            )
            return
        
        # Для дней недели используем текущую учебную неделю
        week_number = self.get_current_academic_week()
        await self.show_quick_day_schedule(query, week_number, day_idx, day_name)

    async def show_quick_day_schedule(self, query, week_number: str, day_idx: int, day_name: str):
        """Показать расписание дня в меню быстрых команд"""
        await self.safe_edit_message(query, f"⏳ Загружаю расписание на {day_name}...")
        
        schedule = await asyncio.get_event_loop().run_in_executor(
            None, self.get_1krd6_schedule, week_number, day_idx
        )
        
        # Добавляем информацию о неделе
        week_info = self.get_week_info()
        week_data = week_info.get(week_number, {})
        week_type = week_data.get('type', '')
        
        full_schedule = f"📅 <b>Неделя {week_number}</b> ({week_type})\n{schedule}"
        
        keyboard = [
            [InlineKeyboardButton("📆 Другой день", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(query, full_schedule, reply_markup)

    async def handle_refresh(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик обновления расписания"""
        await self.safe_edit_message(query, "🔄 Обновляю расписание...")
        success = await asyncio.get_event_loop().run_in_executor(None, self.download_schedule_from_website)
        
        if success:
            await self.safe_edit_message(query, "✅ Расписание обновлено!")
            await asyncio.sleep(1)
            # Используем query для перехода в главное меню
            await self.show_main_menu_from_query(query, context)
        else:
            await self.safe_edit_message(query, "❌ Не удалось обновить расписание")

    async def show_main_menu_from_query(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню из callback query"""
        keyboard = [
            [InlineKeyboardButton("📅 Выбрать неделю", callback_data="select_week")],
            [InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Обновить расписание", callback_data="refresh_schedule")],
            [InlineKeyboardButton("🐛 Отладка", callback_data="debug_weeks")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status = "✅ Актуальное" if self.data_loaded else "⚠️ Старое"
        
        text = (
            f"👋 <b>Бот расписания 1-КРД-6</b>\n\n"
            f"📊 <b>Статус данных:</b> {status}\n\n"
            "🚀 <b>Доступные команды:</b>\n"
            "• /menu - Главное меню\n"
            "• /refresh - Обновить расписание\n"
            "• /week - Выбрать неделю\n"
            "• /today - Сегодня\n• /tomorrow - Завтра\n"
            "• /monday - Пн\n• /tuesday - Вт\n• /wednesday - Ср\n"
            "• /thursday - Чт\n• /friday - Пт\n• /saturday - Сб\n\n"
            "👇 <i>Или используйте кнопки ниже:</i>"
        )
        
        await self.safe_edit_message(query, text, reply_markup)

    async def show_quick_days(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Быстрый доступ по дням недели"""
        keyboard = [
            [
                InlineKeyboardButton("📆 Сегодня", callback_data="quick_today"),
                InlineKeyboardButton("📆 Завтра", callback_data="quick_tomorrow")
            ],
            [
                InlineKeyboardButton("📆 Понедельник", callback_data="quick_monday"),
                InlineKeyboardButton("📆 Вторник", callback_data="quick_tuesday")
            ],
            [
                InlineKeyboardButton("📆 Среда", callback_data="quick_wednesday"),
                InlineKeyboardButton("📆 Четверг", callback_data="quick_thursday")
            ],
            [
                InlineKeyboardButton("📆 Пятница", callback_data="quick_friday"),
                InlineKeyboardButton("📆 Суббота", callback_data="quick_saturday")
            ],
            [InlineKeyboardButton("📅 Выбрать неделю", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(
            query,
            "📆 <b>Быстрый доступ по дням</b>\n\n"
            "Выберите день для просмотра расписания:",
            reply_markup
        )

    async def handle_quick_day_selection(self, query, context: ContextTypes.DEFAULT_TYPE, day_data):
        """Обработчик быстрого выбора дня"""
        df = self.get_dataframe()
        if df is None or not self.is_data_loaded():
            await self.safe_edit_message(
                query, 
                "❌ Данные не загружены. Используйте /start или /refresh для загрузки расписания."
            )
            return
        
        week_number, current_day = self.get_current_week_and_day()
        
        if day_data == "today":
            day_idx = current_day
            day_name = "сегодня"
        elif day_data == "tomorrow":
            day_idx = (current_day + 1) % 6
            day_name = "завтра"
        else:
            day_idx = int(day_data)
            days = ["понедельник", "вторник", "среду", "четверг", "пятницу", "субботу"]
            day_name = days[day_idx]
        
        await self.safe_edit_message(query, f"⏳ Загружаю расписание на {day_name}...")
        
        schedule = await asyncio.get_event_loop().run_in_executor(
            None, self.get_1krd6_schedule, week_number, day_idx
        )
        
        # Добавляем информацию о неделе
        week_info = self.get_week_info()
        week_data = week_info.get(week_number, {})
        week_type = week_data.get('type', '')
        
        full_schedule = f"📅 <b>Неделя {week_number}</b> ({week_type})\n{schedule}"
        
        keyboard = [
            [InlineKeyboardButton("📆 Другой день", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(query, full_schedule, reply_markup)

    async def show_week_selection(self, query=None, context: ContextTypes.DEFAULT_TYPE = None):
        """Показать выбор недели"""
        week_info = await asyncio.get_event_loop().run_in_executor(None, self.get_week_info)
        
        if not week_info:
            if query:
                await self.safe_edit_message(query, "❌ Не удалось загрузить информацию о неделях")
            return
        
        keyboard = []
        for week_num, info in sorted(week_info.items(), key=lambda x: int(x[0])):
            button_text = f"Неделя {week_num} ({info['type']})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"week_{week_num}")])
        
        keyboard.append([InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")])
        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="refresh_schedule")])
        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if query:
            await self.safe_edit_message(
                query,
                "📆 <b>Выберите неделю:</b>",
                reply_markup
            )

    async def handle_week_selection(self, query, context: ContextTypes.DEFAULT_TYPE, week_number):
        """Обработчик выбора недели"""
        context.user_data['selected_week'] = week_number
        
        days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        
        keyboard = []
        for day_idx, day_name in enumerate(days):
            keyboard.append([InlineKeyboardButton(day_name, callback_data=f"day_{week_number}_{day_idx}")])
        
        keyboard.append([InlineKeyboardButton("📅 Вся неделя", callback_data=f"all_days_{week_number}")])
        keyboard.append([InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")])
        keyboard.append([InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")])
        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        week_info = self.get_week_info()
        week_data = week_info.get(week_number, {})
        
        await self.safe_edit_message(
            query,
            f"📆 <b>Неделя {week_number}</b> ({week_data.get('type', '')})\n"
            f"📅 {week_data.get('description', '')}\n\n"
            "Выберите день недели:",
            reply_markup
        )

    async def handle_day_selection(self, query, context: ContextTypes.DEFAULT_TYPE, day_data):
        """Обработчик выбора дня"""
        week_number, day_idx = day_data.replace("day_", "").split('_')
        day_idx = int(day_idx)
        
        await self.safe_edit_message(query, "⏳ Загружаю расписание...")
        
        schedule = await asyncio.get_event_loop().run_in_executor(
            None, self.get_1krd6_schedule, week_number, day_idx
        )
        
        keyboard = [
            [InlineKeyboardButton("📅 Другой день", callback_data=f"week_{week_number}")],
            [InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(query, schedule, reply_markup)

    async def handle_all_days(self, query, context: ContextTypes.DEFAULT_TYPE, week_number):
        """Обработчик всей недели"""
        await self.safe_edit_message(query, "⏳ Загружаю расписание на неделю...")
        
        schedule = await asyncio.get_event_loop().run_in_executor(
            None, self.get_full_week_schedule, week_number
        )
        
        keyboard = [
            [InlineKeyboardButton("📅 Конкретный день", callback_data=f"week_{week_number}")],
            [InlineKeyboardButton("📆 Быстрый доступ по дням", callback_data="quick_days")],
            [InlineKeyboardButton("🔄 Другая неделя", callback_data="select_week")],
            [InlineKeyboardButton("📋 Главное меню", callback_data="back_to_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(query, schedule, reply_markup)

    async def handle_debug(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик отладки"""
        debug_info = await asyncio.get_event_loop().run_in_executor(None, self.debug_weeks_info)
        
        if len(debug_info) > 4000:
            parts = [debug_info[i:i+4000] for i in range(0, len(debug_info), 4000)]
            for i, part in enumerate(parts):
                await query.message.reply_text(f"<pre>Часть {i+1}:\n{part}</pre>", parse_mode='HTML')
        else:
            await query.message.reply_text(f"<pre>{debug_info}</pre>", parse_mode='HTML')

    def get_1krd6_schedule(self, week_number="1", day_filter=None):
        """Оптимизированное получение расписания"""
        try:
            if day_filter is None:
                return self.get_full_week_schedule(week_number)
            return self._get_day_schedule(week_number, day_filter, show_day_header=True)
        except Exception as e:
            return f"❌ Ошибка: {str(e)}"

    def get_full_week_schedule(self, week_number="1"):
        """Оптимизированное получение расписания на неделю"""
        try:
            df = self.get_dataframe()
            if df is None:
                return "❌ Ошибка загрузки данных"
            
            week_info = self.get_week_info()
            if not week_info or week_number not in week_info:
                return "❌ Неделя не найдена"
            
            week_data = week_info[week_number]
            schedule_text = f"📅 <b>Расписание 1-КРД-6</b>\n"
            schedule_text += f"🔢 Неделя: {week_number} ({week_data['type']})\n"
            schedule_text += f"📆 {week_data['description']}\n\n"
            
            days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
            
            for day_idx, day_col in enumerate(week_data['columns']):
                if day_idx >= len(days):
                    break
                day_schedule = self._get_day_schedule(week_number, day_idx, show_day_header=True)
                schedule_text += day_schedule + "\n"
            
            return schedule_text
        except Exception as e:
            return f"❌ Ошибка: {str(e)}"

    def _get_day_schedule(self, week_number, day_idx, show_day_header=True):
        """Оптимизированное получение расписания дня с датой"""
        try:
            df = self.get_dataframe()
            if df is None:
                return "❌ Ошибка загрузки данных"
            
            week_info = self.get_week_info()
            if not week_info or week_number not in week_info:
                return "❌ Неделя не найдена"
            
            week_data = week_info[week_number]
            day_col = week_data['columns'][day_idx]
            
            days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
            
            # Получаем дату дня для ВСЕХ дней
            day_date = self.get_day_date(week_number, day_idx)
            date_suffix = f" ({day_date})" if day_date else ""
            
            day_schedule = f"<b>{days[day_idx]}{date_suffix}</b>\n" if show_day_header else ""
            has_classes = False
            
            # Оптимизированный поиск времени
            time_cells = []
            for row in range(89, 131):  # Ограниченный диапазон
                cell_value = df.iloc[row, day_col]
                if pd.notna(cell_value):
                    cell_str = str(cell_value).strip()
                    if self._is_time_cell(cell_str):
                        time_value = self.extract_time_value(cell_value)
                        time_cells.append({'row': row, 'time': cell_str, 'time_value': time_value})
            
            time_cells.sort(key=lambda x: x['time_value'])
            pair_numbers = self._get_real_pair_numbers([tc['time_value'] for tc in time_cells])
            
            for i, time_cell in enumerate(time_cells):
                if i >= len(pair_numbers):
                    break
                    
                has_classes = True
                pair_num = pair_numbers[i]
                day_schedule += f"<b>Пара {pair_num}:</b>\n🕐 {time_cell['time']}\n"
                
                row = time_cell['row']
                # Быстрое получение данных
                for offset, label in enumerate(['📚', '👨‍🏫', '🏫', '📚', '👨‍🏫', '🏫'], 1):
                    if row + offset <= 130:
                        data_value = df.iloc[row + offset, day_col]
                        if pd.notna(data_value) and str(data_value).strip() not in ['', '(пусто)', 'nan']:
                            day_schedule += f"{label} {str(data_value).strip()}\n"
                
                day_schedule += "\n"
            
            if not has_classes:
                day_schedule += "❌ Пар нет\n\n"
            
            return day_schedule
            
        except Exception as e:
            return f"❌ Ошибка: {str(e)}"

    def _is_time_cell(self, cell_str):
        """Быстрая проверка времени"""
        if not cell_str or cell_str in ['', '(пусто)', 'nan']:
            return False
        return bool(re.search(r'\d{1,2}:\d{2}', cell_str))

    def extract_time_value(self, time_str):
        """Быстрое извлечение времени"""
        if pd.isna(time_str):
            return 0
        time_match = re.search(r'(\d{1,2}):(\d{2})', str(time_str).strip())
        if time_match:
            hours = int(time_match.group(1))
            minutes = int(time_match.group(2))
            return hours * 60 + minutes
        return 0

    def _get_real_pair_numbers(self, time_values):
        """Быстрое определение номеров пар"""
        if not time_values:
            return []
        pair_times = {1: 510, 2: 620, 3: 720, 4: 835, 5: 940, 6: 1045}
        return [min(pair_times.items(), key=lambda x: abs(x[1] - time_value))[0] for time_value in time_values]

    def debug_weeks_info(self):
        """Оптимизированная отладочная информация"""
        try:
            df = self.get_dataframe()
            if df is None:
                return "❌ Ошибка загрузки данных"
            
            debug_text = "🔍 ОТЛАДКА НЕДЕЛЬ:\n\n"
            debug_text += f"📊 Столбцов: {len(df.columns)}, Строк: {len(df)}\n\n"
            
            # Быстрый поиск недель
            week_cells = []
            for col in range(min(50, len(df.columns))):
                cell_value = df.iloc[3, col]
                if pd.notna(cell_value) and 'неделя' in str(cell_value).lower():
                    week_cells.append((col, str(cell_value)))
            
            debug_text += f"📋 Найдено недель: {len(week_cells)}\n"
            
            week_info = self.get_week_info()
            for week_num, info in sorted(week_info.items(), key=lambda x: int(x[0])):
                debug_text += f"\n📅 Неделя {week_num}: {info['description']}\n"
                debug_text += f"   Столбцы: {info['columns']}\n"
            
            return debug_text
        except Exception as e:
            return f"❌ Ошибка отладки: {str(e)}"

    def run(self):
        """Запуск бота"""
        application = Application.builder().token(self.token).build()
        
        # Регистрация команд
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("menu", self.menu))
        application.add_handler(CommandHandler("refresh", self.refresh))
        application.add_handler(CommandHandler("week", self.week))
        application.add_handler(CommandHandler("today", self.today))
        application.add_handler(CommandHandler("tomorrow", self.tomorrow))
        application.add_handler(CommandHandler("monday", self.monday))
        application.add_handler(CommandHandler("tuesday", self.tuesday))
        application.add_handler(CommandHandler("wednesday", self.wednesday))
        application.add_handler(CommandHandler("thursday", self.thursday))
        application.add_handler(CommandHandler("friday", self.friday))
        application.add_handler(CommandHandler("saturday", self.saturday))
        application.add_handler(CommandHandler("debug", self.debug))
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        
        # Настройка меню при запуске
        application.post_init = self.setup_commands
        
        logger.info("🤖 Бот запущен...")
        logger.info("📱 Используйте /start или /menu")
        
        try:
            application.run_polling(drop_pending_updates=True)
        except Exception as e:
            logger.error(f"❌ Ошибка запуска: {e}")

if __name__ == '__main__':
    bot = ScheduleBot()
    bot.run()
