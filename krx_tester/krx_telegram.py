# telegram_bot_with_queue.py
import asyncio
from telegram import Bot
from telegram.error import TelegramError


class TelegramSender:
    def __init__(self, token: str):
        self.bot = Bot(token)
        self.queue = asyncio.Queue()
        self.is_running = False

    async def send_telegram_message_async(self, chat_id: str, message: str):
        """Telegram 메시지 발송"""
        try:
            await self.bot.send_message(chat_id=chat_id, text=message)
            print(f"Message sent successfully to {chat_id}")
        except TelegramError as e:
            print(f"Error sending message: {e}")

    async def message_worker(self):
        """Queue에서 메시지를 하나씩 꺼내서 발송하는 Worker Task"""
        while self.is_running:
            chat_id, message = await self.queue.get()  # Queue에서 메시지 꺼내기
            await self.send_telegram_message_async(chat_id, message)
            self.queue.task_done()  # 작업 완료 알림

    def start(self):
        """Worker Task 시작"""
        self.is_running = True
        asyncio.create_task(self.message_worker())  # Worker Task 생성

    def stop(self):
        """Worker Task 중지"""
        self.is_running = False

    def send_message(self, chat_id: str, message: str):
        """Queue에 메시지 추가"""
        self.queue.put_nowait((chat_id, message))

    async def wait_until_done(self):
        """Queue의 모든 작업이 완료될 때까지 대기"""
        await self.queue.join()
