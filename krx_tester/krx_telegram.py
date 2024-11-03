# telegram_bot_with_queue.py
import asyncio
from telegram import Bot
from telegram.error import TelegramError


class TelegramSender:
    def __init__(self, token: str):
        self.bot = Bot(token)
        self.queue = asyncio.Queue()
        self.is_running = False

    async def send_telegram_photo_async(self, chat_id: str, photo_path: str, caption: str = None):
        """Telegram 사진 발송"""
        try:
            with open(photo_path, 'rb') as photo:
                await self.bot.send_photo(chat_id=chat_id, photo=photo, caption=caption)
        except TelegramError as e:
            print(f"Error sending photo: {e}")
            raise e
        except FileNotFoundError:
            print(f"Error: File {photo_path} not found.")

    async def send_telegram_message_async(self, chat_id: str, message: str):
        """Telegram 메시지 발송"""
        try:
            await self.bot.send_message(chat_id=chat_id, text=message)


            # print(f"Message sent successfully to {chat_id}")
        except TelegramError as e:
            print(f"Error sending message: {e}")
            raise e

    async def retry_on_flood_control(self, func, *args, retry_delay=30, max_retries=3):
        """Flood control 예외 시 지연 후 재시도하는 함수"""
        retries = 0
        while retries < max_retries:
            try:
                return await func(*args)
            except Exception as e:
                if "Flood control exceeded" in str(e):
                    retries += 1
                    print(f"Error: {e}")
                    print(f"Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                    await asyncio.sleep(retry_delay)
                else:
                    raise e
        print("Max retries reached. Message sending aborted.")

    async def message_worker(self):
        """Queue에서 메시지나 사진을 하나씩 꺼내서 발송하는 Worker Task"""
        while self.is_running:
            chat_id, message, photo_path, caption = await self.queue.get()  # asyncio.Queue에서 비동기적으로 메시지 또는 사진 정보 꺼내기
            if photo_path:
                # photo_path가 있는 경우 사진 메시지 발송
                await self.retry_on_flood_control(self.send_telegram_photo_async, chat_id, photo_path, caption)
            else:
                # 텍스트 메시지 발송
                await self.retry_on_flood_control(self.send_telegram_message_async, chat_id, message)
            await asyncio.sleep(0.2)
            self.queue.task_done()  # 작업 완료 알림

    def start(self):
        """Worker Task 시작"""
        self.is_running = True
        return asyncio.create_task(self.message_worker())  # Worker Task 생성

    def stop(self):
        """Worker Task 중지"""
        self.is_running = False

    def send_message(self, chat_id: str, message: str):
        """Queue에 메시지 또는 사진 추가"""
        self.queue.put_nowait((chat_id, message,None,None))

    def send_photo(self, chat_id: str, path: str = None, caption: str = None):
        """Queue에 메시지 또는 사진 추가"""
        self.queue.put_nowait((chat_id, "", path, caption))

    async def wait_until_done(self):
        """Queue의 모든 작업이 완료될 때까지 대기"""
        await self.queue.join()
