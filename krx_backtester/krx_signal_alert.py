import asyncio

from krx_backtester.krx_telegram import TelegramSender

TOKEN = '6588514172:AAH5hED9lPuPcMB7VJ8pHvWFWSWQya5aj80'
CHAT_ID = '-1002209543022'


async def main():
    telegram_sender = TelegramSender(TOKEN)
    telegram_sender.start()

    await telegram_sender.wait_until_done()
    telegram_sender.stop()


if __name__ == "__main__":
    asyncio.run(main())
