# setup.py
from setuptools import setup, find_packages

setup(
    name='krx_backtester',  # 패키지 이름
    version='0.2.4.2',  # 패키지 버전
    packages=find_packages(),  # 포함할 패키지 자동 탐색
    install_requires=['pandas','numpy','asyncio','SQLAlchemy','openpyxl','asyncio','python-telegram-bot','psycopg2'],
    author='heshot88',  # 작성자 이름
    author_email='heshot88@gmail.com',  # 작성자 이메일
    description='A backtesting package for KRX data.',  # 패키지 설명
    url='https://github.com/heshot88/krx_backtester',  # 패키지의 URL
    classifiers=[  # 분류 정보
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',  # 지원하는 Python 버전
)
