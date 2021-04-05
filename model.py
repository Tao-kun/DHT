import asyncio

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Torrent(Base):
    __tablename__="torrent"
    info_hash=Column(String(40),primary_key=True)
    name=Column(String(2047))
    size=Column(Integer)

class AnnounceQueueL:
    __tablename__="announce_queue"
    info_hash=Column(String(40),primary_key=True)
    ip_addr=Column(String(100),primary_key=True)
    port=Column(Integer,primary_key=True)
    lock=Column(Integer, server_default=0)
    insert_time=Column(TIMESTAMP,server_default=func.now())