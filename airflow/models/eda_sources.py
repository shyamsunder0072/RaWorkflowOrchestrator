import enum
from urllib.parse import urlparse, urlunparse
from airflow.models.base import Base
from airflow.utils.db import provide_session
from sqlalchemy import Column, Text, Enum, Integer


class EdaSourcesEnum(enum.Enum):
    hdfs = "hdfs"
    database = "db"


class EdaSource(Base):
    __tablename__ = "eda_sources"

    id = Column(Integer, primary_key=True)
    connection_uri = Column(Text)
    # Source type defined in EdaSourcesEnum.
    source_type = Column(Enum(EdaSourcesEnum), default=EdaSourcesEnum.hdfs)
    # tablename only valid for source_type = db
    tablename = Column(Text)

    def __repr__(self):
        if self.tablename:
            return "{},  table={}".format(
                self.masked_connection_uri,
                self.tablename or '')
        else:
            return self.masked_connection_uri

    @property
    def masked_connection_uri(self):
        parsed = urlparse(self.connection_uri)
        if parsed.password:
            return urlunparse(parsed._replace(
                netloc="{}:{}@{}".format(parsed.username, "****", parsed.hostname)))
        return self.connection_uri

    @classmethod
    @provide_session
    def get(cls,
            connection_uri,
            source_type,
            tablename=None,
            session=None):
        instance = session.query(cls).filter(cls.connection_uri == connection_uri,
                                             cls.source_type == source_type,
                                             cls.tablename == tablename).first()

        return instance

    @classmethod
    @provide_session
    def get_by_id(cls,
                  source_id,
                  session=None):
        instance = session.query(cls).filter(cls.id == source_id).first()

        return instance

    @classmethod
    @provide_session
    def all_sources(cls, session=None):
        return session.query(cls).all()

    @classmethod
    @provide_session
    def add_source(cls,
                   connection_uri,
                   source_type,
                   tablename=None,
                   session=None):
        if not EdaSource.get(connection_uri,
                             source_type,
                             tablename):
            session.add(EdaSource(connection_uri=connection_uri,
                                  source_type=source_type,
                                  tablename=tablename))
            session.flush()

    @classmethod
    @provide_session
    def remove_source(cls, source, session=None):
        session.query(cls).filter(cls.id == source).delete()
