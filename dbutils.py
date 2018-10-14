from contextlib import contextmanager
import datetime

from sqlalchemy import (
    Column, DateTime, Integer, DDL
)
from sqlalchemy.engine.base import Engine
import sqlalchemy


def pkey(id_str, dtype=Integer):
    return Column(
        id_str,
        dtype,
        autoincrement=True,
        primary_key=True,
    )

def datetime_col(colname):
    return Column(
        colname,
        DateTime,
        nullable=False,
        default="now()",
    )


def create_tables(engine, SABase, schema_name):
    engine.execute(DDL('CREATE SCHEMA IF NOT EXISTS {schema}'.format(
        schema=schema_name,
    )))
    SABase.metadata.create_all()


def drop_tables(SABase):
    SABase.metadata.drop_all()

class Base:
    created_at = datetime_col('created_at')
    modified_at = datetime_col('modified_at')
    Session = None

    def __init__(self, time=None):
        if time is None:
            time = datetime.datetime.now().replace(microsecond=0)
        self.created_at = self.modified_at = time

    @staticmethod
    def get_col_name(col):
        return str(col).split('.')[-1]

    @classmethod
    def get_row(cls, col, value, sess):
        query = sess.query(cls).filter(col==value)

        # if it exists, then return it
        row = query.one_or_none()
        if row is not None:
            return row

        # otherwise, create one
        row = cls()
        setattr(
            row,
            cls.get_col_name(col),
            value
        )

        # make sure the row is entered into the db and
        # has its id field populated so its id can be
        # referenced
        sess.add(row)
        sess.commit()

        return row

    Session = None
    @classmethod
    def set_sess(cls, session_or_engine):
        if isinstance(session_or_engine, Engine):
            cls.Session = sqlalchemy.orm.sessionmaker(bind=session_or_engine)
        else:
            cls.Session = session_or_engine

    @classmethod
    @contextmanager
    def get_session(cls, sess=None) -> sqlalchemy.orm.session.Session:
        """

        Note that operations that create rows in
        multiple tables at once need to share
        the same session.
        """
        if sess is None:
            managed = True
            if cls.Session is None:
                raise Exception(
                    "session not set. must be set using Base.set_sess(sqlalchemy.orm.sessionmaker(bind=engine))"
                )
            sess = cls.Session()
        else:
            managed = False
        try:
            yield sess
        except KeyboardInterrupt:
            raise
        except Exception:
            # TODO not sure whether unmanaged session should be rolled back
            sess.rollback()
        else:
            if managed:
                sess.commit()
        finally:
            if managed:
                sess.close()

    def __repr__(self):
        """Generic repr method for 
        """
        attrs = list()
        for k in self.__init__.__code__.co_varnames[1:]:
            if not hasattr(self, k):
                continue
            attrs.append('{}={}'.format(
                k, repr(getattr(self, k))
            ))
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(attrs),
        )

    def __str__(self):
        return repr(self)
