import requests as req
import pandas as pd

import sqlalchemy as sql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData


def connect(db_vers='current'):
    """
    Connect to the childes-db server and return an SQLAlchemy
    engine object

    :param db_vers:
    :return:
    """
    resp = req.get("https://childes-db.stanford.edu/childes-db.json")
    if resp.ok:
        db_info = resp.json()
    else:
        raise Exception("database info lookup failed")

    eng = create_engine("mysql+pymysql://{}:{}@{}/{}".format(
        db_info['user'],
        db_info['password'],
        db_info['host'],
        'childesdb'
    ), echo=True)

    return eng


def get_table(conn=None, table_name=None):
    """
    Given an SQL connection to childes-db, look up a table by name
    and return it as a pandas dataframe

    :param conn: sql connection (i.e. SQLAlchemy engine object)
    :param table_name: name of the table you want to pull
    :return: pandas dataframe of that table
    """
    if not conn:
        conn = connect()
    return pd.read_sql('SELECT * FROM {}'.format(table_name), con=conn)


def get_collections(conn=None):
    """
    Get the collections.
    :param conn: an sql connection
    :return: pandas dataframe of collections
    """
    df = get_table(conn=conn, table_name="collection")
    return df


def get_corpora(conn=None):
    """
    Get the list of corpora.

    :param conn: an sql connection
    :return: dataframe of corpora
    """
    df = get_table(conn=conn, table_name="corpus")
    return df


def get_transcripts(conn=None, collection=None,
                    corpus=None, target_child=None):
    """
    Get transcript data

    :param conn: an sql connection
    :param collection: a list of collections to subsample
    :param corpus: a list of corpora to subsample
    :param target_child: a list of target children to subsample
    :return: dataframe of transcripts, filtered by supplied arguments
    """
    df = get_table(conn, 'transcript')

    if collection:
        df = df[df['collection_name'].isin(collection)]
    if corpus:
        df = df[df['corpus_name'].isin(corpus)]
    if target_child:
        df = df[df['target_child_name'].isin(target_child)]

    return df


get_transcripts(corpus=None)
