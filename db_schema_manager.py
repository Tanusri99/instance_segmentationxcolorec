import sqlite3

def get_db_schema_version(cursor: sqlite3.Cursor) -> int:
    """Fetches the user_version of the sqlite schema.
    https://www.sqlite.org/pragma.html#pragma_user_version

    Args:
        cursor (sqlite3.Cursor): a cursor of a connection to the desired sqlite db

    Returns:
        int: the version of the schema
    """
    
    cursor.execute("PRAGMA main.user_version;")
    
    return int(cursor.fetchone()[0])

def initialise_sqlite_db(db_name: str) -> None:
    """Initialises the db & automatically updates the schema to the latest version."""
    
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    
    # upgrade to version==1
    if get_db_schema_version(cursor) == 0:
        
        sources_table = """CREATE TABLE IF NOT EXISTS Sources (
                camera_id TEXT NOT NULL PRIMARY KEY,
                camera_name TEXT,
                detect_flag INTEGER,
                detect_con_threshold REAL,
                detect_kafka_topic TEXT NOT NULL,
                rtsp_address TEXT NOT NULL UNIQUE,
                source_type TEXT NOT NULL,
                detect_objects TEXT);"""

        # Create table 'Sessions' to record nx session token
        session_table = """CREATE TABLE IF NOT EXISTS Sessions(token TEXT);"""
                            
        # Create table 'UUIDS'
        uuids_table = """CREATE TABLE IF NOT EXISTS UUIDS(
                            UUID_ TEXT NOT NULL
                        );"""

        cursor.execute("PRAGMA main.user_version = 1;")

        cursor.execute(sources_table)
        cursor.execute(session_table)
        cursor.execute(uuids_table)
        
        connection.commit()
    
    # clear tables
    cursor.execute("DELETE FROM Sources")
    cursor.execute("DELETE FROM Sessions")
    connection.commit()

    # upgrade to version 2
    if get_db_schema_version(cursor) == 1:
        cursor.execute("PRAGMA main.user_version = 2;")
        
        connection.commit()
    
    connection.commit()
    cursor.close()
    connection.close()