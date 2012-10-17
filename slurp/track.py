"""
Trackers used to record the progress a channel has made in processing sources.
"""
import logging
import sqlite3


logger = logging.getLogger(__name__)


class Tracker(object):
    """
    SQLite backed progress tracking.

    `db_path`
        Path to file where SQLite database file is stored.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        logger.debug('connecting to "%s"', self.db_path)
        self.cxn = sqlite3.connect(db_path)
        self._init()

    def _init(self):
        cur = self.cxn.cursor()
        cur = cur.execute("""\
            CREATE TABLE IF NOT EXISTS tracks (
                channel TEXT,
                path TEXT,
                offset INTEGER,
                PRIMARY KEY (channel, path)
                )
             """)
        self.cxn.commit()
        cur.close()

    def get(self, channel, event):
        cur = self.cxn.cursor()
        cur.execute("""
            SELECT offset
            FROM tracks
            WHERE channel = ? AND path = ?
            """,
            (channel.name, event.path))
        track = cur.fetchone()
        offset = track[0] if track else None
        cur.close()
        return offset

    def set(self, channel, event, offset):
        cur = self.cxn.cursor()
        cur.execute("""
            UPDATE tracks
            SET offset = ?
            WHERE channel = ? AND path = ?
            """,
            (offset, channel.name, event.path))
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO tracks
                (channel, path, offset)
                VALUES
                (?, ?, ?)
                """,
                (channel.name, event.path, offset))
        logger.debug('track ("%s", "%s") offset %s',
            channel.name, event.path, offset)
        self.cxn.commit()
        cur.close()

    def delete(self, channel, event):
        cur = self.cxn.cursor()
        cur.execute("""
            DELETE FROM tracks
            WHERE channel = ? AND path = ?
            """,
            (channel.name, event.path))
        if cur.rowcount != 0:
            logger.debug('track ("%s", "%s") deleted',
                channel.name, event.path)
        cur.close()

    def delete_prefix(self, prefix):
        cur = self.cxn.cursor()
        cur.execute("""
            DELETE FROM tracks
            WHERE path LIKE ?
            """,
            (prefix + '%',))
        if cur.rowcount != 0:
            logger.debug('track (*, "%s*") deleted', prefix)
        self.cxn.commit()
        cur.close()


class DummyTracker(object):
    """
    Dummy/NOP progress tracking.
    """

    def get(self, channel, event):
        return None

    def set(self, channel, event, offset):
        pass

    def delete(self, channel, event):
        pass

    def delete_prefix(self, prefix):
        pass
