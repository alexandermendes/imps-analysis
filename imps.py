# -*- coding: utf8 -*-
"""A script to analyse the IMPS database."""

import re
import sys
import csv
import codecs
import argparse
import cStringIO


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Analysis(object):

    def __init__(self):
        self.headers = [
            "Shelfmark",
            "Order Frequency",
            "Probable Status",
            "Title",
            "Author",
            "PubDate",
            "DM URL"
        ]
        self.data = {}

    def _probably_partially_digitised(self, row):
        """Estimate if a row relates to a partially digitised item."""
        patterns = ['\d+', 'only']
        pages_folio_col = row[6].lower()
        for p in patterns:
            if re.search(p, pages_folio_col):
                return True
        return False

    def _probably_fully_digitised(self, row):
        """Estimate if a row relates to a fully digitised item."""
        patterns = ['all', 'whole', 'entire', 'full', 'everything', 'complete']
        pages_folio_col = row[6].lower()
        for p in patterns:
            if re.search(p, pages_folio_col):
                return True
        return False

    def _get_digitisation_status(self, row):
        """Return digitisation status."""
        if self._probably_fully_digitised(row):
            return "Full"
        elif self._probably_partially_digitised(row):
            return "Partial"
        return "Unknown"

    def _get_dm_url(self, sm):
        baseURL = "http://www.bl.uk/manuscripts/FullDisplay.aspx"
        try:
            return "{0}?ref={1}".format(baseURL, sm.replace(" ", "_"))
        except Exception as e:
            print e

    def add(self, row):
        sm = row[2]
        title = row[3]
        author = row[4]
        pubdate = row[5]
        status = self._get_digitisation_status(row)

        r = self.data.get(sm)
        if not r:
            r = ["" for h in self.headers]
            r[0] = sm
            r[1] = 0

        r[1] = r[1] + 1  # Update order frequency

        if not r[2] or status == "Full":  # Prefer full digitisation
            r[2] = status
            r[3] = title
            r[4] = author
            r[5] = pubdate
            r[6] = self._get_dm_url(sm)
        self.data[sm] = r


def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8', errors='replace') for cell in row]


def run(csv_path):
    analysis = Analysis()
    with open(csv_path, 'rb') as f:
        reader = unicode_csv_reader(f, delimiter="|")
        for i, row in enumerate(reader):
            analysis.add(row)

    with open('analysis.csv', 'wb') as f:
        writer = UnicodeWriter(f)
        writer.writerow(analysis.headers)
        for row in analysis.data.values():
            r = [unicode(s) for s in row]
            writer.writerow(r)


if __name__ == "__main__":
    csv_path = sys.argv[1]
    run(csv_path)
