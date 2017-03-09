import sqlite3
from datetime import timedelta, datetime
from os.path import join
from glob import iglob

FILE_EXT = '.db'

CATEGORY_EFFORTS_QUERY = """
SELECT t.name 'task_name', r.start 'start', r.end 'end'
FROM ranges r INNER JOIN tasks t
ON r.task_id = t._id
WHERE LOWER(t.name) LIKE ? AND r.start >= ? AND r.end <= ?
ORDER BY start"""

CATEGORY_SEP = '/'

def _att_file_get_category_efforts(categories, attf, start, end):
    query = CATEGORY_EFFORTS_QUERY
    con = sqlite3.connect(attf)
    con.row_factory = sqlite3.Row
    for ctg in categories:
        ctg_sel = ctg + CATEGORY_SEP + '%'
        effort_time = timedelta()
        args = (ctg_sel, start.timestamp()*1000, end.timestamp()*1000)
        for row in con.execute(query, args):
            tstart = datetime.fromtimestamp(row['start']/1000)
            tend = datetime.fromtimestamp(row['end']/1000)
            effort_time += (tend - tstart)
        yield (ctg, effort_time)

def get_category_efforts(categories, start, end, paths):
    efforts = {}
    for path in paths:
        for attf in iglob(join(path, '*'+FILE_EXT)):
            for ctg, eff in _att_file_get_category_efforts(categories, attf,
                    start, end):
                efforts[ctg] = efforts.get(ctg, timedelta()) + eff

    return efforts.items()

def validate(cfgdirs):
    '''Implement validation later'''
    return True
