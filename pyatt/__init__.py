import sys
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

DISTINCT_TASKS_QUERY = """
SELECT COUNT(name), name
FROM tasks
GROUP BY name
HAVING COUNT(name) > 1"""

TASKS_QUERY = """
SELECT name
FROM tasks"""

CATEGORY_SEP = '/'

def android_localized_collation(s0, s1):
    if s0 == s1:
        return 0
    elif s0 < s1:
        return -1
    else:
        return 1

def dbconnect(att_db_fp):
    con = sqlite3.connect(att_db_fp)
    con.create_collation('LOCALIZED', android_localized_collation)
    con.row_factory = sqlite3.Row
    return con

def att_db_fp_get_category_efforts(categories, att_db_fp, start, end):
    query = CATEGORY_EFFORTS_QUERY
    con = dbconnect(att_db_fp)
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
        for att_db_fp in iglob(join(path, '*'+FILE_EXT)):
            for ctg, eff in att_db_fp_get_category_efforts(categories,
                    att_db_fp, start, end):
                efforts[ctg] = efforts.get(ctg, timedelta()) + eff

    return efforts.items()

def validate_categories_distinct(att_db_fp):
    MSGTMPL = 'Duplicate tasks with subject `{}` in file `{}`'
    query = DISTINCT_TASKS_QUERY
    con = dbconnect(att_db_fp)
    for row in con.execute(query):
        print(MSGTMPL.format(row['name'], att_db_fp), file=sys.stderr)
        return False
    return True

def validate_task_format(att_db_fp):
    msgtmpl = 'Task with name `{}` in file `{}` has wrong format'
    query = TASKS_QUERY
    con = dbconnect(att_db_fp)
    for row in con.execute(query):
        category = row['name'].split(',')[0].strip()
        if not category.endswith(CATEGORY_SEP):
            print(msgtmpl.format(row['name'], att_db_fp), file=sys.stderr)
            return False
    return True

def validate(cfgdirs):
    for cfgdir in cfgdirs:
        for att_db_fp in iglob(join(cfgdir, '*'+FILE_EXT)):
            if not validate_categories_distinct(att_db_fp):
                return False
            if not validate_task_format(att_db_fp):
                return False
    return True
