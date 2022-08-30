import datetime
import itertools
import json
import re
import time

import numpy as np

def join_date_time(date, time): #join 01/01/2001 with 01:01 to a  datetime
    """ Joins date and time into a single timestamp, if date exists
    
    Args:
        date (str)
        time (str)
    Returns:
        :obj:`datetime.datetime`
    """

    if not time[0].isdigit(): #get rid of the symbol
        time = time[1:]

    if(date != "--/--/----"):
        return datetime.datetime.strptime(date + " " + time, "%d/%m/%Y %H:%M")
    else:
        return datetime.datetime.strptime(time, "%H:%M").strftime("%H:%M:%S")

def duration_to_sql(duration):
    """ Converts formatted duration string into minutes
    
    Args:
        duration (str)
    Returns:
        int
    """

    minutes = re.search('\d{1,2}(?=m)', duration)
    if minutes is not None:
        minutes = int(minutes.group(0))
    else:
        minutes = 0
    hours = re.search('\d{1,2}(?=h)', duration)
    if hours is not None:
        hours = int(hours.group(0))
    else:
        hours = 0

    final_minutes = 0

    if hours is not None and minutes is None:
        final_minutes = hours*60

    if minutes is not None and hours is None:
        final_minutes = minutes

    if minutes is not None and hours is not None:
        final_minutes = minutes + hours*60

    return final_minutes


def fuzzy_to_sql(duration): 
    """ Parses range values into minutes. Duration is divided in half for query purposes if not specified as ±
    
    Args:
        duration (str)
    Returns:
        int
    """

    if not duration[0].isdigit():
        if duration[0] == '±':
            minutes = int(''.join([x for x in duration if x.isdigit()]))
    else:
        minutes = int(''.join([x for x in duration if x.isdigit()])) / 2.0 #equivalent to js parseInt
        
    minutes = time.strftime("%H:%M:%S", time.gmtime(minutes*60)) #uses seconds
    return minutes

def spatial_range_to_meters(range):
    """ Parses spatial range string
    
    Args: 
        range (str)
    Returns:
        int
    """
    return int(''.join([x for x in range if x.isdigit()]))

def get_symbol(duration): 
    """ Extracts the first char from the duration string (in this case, the symbol)
    
    Args: 
        duration (str)
    Returns:
        str
    """
    if not duration[0].isdigit():
        if duration[0] == '≤':
            return '<='
        elif duration[0] == '≥':
            return '>='
        else:
            return duration[0]
    else:
        return '='

def get_all_but_symbol(duration):
    """ Extracts every char from the duration string excluding the symbol
    
    Args: 
        duration (str)
    Returns:
        str
    """
    result = ""
    if duration.strip() == "":
        return ""
    if not duration[0].isdigit():
        return duration[1:-1]
    else:
        return duration

def is_full_date(date):
    """ Extracts date type
    
    Args:
        date (str)
    Returns:
        str
    """
    if date != "--/--/----":
        return 'TIMESTAMP', ""
    else:
        return 'TIME', "::time"

def is_coordinates(loc):
    """ Uses pattern matching on location string
    
    Args:
        loc (str): coordinates string
    Returns:
        :obj:`re.Match` or None
    """

    return re.match(r'^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$', loc)

def switch_coordinates(loc):
    """ Flips latitude and longitude
    
    Args:
        loc (str): coordinates string
    Returns:
        str
    """
    coords = [x.strip() for x in loc.split(',')]
    return coords[1] + ", " + coords[0]

def represent_int(s):
    """ Tests string to determine if it can be converted into int
    
    Args:
        s (str)
    Returns:
        bool
    """
    try:
        int(s)
        return True
    except ValueError:
        return False

def quartiles(to_show, nr_queries):
    """ Clusters similar results into the same lists

    Args:
        to_show (:obj:`list` of :obj:`dict`): list with query results
        nr_queries (int): total number of ranges/intervals in query
    Returns:
        :obj:`dict` of :obj:`list`
    """
    dict = {}

    for key, value in list(to_show.items()):
        size = len(value)
        startList = []
        endList = []

        startListInterval = []
        endListInterval = []

        for result in value:
            for range in result[::2]:
                startList.append((time.mktime(range.start_date.timetuple()), range.id, range.date, range.points))
                endList.append((time.mktime(range.end_date.timetuple()), range.id, range.date, range.points))
            for interval in result[1::2]:
                startListInterval.append((time.mktime(interval.start_date.timetuple()), interval.id, interval.date, interval.points))
                endListInterval.append((time.mktime(interval.end_date.timetuple()), interval.id, interval.date, interval.points))

        endListOrdered = sorted(endList, key = lambda tup: tup[0])
        startListOrdered = sorted(startList, key = lambda tup: tup[0])

        endListIntervalOrdered = sorted(endListInterval, key = lambda tup: tup[0])
        startListIntervalOrdered = sorted(startListInterval, key = lambda tup: tup[0])


        if size > 4:
            size = 4
        
        endTimes = np.array_split(np.array([x[0] for x in endListOrdered]), size)
        startTimes = np.array_split(np.array([x[0] for x in startListOrdered]), size)

        endTimesInterval = np.array_split(np.array([x[0] for x in endListIntervalOrdered]), size)
        startTimesInterval = np.array_split(np.array([x[0] for x in startListIntervalOrdered]), size)

        if nr_queries == 1:
            tempEnd = endTimes
            tempStart = startTimes
            endTimes = []
            startTimes = []

            for array in tempEnd:
                endTimes.append([sum(array)/len(array)])

            for array in tempStart:
                startTimes.append([sum(array)/len(array)])


        endT = []
        startT = []

        endTI = []
        startTI = []

        for array in endTimes:
            for date in array:
                endT.append(datetime.datetime.fromtimestamp(date))

        for array in startTimes:
            for date in array:
                startT.append(datetime.datetime.fromtimestamp(date))

        for array in endTimesInterval:
            for date in array:
                endTI.append(datetime.datetime.fromtimestamp(date))

        for array in startTimesInterval:
            for date in array:
                startTI.append(datetime.datetime.fromtimestamp(date))

        dict[key] = list(list(zip(startT, endT, [x[1] for x in startListOrdered], [x[2] for x in startListOrdered], [x[3] for x in startListOrdered])) + list(zip(startTI, endTI, [x[1] for x in startListIntervalOrdered],[x[2] for x in startListIntervalOrdered],[x[3] for x in startListIntervalOrdered])))

    return dict



def avg_time(times):
    """ Calculates the average time of a list of times
    
    Args:
        times (:obj:`list` of :obj:`datetime.datetime`)
    Returns:
        :obj:`datetime.datetime`
    """
    avg = 0
    for elem in times:
        avg += elem.second + 60*elem.minute + 3600*elem.hour
    avg /= len(times)
    rez = str(avg/3600) + ' ' + str((avg%3600)/60) + ' ' + str(avg%60)
    return datetime.datetime.strptime(rez, "%H %M %S")

def percentage_split(seq, percentages):
    cdf = np.cumsum(percentages)
    assert cdf[-1] == 1.0
    stops = list(map(int, cdf * len(seq)))
    return [seq[a:b] for a, b in zip([0]+stops, stops)]


def fortnight(date):
    start_date=datetime.datetime.now()
    return (date-start_date).seconds // 3600 // 5

    
class groupby(dict):
    def __init__(self, seq, key=lambda x:x):
        for value in seq:
            k = key(value)
            self.setdefault(k, []).append(value)
    def __iter__(self):
        return iter(self.items())

class groupbyDate(dict):
    def __init__(self, seq, key=lambda x:x):
        for value in seq:
            k = key(value)
            self.setdefault(str(k), []).append(value)
    def __iter__(self):
        return iter(self.items())

def refine_with_group_by(to_show):
    dict = {}


    to_show.sort(key=lambda item: str(item[0].id))

    for elt, items in groupby(to_show, lambda item: item[0].id):
        dict[elt] = []
        for i in items:
            dict[elt].append(i)

    return dict

def refine_with_group_by_date(to_show):
    dict = {}
    id = 0
    for key1, value in list(to_show.items()):
        transactions=value

        transactions.sort(key=lambda r: r[0].end_date)

        for key,grp in itertools.groupby(transactions,key=lambda date:fortnight(date[0].end_date)):
            list1 = list(grp)
            try:
                dict[str(key1)+str(key)] += list1
            except KeyError:
                dict[str(key1)+str(key)] = []
                dict[str(key1)+str(key)] += list1
    return dict

class DateEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)

        if isinstance(obj, datetime.date):
            return str(obj)

        return json.JSONEncoder.default(self, obj)