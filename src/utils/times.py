#
# Angel Farguell, CU Denver
#

from datetime import datetime
import pytz, logging

def utc_now():
    # UTC datetime now
    dnow = datetime.utcnow()
    return dnow

def local_now():
    # local datetime now
    dnow = datetime.now()
    return dnow

def dt_to_str(dt,fmt="%04d-%02d-%02d_%02d:%02d:%02d"):
    """
    Converts a UTC datetime into UTC string format.

    :param utc: python UTC datetime
    :param fmt: optional, string format following (year,month,day,hour,minute,second)
                default %Y-%m-%dT%H:%M:%SZ 
    :return: a string in UTC string format %Y-%m-%dT%H:%M:%SZ
    """
    if not isinstance(dt,datetime):
        logging.error('dt_to_str - input must be a dataframe')
        return None
    return fmt % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

def dt_to_esmf(dt):
    """
    Converts a UTC datetime into UTC string format %Y-%m-%dT%H:%M:%SZ.

    :param utc: python UTC datetime
    :return: a string in UTC string format %Y-%m-%dT%H:%M:%SZ
    """
    return dt_to_str(dt,fmt="%04d-%02d-%02dT%02d:%02d:%02dZ")

def str_to_dt(t_str,fmt="%Y-%m-%d_%H:%M:%S"):
    """
    Converts an ESMF datetime into a UTC datetime.

    :param t_str: string date & time using format fmt 
                default ESMF format (YYYY-MM-DD_hh:mm:ss)
    :return: a python datetime with UTC timezone
    """
    if not isinstance(t_str,str):
        logging.error('str_to_dt - input must be an string')
        return None
    return datetime.strptime(t_str,fmt).replace(tzinfo=pytz.UTC)

def esmf_to_dt(t_str):
    """
    Converts an ESMF datetime into a UTC datetime.

    :param t_str: string date & time using format fmt 
                default ESMF format (YYYY-MM-DD_hh:mm:ss)
    :return: a python datetime with UTC timezone
    """
    return str_to_dt(t_str,fmt="%Y-%m-%dT%H:%M:%SZ")

def esmf_now():
    # ESMF datetime now
    return dt_to_str(local_now())
