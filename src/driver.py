#
# Angel Farguell, CU Denver
#

from job import Job
from ingest.MODIS import Terra,Aqua
from ingest.VIIRS import SNPP

class DriverError(Exception):
    """
    Raised when a Driver produces an error.
    """
    pass

class Driver(object):

    def __init__(self,job_file):
        # create Job class
        self.job = Job(job_file)
        # satellite sources
        self.sat_sources = self.resolve_sat_sources(self.job)

    @staticmethod
    def resolve_sat_sources(js):
        """
        Creates all the SatSource objects from the list of names.

        :param js: configuration json
        """
        sat_list = js.get('sat_sources',[])
        sat_objs = []
        if 'Terra' in sat_list:
            sat_objs.append(Terra(js))
        if 'Aqua' in sat_list:
            sat_objs.append(Aqua(js))
        if 'SNPP' in sat_list:
            sat_objs.append(SNPP(js))
        return sat_objs

if __name__=='__main__':
    dv = Driver(sys.argv[1])
    print(dir(dv))
