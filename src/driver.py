#
# Angel Farguell, CU Denver
#

from job import Job
from ingest.MODIS import Terra,Aqua
from ingest.VIIRS import SNPP

from multiprocessing import Process,Queue
import os.path as osp
import sys,logging,traceback,json

class DriverError(Exception):
    """
    Raised when a Driver produces an error.
    """
    pass

class Driver(object):

    def __init__(self,job_file):
        # create Job class
        self.job = Job(job_file)
        # resolve satellite sources
        self.sat_sources = self.resolve_sat_sources()

    def resolve_sat_sources(self):
        """
        Creates all the SatSource objects from the list of names in the Job object.
        """
        js = self.job
        sat_list = js.get('sat_sources',[])
        sat_objs = []
        if 'Terra' in sat_list:
            sat_objs.append(Terra(js))
        if 'Aqua' in sat_list:
            sat_objs.append(Aqua(js))
        if 'SNPP' in sat_list:
            sat_objs.append(SNPP(js))
        return sat_objs

    def retrieve_sat_data(self):
        """
        This function retrieves all satellite data sources.
        """
        # create queue
        proc_q = Queue()
        sat_proc = {}
        # create process for each sat source
        for sat_source in self.sat_sources:
            sat_proc[sat_source.id] = Process(target=retrieve_sat_source, args=(self.job, sat_source, proc_q))
        # start processes
        for sat_source in self.sat_sources:
            sat_proc[sat_source.id].start()
        # wait processes
        for sat_source in self.sat_sources:
            sat_proc[sat_source.id].join()
        # ensure processes
        for sat_source in self.sat_sources:
            if proc_q.get() != 'SUCCESS':
                return
        # close queue
        proc_q.close()

def retrieve_sat_source(js, sat_source, q):
    """
    This function retrieves satellite data from sat_source.

    It returns either 'SUCCESS' or 'FAILURE' on completion.

    :param js: the Job object
    :param sat_source: the SatSource object
    :param q: the multiprocessing Queue into which we will send either 'SUCCESS' or 'FAILURE'
    """
    try:
        logging.info('retrieve_sat_source - retrieving satellite files from {}'.format(sat_source.id))
        # retrieve satellite granules intersecting the last domain
        manifest = sat_source.retrieve_data()
        # write a json file with satellite information
        sat_file = sat_source.id+'.json'
        json.dump(manifest, open(osp.join(js.job_path,sat_file),'w'), indent=4, separators=(',', ': '))
        logging.info('retrieve_sat_source - satellite retrieval complete for {}'.format(sat_source.id))
        q.put('SUCCESS')

    except Exception as e:
        logging.error('retrieve_sat_source - satellite retrieving step failed with exception {}'.format(repr(e)))
        traceback.print_exc()
        q.put('FAILURE')

if __name__=='__main__':
    # create driver
    dv = Driver(sys.argv[1])
    # retrieve satellite data
    dv.retrieve_sat_data()
    # run ML estimation
