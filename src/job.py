#
# Angel Farguell, CU Denver
#

import logging
import os.path as osp

from utils.general import Dict, load_sys_cfg, process_arguments
from utils.times import esmf_now, str_to_dt

class JobError(Exception):
    """
    Raised when a Job produces an error.
    """
    pass

class Job(Dict):

    def __init__(self,job_file):
        # configure the basic logger
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # load sys configuration
        sys_cfg = load_sys_cfg()
        # process arguments
        self.update(process_arguments(sys_cfg,job_file))
        # define job name
        self.job_name = self.case_name+'_'+esmf_now()
        # job path
        self.job_path = osp.join(self.workspace_path,self.job_name)
        # add new attributes
        self.bounds = tuple(self.bbox) 
        self.from_utc = str_to_dt(self.start_utc)
        self.to_utc = str_to_dt(self.end_utc)
        self.times = (self.from_utc,self.to_utc)
        # verify inputs
        verify_inputs(self)

    def __str__(self):
        return self.job_name

def verify_inputs(args):
    if None:
        raise JobError('error message')
    return

if __name__=='__main__':
    import sys 
    job = Job(sys.argv[1])
    print(job)
