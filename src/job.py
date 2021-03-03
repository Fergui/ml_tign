#
# Angel Farguell, CU Denver
#

from utils.general import Dict, load_sys_cfg, make_dir, process_arguments, process_bounds
from utils.times import esmf_now, str_to_dt

import logging,json
import os.path as osp

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
        make_dir(self.job_path)	
        # save job state in work directory
        json.dump(self, open(osp.join(self.job_path,'job.json'),'w'), indent=4, separators=(',', ': '))
        # add new attributes
        self.bounds = process_bounds(self.bbox) 
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
