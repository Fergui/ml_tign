from utils.general import json_join
import utils.saveload as sl
from vis.sat_granule import TerraGranule,AquaGranule,SNPPGranule

import os.path as osp
import logging,sys

class SatCollectionError(Exception):
    """
    Raised when a SatCollection cannot retrieve satellite data.
    """
    pass

class SatCollection(object):
    """
    The parent class of all satellite collection that implements common functionality
    """

    def __init__(self, js):
        """
        Initialize satellite collection from a job.

        :param js: Job object. 
        """
        self.manifest = js.manifest
        self.job_path = js.job_path
        self.bounds = js.bounds
        self.sat_sources = [key for key in self.manifest.keys() if self.manifest[key]]

    def process_data(self):
        granules = {}
        for source in self.sat_sources:
            logging.info('SatCollection.process_data - processing sat source {}'.format(source))
            sys.stdout.flush()
            if source == 'Terra':
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    sys.stdout.flush()
                    granules.update({'MOD_'+key: TerraGranule(granule,self.bounds).read_granule()})
            elif source == 'Aqua':
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    sys.stdout.flush()
                    granules.update({'MYD_'+key: AquaGranule(granule,self.bounds).read_granule()})
            elif source == 'SNPP':
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    sys.stdout.flush()
                    granules.update({'VNP_'+key: SNPPGranule(granule,self.bounds).read_granule()})
            else:
                logging.warning('SatCollection.process_data: sat source {} not existent'.format(source))
        logging.info('SatCollection.process_data: granules proccesed {}'.format(list(granules.keys())))
        sat_file = osp.join(self.job_path,'satdata')
        sl.save(granules,sat_file)
        logging.info('SatCollection.process_data: satellite data processed as {}'.format(sat_file))
        return granules
