from utils.general import Dict
import utils.saveload as sl
from vis.sat_granules import TerraGranule,AquaGranule,SNPPGranule

import os.path as osp

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
        self.manifest = json_join(js.job_path, js.sat_sources)
        self.job_path = js.job_path
        self.bounds = js.bounds
        self.sat_sources = [key for key in js.keys() if js[key]]

    def process_data(self):
        granules = Dict({})
        for source in self.sat_sources:
            logging.info('SatCollection.process_data - processing sat source {}'.format(source))
            if source == 'Terra':
                granules['MOD-'+key] = Dict({}) 
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    granules['MOD-'+key].update(TerraGranule(granule,self.bounds))
            elif source == 'Aqua':
                granules['MYD-'+key] = Dict({}) 
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    granules['MYD-'+key].update(AquaGranule(granule,self.bounds))
            elif source == 'SNPP':
                granules['SNPP-'+key] = Dict({}) 
                for key,granule in self.manifest[source].items():
                    logging.info('SatCollection.process_data - processing granule {}'.format(key))
                    granules['SNPP-'+key].update(SNPPGranule(granule,self.bounds))
            else:
                logging.warning('process_data: sat source {} not existent'.format(source))
        sl.save(granules,osp.join(self.job_path,'satdata'))
        return granules
