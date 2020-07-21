#
# Angel Farguell, CU Denver
#

import logging, sys
from job import Job
from ingest.MODIS import Terra, Aqua
from ingest.VIIRS import SNPP, SNPPHR, NOAA20

if __name__ == '__main__':
    # create job
    jb = Job(sys.argv[1])
    # sat sources
    sat_sources = list(jb.sat_sources)

    # create satellite classes
    logging.info('Retrieving all the satellite data in:') 
    logging.info('* Bounding box (%s,%s,%s,%s), and' % jb.bounds)
    logging.info('* Time interval (%s,%s)' % (jb.start_utc, jb.end_utc))
    if 'Terra' in sat_sources:
        logging.info('>> MODIS Terra <<')
        terra=Terra(jb)
        # retrieve granules
        m_terra=terra.retrieve_data()
    if 'Aqua' in sat_sources:
        logging.info('>> MODIS Aqua <<')
        aqua=Aqua(jb)
        # retrieve granules
        m_aqua=aqua.retrieve_data()
    if 'SNPP' in sat_sources:
        logging.info('>> S-NPP VIIRS <<')
        snpp=SNPP(jb)
        # retrieve granules
        m_snpp=snpp.retrieve_data()
    if 'SNPP_HR' in sat_sources:
        logging.info('>> High resolution S-NPP VIIRS <<')
        snpphr=SNPPHR(jb)
        # retrieve granules
        m_snpphr=snpphr.retrieve_data()
        print(m_snpphr)
    if 'NOAA-20' in sat_sources:
        logging.info('>> NOAA-20 VIIRS <<')
        noaa20=NOAA20(jb)
        # retrieve granules
        m_noaa20=noaa20.retrieve_data()

