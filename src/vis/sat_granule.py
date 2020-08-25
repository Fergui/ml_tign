from utils.general import Dict

import numpy as np

class SatGranuleError(Exception):
    """
    Raised when a SatGranule cannot retrieve satellite data.
    """
    pass

class SatGranule(object):
    """
    The parent class of all satellite granules that implements common functionality, for example
    """

    def __init__(self, js, bounds):
        """
        Initialize satellite source with ingest directory (where satellite files are stored).

        :param js: granule information from manifest
        """
        self.manifest = Dict(js)
        self.time_start_iso = self.manifest.get('time_start_iso')
        self.datetime = esmf_to_dt(self.time_start_iso)
        self.time_num = dt_to_num(self.datetime)
        self.acq_date = "{0:04d}-{1:02d}-{2:02d}".format(self.datetime.year,
                                                self.datetime.month,
                                                self.datetime.day)
        self.acq_time = "{0:02d}{1:02d}".format(self.datetime.hour,
                                                self.datetime.minute)
        self.time_end_iso = self.manifest.get('time_end_iso')
        self.file_geo = self.manifest.get('geo_local_path')  
        self.file_fire = self.manifest.get('fire_local_path')  
        self.bounds = bounds

    def read_granule(self):
        geo_ds,geo_ext = open_file(self.manifest.geo_local_path)
        fire_ds,fire_ext = open_file(self.manifest.fire_local_path)
        granule = Dict({})
        for key,field in self.geo_fields:
            granule[key] = self.read_geo_field(geo_ds,field)
        granule['granule_mask'] = self.compute_mask(np.ravel(granule.lat),np.ravel(granule.lon))   
        key,field = self.fire_mask_field
        granule[key] = self.read_field(fire_ds,field)
        for key,field in self.geo_fire_fields:
            granule[key] = self.read_field(fire_ds,field)
        granule['detect_mask'] = self.compute_mask(np.ravel(granule.lat_fire),np.ravel(granule.lon_fire))   
        for key,field in self.fire_fields:
            granule[key] = self.read_field(fire_ds,field,granule['detect_mask'])
        granule.scan_angle_fire,granule.scan_fire,granule.track_fire=self.pixel_dims(granule.sample_fire)
        close_file(geo_ds,geo_ext)
        close_file(fire_ds,fire_ext)
        return granule

    def pixel_dims(self,sample):
        """
        Computes pixel dimensions (along-scan and track pixel sizes)

        :param sample: array of integers with the column number (sample variable in files)
        :return theta: scan angle in radiands
        :return scan: along-scan pixel size in km
        :return track: along-track pixel size in km
        """
        Re = 6378 # approximation of the radius of the Earth in km
        r = Re+self.sat_altitude
        M = (self.num_cols-1)*0.5
        s = np.arctan(self.nadir_pixel_res/self.sat_altitude) # trigonometry (deg/sample)
        alpha = self.angle_changes
        if alpha:
            Ns = np.array([int((alpha[k]-alpha[k-1])/s[k-1]) for k in range(1,len(alpha)-1)])
            Ns = np.append(Ns,int(M-Ns.sum()))
            theta = s[0]*(sample-M)
            scan = Re*s[0]*(np.cos(theta)/np.sqrt((Re/r)**2-np.square(np.sin(theta)))-1)
            track = r*s[0]*(np.cos(theta)-np.sqrt((Re/r)**2-np.square(np.sin(theta))))
            for k in range(1,len(Ns)):
                p = sample<=M-Ns[0:k].sum()
                theta[p] = s[k]*(sample[p]-(M-Ns[0:k].sum()))-(s[0:k]*Ns[0:k]).sum()
                scan[p] = Re*np.mean(s)*(np.cos(theta[p])/np.sqrt((Re/r)**2-np.square(np.sin(theta[p])))-1)
                track[p] = r*np.mean(s)*(np.cos(theta[p])-np.sqrt((Re/r)**2-np.square(np.sin(theta[p]))))
                p = sample>=M+Ns[0:k].sum()
                theta[p] = s[k]*(sample[p]-(M+Ns[0:k].sum()))+(s[0:k]*Ns[0:k]).sum()
                scan[p] = Re*np.mean(s)*(np.cos(theta[p])/np.sqrt((Re/r)**2-np.square(np.sin(theta[p])))-1)
                track[p] = r*np.mean(s)*(np.cos(theta[p])-np.sqrt((Re/r)**2-np.square(np.sin(theta[p]))))
        else:
            theta = s*(sample-M)
            scan = Re*s*(np.cos(theta)/np.sqrt((Re/r)**2-np.square(np.sin(theta)))-1)
            track = r*s*(np.cos(theta)-np.sqrt((Re/r)**2-np.square(np.sin(theta))))
        return (theta,scan,track)

    def compute_mask(self,lats,lons):
        return np.logical_and(np.logical_and(np.logical_and(lons >= self.bounds[0], lons <= self.bounds[1]), lats >= self.bounds[2]), lats <= self.bounds[3])

    # instance variables
    info_url=None
    info=None
    prefix=None
    platform=None
    num_cols=None
    sat_altitude=None
    nadir_pixel_res=None
    angle_changes=None
    geo_fields=[('lat','Latitude'),
                ('lon','Longitude')]
    geo_fire_fields=[('lat_fire','FP_latitude'),
                    ('lon_fire','FP_longitude')]
    fire_mask_field=('fire','fire mask')
    fire_fields=None



class MODISGranule(SatGranule):
    """
    MODIS (Moderate Resolution Imaging Spectroradiometer) granule.
    """
    def __init__(self, arg):
        super(MODISGranule, self).__init__(arg)

    def read_geo_field(self,ds,field):
        return self.read_field(ds,field) 

    @staticmethod
    def read_field(ds,field,mask = None):
        try: 
            if mask:
                return np.array(ds.select(field).get())[mask]
            else:
                return np.array(ds.select(field).get())
        else:
            return np.array([])  

    # instance variables
    num_cols=1354
    sat_altitude=705.
    nadir_pixel_res=1.
    fire_fields=[('brig_fire','FP_T21'),
                ('sample_fire','FP_sample'),
                ('conf_fire','FP_confidence'),
                ('t31_fire','FP_T31'),
                ('frp_fire','FP_power')]

class TerraGranule(MODISGranule):
    """
    Terra MODIS (Moderate Resolution Imaging Spectroradiometer) granule.
    """
    def __init__(self, arg):
        super(TerraGranule, self).__init__(arg)

    # instance variables
    info_url='https://terra.nasa.gov/about/terra-instruments/modis'
    info='Terra Moderate Resolution Imaging Spectroradiometer (MODIS)'
    prefix='MOD'
    platform='Terra'

class AquaGranule(MODISGranule):
    """
    Aqua MODIS (Moderate Resolution Imaging Spectroradiometer) granule.
    """
    def __init__(self, arg):
        super(AquaGranule, self).__init__(arg)

    # instance variables
    info_url='https://aqua.nasa.gov/modis'
    info='Aqua Moderate Resolution Imaging Spectroradiometer (MODIS)'
    prefix='MYD'
    platform='Aqua'


class VIIRSGranule(SatGranule):
    """
    VIIRS (Visible Infrared Imaging Radiometer Suite) satellite source.
    """
    def __init__(self, arg):
        super(VIIRSGranule, self).__init__(arg)

    @staticmethod
    def read_geo_field(ds,field):
        return np.array(ds['HDFEOS']['SWATHS']['VNP_750M_GEOLOCATION']['Geolocation Fields'][field]) 

    @staticmethod
    def read_field(ds,field,mask = None):
        try: 
            if mask:
                return np.array(ds.variables[field][:])[mask]
            else:
                return np.array(ds.variables[field][:])
        else:
            return np.array([]) 

    # instance variables
    num_cols=3200
    sat_altitude=828.
    nadir_pixel_res=np.array([0.75,0.75/2,0.75/3])
    angle_changes=np.array([0,31.59,44.68,56.06])/180*np.pi
    fire_fields=[('brig_fire','FP_T13'),
                ('sample_fire','FP_sample'),
                ('conf_fire','FP_confidence'),
                ('t31_fire','FP_T15'),
                ('frp_fire','FP_power')]

class SNPPGranule(VIIRSGranule):
    """
    S-NPP VIIRS (Visible Infrared Imaging Radiometer Suite) satellite source.
    """
    def __init__(self, arg):
        super(SNPPGranule, self).__init__(arg)
    
    # instance variables
    info_url='https://www.nasa.gov/mission_pages/NPP/mission_overview/index.html'
    info='S-NPP Visible Infrared Imaging Radiometer Suite (VIIRS)'
    prefix='VNP'
    platform='S-NPP'


def open_file(path_file):
    """
    Open file depending on its extension

    :param path_file: local path to the file
    """
    path_file = str(path_file)
    if not osp.exists(path_file):
        logging.error('open_file: file %s does not exist locally' % path_file)
        return
    ext = osp.splitext(path_file)[1]
    logging.info('open_file: open file %s with extension %s' % (path_file, ext))
    if ext == ".nc":
        try:
            d = nc4.Dataset(path_file,'r')
        except Exception as e:
            logging.error('open_file: can not open file %s with exception %s' % (path_file,e))
    elif ext == ".hdf":
        try:
            d = SD(path_file,SDC.READ)
        except Exception as e:
            logging.error('open_file: can not open file %s with exception %s' % (path_file,e))
            sys.exit(1)
    elif ext == ".h5":
        try:
            d = h5py.File(path_file,'r')
        except Exception as e:
            logging.error('open_file: can not open file %s with exception %s' % (path_file,e))
    else:
        logging.error('open_file: unrecognized extension %s' % ext)
        return
    return d,ext

def close_file(d, ext):
    """
    Close file depending on its extension

    :param d: open file to close
    :param ext: extension of the file
    """
    if ext == ".nc":
        d.close()
    elif ext == ".hdf":
        d.end()
    elif ext == ".h5":
        d.close()
    else:
        logging.error('close_file: unrecognized extension %s' % ext)
