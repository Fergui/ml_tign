import netCDF4 as nc
import numpy as np
import os.path as osp

class WRFFileError(Exception):
    """
    Raised when a WRFFile failed.
    """
    pass

class WRFFile(object):
    """
    Represents the content of one netCDF WRF file.
    """

    def __init__(self, path):
        """
        Initialize WRFFile object
        """
        if not osp.exists(path):
            raise WRFFileError('WRFFile: path {} provided does not exist'.format(path))
        self.path = path
        self.dataset = nc.Dataset(path)
        self.extra_strip()

    def extra_strip(self):
        """
        Calculate extra strip dimensions
        """
        m,n = self.dataset.variables['XLONG'][0,:,:].shape
        self.m = m
        self.n = n
        fm,fn = self.dataset.variables['FXLONG'][0,:,:].shape
        self.fm = fm-fm//(m+1) # dimensions corrected for extra strip
        self.fn = fn-fn//(n+1)

    def fire_grid(self):
        """
        Read grid from NetCDF4 file
        """
        fxlon = np.array(self.dataset.variables['FXLONG'][0,:self.fm,:self.fn]) #  masking  extra strip
        fxlat = np.array(self.dataset.variables['FXLAT'][0,:self.fm,:self.fn])
        return fxlon,fxlat 

    def atmph_grid(self):
        """
        Read grid from NetCDF4 file
        """
        xlon = np.array(self.dataset.variables['XLONG'][0,:,:])
        xlat = np.array(self.dataset.variables['XLAT'][0,:,:])
        return xlon,xlat 

    def read_var(self, var, ts=None):
        """
        Read grid from NetCDF4 file
        
        :param var: variable name in netCDF file
        :param ts: time step
        """
        if not var in self.dataset.variables.keys():
            raise WRFFileError('WRFFile: variable {} not in file'.format(var))
        nsub = len([dim for dim in d.variables[var].dimensions if 'subgrid' in dim])        
        if nsub == 2:
            if ts is None:
                return np.array(d.variables[var][:,:self.fm,:self.fn])
            else:
                return np.array(d.variables[var][ts,:self.fm,:self.fn])
        else:
            if ts is None:
                return np.array(d.variables[var][:,:,:])
            else:
                return np.array(d.variables[var][ts,:,:])

    def __exit__(self):
        self.dataset.close()

