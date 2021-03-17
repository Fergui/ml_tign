import imblearn
import sklearn
import numpy as np
import logging
import pickle
import psutil
from collections import Counter
from scipy import interpolate

def make_meshgrid(n):
    nx, ny, nz = n
    gx, gy, gz = np.meshgrid(np.linspace(0., 1., ny),
                         np.linspace(0., 1., nx),
                         np.linspace(0., 1., nz))
    return gx, gy, gz

def find_roots(Fx,Fy,zr,Z):
    Fz = np.zeros(Fx.shape)
    for k1 in range(Fx.shape[0]):
        for k2 in range(Fx.shape[1]):
            pz = interpolate.CubicSpline(zr, Z[k1,k2])
            rr = pz.roots()
            realr = rr.real[np.logical_and(abs(rr.imag) < 1e-5, np.logical_and(rr.real > zr.min(), rr.real < zr.max()))]
            if len(realr) > 0:
                Fz[k1,k2] = realr.min()
            else:
                Fz[k1,k2] = np.nan
    return Fz

class SVM(object):
    def __init__(self, param_grid = {}):
        logspace = np.logspace(-2.,2.,9)
        self.param_grid = param_grid if len(param_grid) else {'C': logspace, 'gamma': logspace} 
        self.total_cache = int(psutil.virtual_memory().total)/float(1<<20) # in MB
        self.model = sklearn.svm.SVC(C=1.,class_weight="balanced",cache_size=self.total_cache//2)
        logging.info('SVM - {}'.format(self.model))
        self.nproc = psutil.cpu_count()

    def preprocess(self, X, y, sample_weight=None):
        logging.info('SVM.preprocess - {}'.format(Counter(y)))
        logging.info('SVM.preprocess - performing MinMaxScaler')
        X = np.ascontiguousarray(X)
        Xs = sklearn.preprocessing.MinMaxScaler().fit(X).transform(X)
        logging.info('SVM.preprocess - performing OneSidedSelection')
        oss = imblearn.under_sampling.OneSidedSelection(sampling_strategy='majority', n_neighbors=1, n_seeds_S=10000, n_jobs=-2)
        X_oss, y_oss = oss.fit_resample(Xs, y)
        counter = Counter(y_oss)
        logging.info('SVM.preprocess - {}'.format(counter))
        prop = min(counter.values())/max(counter.values())
        if prop > 0 and prop < 1:
            logging.info('SVM.preprocess - performing extra RandomUnderSampler')
            rus = imblearn.under_sampling.RandomUnderSampler(sampling_strategy=.4)
            _,y_rus = rus.fit_resample(X_oss, y_oss) 
            logging.info('SVM.preprocess - {}'.format(Counter(y_rus)))
            self.sample_indices = oss.sample_indices_[rus.sample_indices_]
        else:
            self.sample_indices = oss.sample_indices_
        X = X[self.sample_indices,:]
        y = y[self.sample_indices]
        self.scaler = sklearn.preprocessing.MinMaxScaler().fit(X)
        return self.scaler.transform(X),y

    def fit(self, X, y, sample_weight=None):
        logging.info('SVM.fit - preprocessing the data')
        X, y = self.preprocess(X, y, sample_weight)
        if sample_weight is not None:
            sample_weight = sample_weight[self.sample_indices]
        logging.info('SVM.fit - fitting the model')
        self.model.fit(X, y, sample_weight=sample_weight)

    def grid_cv(self, X, y, sample_weight=None):
        logging.info('SVM.grid_cv - preprocessing the data')
        X, y = self.preprocess(X, y, sample_weight)
        if sample_weight is not None:
            sample_weight = sample_weight[self.sample_indices]
        logging.info('SVM.grid_cv - tunning hyperparameters')
        self.grid_cv = sklearn.model_selection.GridSearchCV(self.model, self.param_grid, n_jobs=-2)
        self.grid_cv.fit(X, y, fit_params={'sample_weight': sample_weight})

    def frontier(self, gx, gy, gz, block=None):
        G = np.c_[np.ravel(gx), np.ravel(gy), np.ravel(gz)]
        Z = self.model.decision_function(G)
        Fx = gx[:,:,0]
        Fy = gy[:,:,0]
        zr = gz[0,0]
        Fz = find_roots(Fx,Fy,zr,Z)
        return Fx, Fy, Fz

    def estimate_tign_g(self, n=(500,500,100)):
        gx,gy,gz = make_meshgrid(n)
        Fx,Fy,Fz = self.frontier(gx, gy, gz)
        return Fx,Fy,Fz

    def save_model(self, path):
        logging.info('SVM.save_model - saving the model into {}'.format(path))
        with open('model','wb') as f:
            pickle.dumps(self,f)

    @classmethod
    def load_model(path):
        if osp.exists(path):
            with open(path,'rb') as f:
               return pickle.load(f)
        else:
            return None
