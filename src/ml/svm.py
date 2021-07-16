import imblearn
import sklearn
import numpy as np
import logging
import pickle
import psutil
import concurrent.futures
from multiprocessing import Pool
import os.path as osp
from collections import Counter
from scipy import interpolate

def make_meshgrid(n):
    logging.info('making meshgrid with size={}'.format(n))
    nx, ny, nz = n
    gx, gy, gz = np.meshgrid(np.linspace(0., 1., ny),
                         np.linspace(0., 1., nx),
                         np.linspace(0., 1., nz))
    return gx, gy, gz

def find_roots(Fx,Fy,zr,Z):
    logging.info('finding roots of the decision function')
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
        C_grid = np.array([.5,1.,2.])
        g_grid = np.array([.5,1.,2.])
        self.param_grid = param_grid if len(param_grid) else {'C': C_grid, 'gamma': g_grid} 
        self.total_cache = int(psutil.virtual_memory().total)/float(1<<20) # in MB
        self.nproc = psutil.cpu_count()
        self.model = sklearn.svm.SVC(class_weight="balanced", cache_size=(self.total_cache//(2*self.nproc)))
        logging.info('SVM - {}'.format(self.model))

    def preprocess(self, X, y):
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

    def hyper_opt(self, X):
        influ_km = 2 # influence in kilometers
        self.domain_size = (X[:,0].max()-X[:,0].min())*111 # domain size x in kilometers
        sigma = influ_km/self.domain_size # sigma scaled to [0,1]
        self.gamma = 1/(2*sigma**2) # gamma scaled to [0,1]
        logging.info('SVM.hyper_opt - gamma={}'.format(self.gamma))
        self.param_grid['gamma'] *= self.gamma 
        size_y = (X[:,1].max()-X[:,1].min())*111 # domain size y in kilometers
        freq_ros = 2 # frequent ROS in km/h
        influ_days = influ_km/freq_ros/24 # influence in days
        total_days = X[:,2].max()-X[:,2].min() # total number of days
        self.scale_dims = np.array([1., # no scale for x
				size_y/self.domain_size, # scale kilometers
				sigma/(influ_days/total_days)]) # scale days
        logging.info('SVM.hyper_opt - scale_dims={}'.format(self.scale_dims))

    def fit(self, X, y, sample_weight=None):
        # hyper-parameter approximation
        self.hyper_opt(X)
        # compute min-max lon-lat to estimate size of domain and proportion in time
        logging.info('SVM.fit - preprocessing the data')
        X, y = self.preprocess(X, y)
        X *= self.scale_dims
        self.model.gamma = self.gamma
        if sample_weight is not None:
            sample_weight = sample_weight[self.sample_indices]
        logging.info('SVM.fit - fitting the model with C={} and gamma={}'.format(self.model.C,self.model.gamma))
        self.model.fit(X, y, sample_weight=sample_weight)

    def grid_cv(self, X, y, sample_weight=None):
        # hyper-parameter approximation
        self.hyper_opt(X)
        # compute min-max lon-lat to estimate size of domain and proportion in time
        logging.info('SVM.grid_cv - preprocessing the data')
        X, y = self.preprocess(X, y)
        X *= self.scale_dims
        if sample_weight is not None:
            sample_weight = sample_weight[self.sample_indices]
        logging.info('SVM.grid_cv - tunning hyperparameters')
        logging.info('SVM.grid_cv - parameter grid: {}'.format(self.param_grid))
        scorer = sklearn.metrics.make_scorer(sklearn.metrics.f1_score,average='weighted')
        self.grid_cv = sklearn.model_selection.GridSearchCV(estimator=self.model, param_grid=self.param_grid, 
							    scoring=scorer, cv=3, verbose=4, n_jobs=-2)
        self.grid_cv.fit(X, y, sample_weight=sample_weight)
        logging.info('SVM.grid_cv - best parameters: {}'.format(self.grid_cv.best_params_))
        self.model = self.grid_cv.best_estimator_

    def decision_function(self, G, mthreads=True):
        logging.info('SVM.decision_function - evaluating the decision function for {} points'.format(len(G)))
        if mthreads:
            self.nsplits = [c for c in range(1,self.nproc+1) if len(G) % c == 0][-1]
            if len(G)/self.nsplits > 10.:
                logging.info('SVM.decision_function - using parallel strategy with {} splits'.format(self.nsplits))
                iterator = list(np.split(G,self.nsplits))
                with Pool(self.nsplits) as pool:
                    Z = pool.map(self.model.decision_function, iterator)
                Z = np.concatenate(tuple(Z))
            else:
                logging.info('SVM.decision_function - using no parallelization')
                Z = self.model.decision_function(G)
        else:
            logging.info('SVM.decision_function - using no parallelization')
            Z = self.model.decision_function(G)
        return Z

    def estimate_tign_g(self, n=(400,400,40)):
        logging.info('SVM.estimate_tign_g - estimating tign_g')
        gx,gy,gz = make_meshgrid(n)
        G = np.c_[np.ravel(gx), np.ravel(gy), np.ravel(gz)]
        G *= self.scale_dims
        Z = self.decision_function(G)
        Zg = np.reshape(Z,gx.shape)
        Fx = gx[:,:,0]
        Fy = gy[:,:,0]
        zr = gz[0,0]
        Fz = find_roots(Fx,Fy,zr,Zg)
        Fz[np.isnan(Fz)] = max(np.nanmax(Fz),self.scale_dims[-1])
        F = np.c_[np.ravel(Fx), np.ravel(Fy), np.ravel(Fz)]
        F /= self.scale_dims
        F = self.scaler.inverse_transform(F)
        return np.reshape(F[:,0],Fx.shape), np.reshape(F[:,1],Fx.shape), np.reshape(F[:,2],Fx.shape)

    def save_model(self, path):
        logging.info('SVM.save_model - saving the model into {}'.format(path))
        with open(path,'wb') as f:
            pickle.dump(self,f)

    @classmethod
    def load_model(cls, path):
        if osp.exists(path):
            with open(path,'rb') as f:
               return pickle.load(f)
        else:
            return None
