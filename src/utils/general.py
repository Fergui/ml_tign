#
# Angel Farguell, CU Denver
#

import os, sys, logging, json, collections
import os.path as osp

class Dict(dict):
    """
    A dictionary that allows member access to its keys.
    A convenience class.
    """
    def __init__(self, d):
        """
        Updates itself with d.
        """
        self.update(d)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, item, value):
        self[item] = value

def ensure_dir(path):
    """
    Ensure all directories in path if a file exist, for convenience return path itself.

    :param path: the path whose directories should exist
    :return: the path back for convenience
    """
    path_dir = osp.dirname(path)
    if not osp.exists(path_dir):
        os.makedirs(path_dir)
    return path

def make_dir(dir):
    """
    Create a directory if it does not exist. Creates any intermediate dirs as necessary.
    For convenience return the director path itself

    :param dir: the directory to be created
    :retun: same as input
    """
    if not osp.exists(dir):
        os.makedirs(dir)
    return dir

def remove(tgt):
    """
    os.remove wrapper
    """
    if osp.isfile(tgt):
        logging.info('remove - file {} exists, removing'.format(tgt))
        os.remove(tgt)

def load_json(json_path,critical=False):
    """
    Load json file into Dict 

    :return: dictonary with json file content
    """
    ret = None
    try:
        ret = Dict(json.load(open(json_path)))
        for key in list(ret.keys()):
            if ret[key] is None:
                logging.warning('load_json - argument {}=None, ignoring'.format(key))
                del ret[key]
    except:
        if critical:
            logging.critical('load_json - any {} specified, found, or readable'.format(json_path))
            sys.exit(2)
        else:
            logging.warning('load_json - any {} specified, found, or readable'.format(json_path))
            ret = Dict({})

    return ret

def load_sys_cfg():
    """
    Load the system configuration

    :return: system configuration in a dictionary
    """
    sys_cfg = load_json('etc/sys.json')
    if not sys_cfg: logging.warning('load_sys_cfg - cannot find system configuration, creating default configuration...')
    
    # set defaults
    sys_cfg.sys_install_path = sys_cfg.get('sys_install_path',os.getcwd())
    # configuration defaults + make directories if they do not exist
    sys_cfg.workspace_path = make_dir(osp.abspath(sys_cfg.get('workspace_path','work')))
    sys_cfg.ingest_path = make_dir(osp.abspath(sys_cfg.get('ingest_path','ingest')))
    sys_cfg.log_path = make_dir(osp.abspath(sys_cfg.get('log_path','logs')))
    return sys_cfg

def available_locally(path):
    """
    Check if a file is available locally and if it's file size checks out.

    :param path: the file path
    """
    info_path = path + '.size'
    if osp.exists(path) and osp.exists(info_path):
        content_size = int(open(info_path).read())
        return osp.getsize(path) == content_size
    else:
        return False

def duplicates(replist):
    """
    Give dictionary of repeated elements (keys) and their indexes (array in values)

    :param replist: list to look for repetitions
    """
    counter=collections.Counter(replist)
    dups=[i for i in counter if counter[i]!=1]
    result={}
    for item in dups:
        result[item]=[i for i,j in enumerate(replist) if j==item]
    return result

def process_arguments(sys_cfg,job_file):
    """
    Convert arguments passed into program via the JSON configuration file and job json argument.
    This is processed after the configuration is updated by the job json file.
    """
    args = sys_cfg
    # load job JSON and merge job arguments into sys configuration options
    job_args = load_json(job_file,critical=True)
    args.update(job_args)
    # load svm configuration if etc/svm.json exists
    svm_args = load_json('etc/svm.json')
    args.update(svm_args)
    # load tokens if etc/tokens.json exists
    tokens = load_json('etc/tokens.json')
    args.update(tokens)
    return args

def json_join(path,json_list):
    """
    Join local jsons in a singular json and remove the previous jsons

    :param path: local path to the jsons
    :param json_list: list of json files to join
    """
    manifest = Dict({})
    for jj in json_list:
        json_path = osp.join(path,str(jj)+'.json')
        try:
            f = json.load(open(json_path))
            manifest.update({jj: f})
        except:
            logging.warning('no satellite data for source %s in manifest json file %s' % (jj,json_path))
            manifest.update({jj: {}})
            pass
        remove(json_path)
    json.dump(manifest, open(osp.join(path, 'granules.json'),'w'), indent=4, separators=(',', ': '))
    return manifest
