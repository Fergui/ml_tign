#
# Angel Farguell, CU Denver
#

import logging, time, subprocess, requests, random, os
from six.moves.urllib import request as urequest
import os.path as osp

from utils.general import ensure_dir, load_sys_cfg, remove

cfg = load_sys_cfg()
sleep_seconds=cfg.get('sleep_seconds', 20)
max_retries=cfg.get('max_retries', 3)
wget=cfg.get('wget','/usr/bin/wget')
wget_options=cfg.get('wget_options',["--read-timeout=1"])
download_sleep_seconds=cfg.get('download_sleep_seconds', 5)

class DownloadError(Exception):
    """
    Raised when the downloader is unable to retrieve a URL.
    """
    pass

def request_url(url,use_urllib2=False,token=None):
    """
    Request web url

    :param url: the remote URL
    :param url: the remote URL
    """
    if token:
        r = urequest.urlopen(urequest.Request(url,headers={'Authorization': 'Bearer {}'.format(token)})) if use_urllib2 else requests.get(url, stream=True, headers={'Authorization': 'Bearer {}'.format(token)})   
    else:
        r = urequest.urlopen(url) if use_urllib2 else requests.get(url, stream=True)
    return r

def download_url(url, local_path, max_retries=max_retries, sleep_seconds=sleep_seconds, token=None):
    """
    Download a remote URL to the location local_path with retries.

    On download, the file size is first obtained and stored.  When the download completes,
    the file size is compared to the stored file.  This prevents broken downloads from
    contaminating the processing chain.

    :param url: the remote URL
    :param local_path: the path to the local file
    :param max_retries: how many times we may retry to download the file
    :param sleep_seconds: sleep seconds between retries
    :param token: use a header token if specified
    """
    logging.info('download_url - {0} as {1}'.format(url, local_path))
    logging.debug('download_url - if download fails, will try {0} times and wait {1} seconds each time'.format(max_retries, sleep_seconds))
    sec = random.random() * download_sleep_seconds
    logging.info('download_url - sleeping {} seconds'.format(sec))
    time.sleep(sec)

    use_urllib2 = url[:6] == 'ftp://'

    try:
        r = request_url(url,use_urllib2,token)
    except Exception as e:
        if max_retries > 0:
            logging.info('download_url - not found, trying again, retries available {}'.format(max_retries))
            logging.info('download_url - sleeping {} seconds'.format(sec))
            time.sleep(sleep_seconds)
            download_url(url, local_path, max_retries=max_retries-1, token=token)
        return

    logging.info('download_url - {0} as {1}'.format(url,local_path))
    remove(local_path)
    command=[wget,'-O',ensure_dir(local_path),url]
    for opt in wget_options:
        command.insert(1,opt)
    if token:
        command.insert(1,'--header=\'Authorization: Bearer {}\''.format(token))
    logging.info(' '.join(command))
    subprocess.call(' '.join(command),shell=True)

    file_size = osp.getsize(local_path)

    # content size may have changed during download
    r = request_url(url,use_urllib2,token)
    content_size = int(r.headers.get('content-length',0))

    logging.info('download_url - local file size {0} remote content size {1}'.format(file_size, content_size))

    if int(file_size) != int(content_size):
        logging.warning('download_url - wrong file size, trying again, retries available {}'.format(max_retries))
        if max_retries > 0:
            # call the entire function recursively, this will attempt to redownload the entire file
            # and overwrite previously downloaded data
            logging.info('download_url - sleeping {} seconds'.format(sleep_seconds))
            time.sleep(sleep_seconds)
            download_url(url, local_path, max_retries = max_retries-1, token = token)
            return  # success
        else:
            os.remove(local_path)
            raise DownloadError('download_url - failed to download file {}'.format(url))

    # dump the correct file size to an info file next to the grib file
    # when re-using the GRIB2 file, we check its file size against this record
    # to avoid using partial files
    info_path = local_path + '.size'
    open(ensure_dir(info_path), 'w').write(str(content_size))

