import hashlib
import logging
import requests

from lxml import etree

from . import discover
from . import exc
from . import template
from . import volume

log = logging.getLogger(__name__)

def list_cloud_images(pool, distro, distroversion, arch):
    """
    List all Cloud images in the libvirt pool.
    Return the keys.
    """

    #Fix distro version if someone did not use quotes
    if distro == "ubuntu":
        if isinstance(distroversion, float):
            distroversion = '%.2f' % distroversion

    PREFIX = distro+"-"+distroversion+"-"
    SUFFIX = '-cloudimg-'+arch+'.img'

    for name in pool.listVolumes():
        log.debug('Considering image: %s', name)
        if not name.startswith(PREFIX):
            continue
        if not name.endswith(SUFFIX):
            continue
        if len(name) <= len(PREFIX) + len(SUFFIX):
            # no serial number in the middle
            continue
        # found one!
        log.debug('Saw image: %s', name)
        yield name


def find_cloud_image(pool, distro, distroversion, arch):
    """
    Find a Cloud image in the libvirt pool.
    Return the name.
    """
    names = list_cloud_images(pool, distro=distro, distroversion=distroversion, arch=arch)
    # converting into a list because max([]) raises ValueError, and we
    # really don't want to confuse that with exceptions from inside
    # the generator
    names = list(names)

    if not names:
        log.debug('No cloud images found.')
        return None

    # the build serial is zero-padded, hence alphabetically sortable;
    # max is the latest image
    return max(names)


def ensure_cloud_image(conn, distro, distroversion, arch):
    """
    Ensure that the Ubuntu Cloud image is in the libvirt pool.
    Returns the volume.
    """
    log.debug('Opening libvirt pool...')
    pool = conn.storagePoolLookupByName('default')

    log.debug('Listing cloud image in libvirt...')
    name = find_cloud_image(pool=pool, distro=distro, distroversion=distroversion, arch=arch)
    if name is not None:
        # all done
        log.debug('Already have cloud image: %s', name)
        vol = pool.storageVolLookupByName(name)
        return vol

    log.debug('Discovering cloud images...')
    image = discover.get(distro=distro, distroversion=distroversion, arch=arch)
    log.debug('Will fetch serial number: %s', image['serial'])

    url = image['url']

    log.info('Downloading image: %s', url)
    r = requests.get(url)
    # volumes have no atomic completion marker; this will forever be
    # racy!

    PREFIX = distro+"-"+distroversion+"-"
    SUFFIX = '-cloudimg-'+arch+'.img'

    name = '{prefix}{serial}{suffix}'.format(
        prefix=PREFIX,
        serial=image['serial'],
        suffix=SUFFIX,
        )
    return volume.create_volume(pool, name, r.raw,
                                hash_function='sha512',
                                checksum=image['sha512'])

