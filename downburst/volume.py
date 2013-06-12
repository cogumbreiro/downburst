import hashlib
import logging

from lxml import etree

from . import exc
from . import template

log = logging.getLogger(__name__)

def upload_volume(vol, fp, checksum=None, hash_function=None):
    """
    Upload a volume into a libvirt pool.
    """

    stream = vol.connect().newStream(flags=0)
    vol.upload(stream=stream, offset=0, length=0, flags=0)

    if checksum is not None:
        h = hashlib.new(hash_function)
        def handler(stream, nbytes, _):
            data = fp.read(nbytes)
            h.update(data)
            return data
        stream.sendAll(handler, None)
        if h.hexdigest() != checksum:
            stream.abort()
            raise exc.ImageHashMismatchError()
    else:
        def handler(stream, nbytes, _):
            data = fp.read(nbytes)
            return data
        stream.sendAll(handler, None)

    stream.finish()

def create_volume(pool, filename, fp, checksum=None, hash_function=None, **kwargs):
    """
    Creates a volume in a libvirt pool.
    """
    log.debug('Creating libvirt volume: %s ...', filename)
    volxml = template.volume(
        name=filename,
        # TODO we really should feed in a capacity, but we don't know
        # what it should be.. libvirt pool refresh figures it out, but
        # that's probably expensive
        # capacity=2*1024*1024,
        **kwargs)
    vol = pool.createXML(etree.tostring(volxml), flags=0)
    upload_volume(
        vol=vol,
        fp=fp,
        checksum=checksum,
        hash_function=hash_function,
        )
    # TODO only here to autodetect capacity
    pool.refresh(flags=0)
    return vol


