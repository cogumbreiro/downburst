import os
import subprocess
import tempfile

from lxml import etree

from . import meta
from . import template
from . import volume

def generate_meta_iso(
    name,
    fp,
    meta_data,
    user_data,
    ):
    def gentemp(prefix):
        return tempfile.NamedTemporaryFile(
            prefix='downburst.{prefix}.'.format(prefix=prefix),
            suffix='.tmp',
            )
    with gentemp('meta') as meta_f, gentemp('user') as user_f:
        meta.write_meta(meta_data=meta_data, fp=meta_f)
        meta.write_user(user_data=user_data, fp=user_f)

        subprocess.check_call(
            args=[
                'genisoimage',
                '-quiet',
                '-input-charset', 'utf-8',
                '-volid', 'cidata',
                '-joliet',
                '-rock',
                '-graft-points',
                'user-data={path}'.format(path=user_f.name),
                'meta-data={path}'.format(path=meta_f.name),
                ],
            stdout=fp,
            close_fds=True,
            )

def create_meta_iso(
    pool,
    name,
    meta_data,
    user_data,
    ):
    with tempfile.TemporaryFile() as iso:
        generate_meta_iso(
            name=name,
            fp=iso,
            meta_data=meta_data,
            user_data=user_data,
            )
        iso.seek(0)
        length = os.fstat(iso.fileno()).st_size
        assert length > 0
        return volume.create_volume(pool,
            'cloud-init.{name}.iso'.format(name=name),
            fp=iso,
            format_='raw')

