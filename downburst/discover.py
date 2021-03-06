import requests
import re
import csv

URL="http://ceph.com/cloudinit/"

class UbuntuHandler:
    URL = 'http://cloud-images.ubuntu.com'

    VERSION_TO_RELEASE = {
        '4.10': 'warty',
        '5.10': 'hoary',
        '5.10': 'breezy',
        '6.06': 'dapper',
        '6.10': 'edgy',
        '7.04': 'feisty',
        '7.10': 'gutsy',
        '8.04': 'hardy',
        '8.10': 'intrepid',
        '9.04': 'jaunty',
        '9.10': 'karmic',
        '10.04': 'lucid',
        '10.10': 'maverick',
        '11.04': 'natty',
        '11.10': 'oneiric',
        '12.04': 'precise',
        '12.12': 'quantal',
        '13.04': 'raring',
        '13.10': 'saucy'}

    RELEASE_TO_VERSION = {v:k for k, v in VERSION_TO_RELEASE.items()}

    def get_release(self, distroversion):
        try:
            if "." in distroversion:
                version = distroversion.split('.', 1)
                major = version[0]
                minor = version[1].split('.', 1)[0]
                return self.VERSION_TO_RELEASE[major + "." + minor]
        except KeyError:
            return distroversion

    def get_version(self, distroversion):
        try:
            return self.RELEASE_TO_VERSION[distroversion]
        except KeyError:
            pass
        return distroversion

    def get_serial(self, release):
        url = self.URL + '/query/released.latest.txt'
        r = requests.get(url)
        r.raise_for_status()
        serial = None
        for row in csv.DictReader(r.content.strip().split("\n"),
                                  delimiter="\t",
                                  fieldnames=('release', 'flavour', 'stability',
                                              'serial')):
            if row['release'] == release and row['flavour'] == 'server' \
                                         and row['stability'] == 'release':
                return row['serial']
        raise NameError('Image not found on server at ' + url)

    def get_filename(self, arch, version):
        return 'ubuntu-' + version + '-server-cloudimg-'+ arch + '-disk1.img'
        

    def get_base_url(self, release, serial):
        return self.URL + '/releases/' + release + '/release-' + serial
        
    def get_url(self, base_url, filename):
        return base_url + "/" + filename
        
    def get_sha256(self, base_url, filename):
        url = base_url + "/SHA256SUMS"
        r = requests.get(url)
        rows = csv.DictReader(r.content.strip().split("\n"), delimiter=" ",
                              fieldnames=('hash', 'file'))
        for row in rows:
            if row['file'] == "*" + filename:
                return row['hash']
        raise NameError('SHA-256 checksums not found for file ' + filename +
                        ' at ' + url)
        
    def __call__(self, distroversion, arch):
        distroversion = distroversion.lower()
        if arch == "x86_64":
            arch = "amd64"
        release = self.get_release(distroversion)
        version = self.get_version(distroversion)
        serial = self.get_serial(release)
        filename = self.get_filename(arch, version)
        base_url = self.get_base_url(release, serial)
        sha256 = self.get_sha256(base_url, filename)
        url = self.get_url(base_url, filename)
        
        return {'url': url, 'serial': serial, 'checksum': sha256,
                'hash_function': 'sha256'}

HANDLERS = {'ubuntu': UbuntuHandler()}

def get(distro, distroversion, arch):
    if distro in HANDLERS:
        handler = HANDLERS[distro]
        return handler(distroversion, arch)
    r = requests.get(URL)
    r.raise_for_status()
    c = re.sub('.*a href="', '', r.content)
    content = re.sub('.img">.*', '.img', c)
    list = re.findall('.*-cloudimg-.*', content)
    imageprefix = distro + '-' + distroversion + '-(\d+)'
    imagesuffix = '-cloudimg-' + arch + '.img'
    imagestring = imageprefix + imagesuffix
    file = search(imagestring=imagestring, list=list)
    if file is not False:
        sha512 = requests.get(URL + file + ".sha512")
        sha512.raise_for_status()
        returndict = {}
        returndict['url'] = URL + "/" + file
        returndict['serial'] = file.split('-')[2]
        returndict['checksum'] = sha512.content.rstrip()
        returndict['hash_function'] = 'sha512'
        return returndict
    else:
        raise NameError('Image not found on server at ' + URL)

def search(imagestring, list):
    for imagename in list:
        if re.match(imagestring, imagename):
            return imagename
    return False
