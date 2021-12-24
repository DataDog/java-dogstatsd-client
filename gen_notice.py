#!/usr/bin/env python3

"""
Generate third-party license information.
- NOTICE file for the binary distribution
- THIRDPARTY.md to be displayed on github

When using this script to update license information, verify that the
sources included (this script unpacks them into target/dependency)
matches the license.
"""

import functools
import os, os.path, re
import xml.etree.ElementTree as ElementTree
import urllib.request
from collections import namedtuple

Dep = namedtuple('Dep', ['groupId', 'artifactId', 'version'])
def list_dependencies():
    pattern = re.compile(".* (?P<groupId>[^:]*):(?P<artifactId>[^:]*):jar:(?P<version>[^:]*):compile")
    with os.popen("mvn dependency:list") as l:
        for line in l:
            m = pattern.match(line)
            if not m:
                continue
            yield Dep(**{k: m[k] for k in ['groupId', 'artifactId', 'version']})

def unpack_dependencies():
    if not os.path.isdir("target/dependency"):
        os.system("mvn dependency:copy-dependencies -Dmdep.copyPom -Dmdep.useRepositoryLayout")
        os.system("mvn dependency:unpack-dependencies -DincludeScope=compile -Dmdep.useRepositoryLayout")
        os.system("mvn dependency:unpack-dependencies -Dclassifier=sources -DincludeScope=compile -Dmdep.useRepositoryLayout")

def dep_path(dep):
    return os.path.join(
        'target', 'dependency',
        *dep.groupId.split('.'),
        dep.artifactId,
        dep.version)

def scan_unpacked(dep, ignored={'module-info.class'}):
    classdirs = set()
    prefix = dep_path(dep)
    for dirpath, _, filenames in os.walk(prefix):
        for f in filenames:
            if f.endswith('.class') and f not in ignored:
                if dirpath.removeprefix(prefix).removeprefix('/') == '':
                    raise ValueError(dirpath, prefix)
                classdirs.add(dirpath.removeprefix(prefix).removeprefix('/'))

    return unify_paths(classdirs)

def unify_paths(paths):
    res = set()
    for p in sorted(paths):
        if not any(p.startswith(r) for r in res):
            res.add(p)
    return res

Pom = namedtuple('Pom', ['authors', 'license'])
PomLicense = namedtuple('PomLicense', ['name', 'url'])
def read_pom(dep, ns={'pom': 'http://maven.apache.org/POM/4.0.0'}):
    path = os.path.join(dep_path(dep), f"{dep.artifactId}-{dep.version}.pom")
    pom = ElementTree.parse(path)
    auth = pom.findall('.//pom:developers/pom:developer/pom:name', ns)
    auth = ', '.join(a.text for a in auth)
    lic = pom.find('.//pom:licenses/pom:license', ns)
    return Pom(auth, PomLicense(
        lic.findtext('./pom:name', None, ns),
        lic.findtext('./pom:url', None, ns),
    ))

LICENSES = {
    # Links to the project front-page.
    'org.ow2.asm': 'https://raw.githubusercontent.com/llbit/ow2-asm/master/LICENSE.txt',
    # These two link to generic license text without copyrights.
    'com.github.jnr/jnr-posix': 'https://raw.githubusercontent.com/jnr/jnr-posix/jnr-posix-{version}/LICENSE.txt',
    'com.github.jnr/jnr-x86asm': 'https://raw.githubusercontent.com/jnr/jnr-x86asm/{version}/LICENSE',
}

@functools.cache
def fetch(url):
    print(f'... fetching {url}')
    with urllib.request.urlopen(url) as f:
        return clean(f.read().decode('ascii'))

def clean(s, remove=re.compile('[^\n\t !-~]', re.A)):
    return remove.sub('', s)

def write_notice(notice, dep, classdirs, license_text):
    """
    NOTICE file to be included with the binary distribution and
    fullfill third-party license requirements.
    """
    header = f'{dep.groupId} {dep.artifactId} {dep.version}'
    print(header, file=notice)
    print('-' * len(header), file=notice)

    repo_path = '/'.join([*dep.groupId.split('.'), dep.artifactId, dep.version])
    print(f'Sources are available at https://repo.maven.apache.org/maven2/{repo_path}/\n', file=notice)

    print('Files in the following directories:', ' '.join(sorted(classdirs)), file=notice)
    print('are distributed under the terms of the following license:\n', file=notice)

    print(license_text, file=notice)
    print('\n', file=notice)

LISTING_HEADER = """\
This file lists third-party software included in
'jar-with-dependencies' binary distribution. Where multiple licenses
are available, we choose the most permissive license to apply to the
corresponding part of the binary distribution.

For full license text and copyrights (where applicable) please refer
to the [NOTICE](src/main/resources/META-INF/NOTICE) file.

| Group | Artifact | Version | Developers | License | License URL |
|-|-|-|-|-|-|
"""

def write_listing(listing, dep, pom):
    """
    THIRDPARTY.md provides an overview of third-party licenses that
    will apply to the binary distribution.
    """
    print(f'| {dep.groupId} | {dep.artifactId} | {dep.version} ', end='', file=listing)
    print(f'| {pom.authors} ', end='', file=listing)
    print(f'| {pom.license.name} | {pom.license.url} ', end='', file=listing)
    print(f'|', file=listing)

def gen_all(notice, listing):
    deps = list_dependencies()
    unpack_dependencies()

    print(LISTING_HEADER, end='', file=listing)

    for dep in sorted(deps):
        classdirs = scan_unpacked(dep)
        pom = read_pom(dep)

        license_url = LICENSES.get(f'{dep.groupId}/{dep.artifactId}') or LICENSES.get(dep.groupId)
        if license_url:
            license_url = license_url.format(**dep._asdict())
        else:
            license_url = pom.license.url
        if not license_url:
            print(f'License is missing for {dep}')
            break

        license_text = fetch(license_url)

        write_notice(notice, dep, classdirs, license_text)
        write_listing(listing, dep, pom)

if __name__ == '__main__':
    with open('src/main/resources/META-INF/NOTICE', 'w') as notice:
        with open('THIRDPARTY.md', 'w') as listing:
            gen_all(notice, listing)
