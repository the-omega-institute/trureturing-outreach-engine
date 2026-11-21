"""Prepare a scientific-only Mathesis handoff from a selected note package.

This command never submits a manuscript or checks author consent on their behalf.
It reuses the existing Mathesis manuscript source contract, leaving the platform's
consent, extracted-statement confirmation, review and acceptance intact.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import zipfile

from .location import DATA_REPO

from .location import DATA_REPO


def prepare(directory,output):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts/short_notes'))
    import workflow as w
    directory,output=Path(directory),Path(output)
    manifests=list(directory.glob('note-v*.json'))
    if len(manifests)!=1:raise ValueError('Select exactly one immutable note revision')
    package=w.read(manifests[0]);w.validate_metadata(package['metadata']);w.verify_package(package,directory)
    output.mkdir(parents=True,exist_ok=True)
    if list(output.iterdir()):raise ValueError('Use an empty handoff output directory')
    for name in package['files']:shutil.copyfile(directory/name,output/name)
    result={'schema_version':'outreach-mathesis-handoff.v1','kind':'short_note',
            'source_note':package['metadata'],'files':package['files'],
            'source_repository':DATA_REPO,
            'status':'prepared-not-submitted','third_party_processing_consent':'not-asserted',
            'statement_confirmation':'required-in-mathesis',
            'private_correspondence_included':False}
    (output/'handoff.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return result


def archive(directory, destination):
    """One reproducible scientific bundle, without any surrounding workspace."""
    directory, destination = Path(directory), Path(destination)
    handoff = json.loads((directory / 'handoff.json').read_text())
    if (handoff.get('schema_version') != 'outreach-mathesis-handoff.v1'
            or handoff.get('private_correspondence_included') is not False):
        raise ValueError('Select a scientific-only handoff')
    names = ['handoff.json']
    for name, expected in handoff['files'].items():
        path = directory / name
        if (Path(name).name != name or path.is_symlink() or
                hashlib.sha256(path.read_bytes()).hexdigest() != expected):
            raise ValueError('The handoff differs from its selected immutable files')
        names.append(name)
    if destination.resolve() in [(directory / n).resolve() for n in names]:
        raise ValueError('Do not overwrite a handoff input')
    with zipfile.ZipFile(destination, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for name in sorted(names):
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(info, (directory / name).read_bytes())
    return {'archive': str(destination), 'sha256': hashlib.sha256(destination.read_bytes()).hexdigest(),
            'files': sorted(names), 'note_id': handoff['source_note']['id'],
            'revision': handoff['source_note']['revision']}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('package',type=Path);p.add_argument('output',type=Path)
    p.add_argument('--archive', type=Path)
    a=p.parse_args(); result=prepare(a.package,a.output)
    if a.archive: result['bundle']=archive(a.output,a.archive)
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
