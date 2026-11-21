"""Install a pinned, task-local TeX engine; compile without shell escape.

The runtime belongs inside the current CMA task or local mail workspace. It is
never installed into the control repository or a shared machine environment.
"""
import argparse
import hashlib
import io
import json
import os
import platform
from pathlib import Path
import subprocess
import tarfile
from urllib.request import Request, urlopen


VERSION = '0.17.0'
DISTRIBUTIONS = {
    'x86_64': ('x86_64', '8533d07f9ccbd7a65824b9e0459041bca34af1eb33daba48f59215593753a3b7'),
    'aarch64': ('aarch64', 'b10954a95404f3ab2328d2fa59a5ebab8e657f893fab096f98be8db7c0c979b8'),
}


def install(root):
    root = Path(root).resolve()
    if platform.system() != 'Linux' or platform.machine() not in DISTRIBUTIONS:
        raise ValueError('The pinned CMA runtime supports Linux x86_64/aarch64')
    arch, expected = DISTRIBUTIONS[platform.machine()]
    root.mkdir(parents=True, exist_ok=True)
    binary = root / 'tectonic'
    receipt = root / 'tectonic.json'
    if binary.is_file() and receipt.is_file():
        known = json.loads(receipt.read_text())
        if (known.get('archive_sha256') == expected and
                known.get('binary_sha256') == hashlib.sha256(binary.read_bytes()).hexdigest()):
            return binary
    url = ('https://github.com/tectonic-typesetting/tectonic/releases/download/'
           f'tectonic%40{VERSION}/tectonic-{VERSION}-{arch}-unknown-linux-musl.tar.gz')
    req = Request(url, headers={'User-Agent': 'trureturing-outreach/runtime'})
    with urlopen(req, timeout=60) as response:
        raw = response.read(30 * 1024 * 1024 + 1)
    if len(raw) > 30 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('Official TeX distribution does not match the pinned digest')
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        entries = [m for m in archive.getmembers() if m.isfile() and Path(m.name).name == 'tectonic']
        if len(entries) != 1 or entries[0].size > 100 * 1024 * 1024:
            raise ValueError('Unexpected TeX distribution contents')
        content = archive.extractfile(entries[0]).read()
    temporary = root / 'tectonic.download'
    temporary.write_bytes(content)
    temporary.chmod(0o700)
    temporary.replace(binary)
    result = subprocess.run([str(binary), '--version'], check=True, capture_output=True, text=True)
    if VERSION not in result.stdout:
        raise ValueError('Unexpected installed TeX version')
    receipt.write_text(json.dumps({'version': VERSION, 'url': url, 'archive_sha256': expected,
                                  'binary_sha256': hashlib.sha256(content).hexdigest()}, indent=2) + '\n')
    return binary


def compile_pdf(source, output, runtime):
    source, output = Path(source).resolve(), Path(output).resolve()
    if source.suffix != '.tex' or not source.is_file():
        raise ValueError('Select an existing main .tex source')
    output.mkdir(parents=True, exist_ok=True)
    binary = install(runtime)
    task_runtime = Path(runtime).resolve()
    environment = dict(os.environ)
    for variable, folder in [('XDG_CACHE_HOME', 'cache'), ('XDG_CONFIG_HOME', 'config'),
                             ('XDG_DATA_HOME', 'data'), ('TMPDIR', 'tmp')]:
        location = task_runtime / folder
        location.mkdir(parents=True, exist_ok=True)
        environment[variable] = str(location)
    fonts = task_runtime / 'fonts.conf'
    if not fonts.exists():
        fonts.write_text('<?xml version="1.0"?>\n<fontconfig><cachedir prefix="xdg">fontconfig</cachedir></fontconfig>\n')
    environment.update(FONTCONFIG_FILE=str(fonts), FONTCONFIG_PATH=str(task_runtime))
    subprocess.run([str(binary), '--untrusted', '--keep-logs', '--outdir', str(output), str(source)],
                   cwd=source.parent, env=environment, check=True, timeout=600)
    pdf = output / (source.stem + '.pdf')
    if not pdf.is_file() or not pdf.read_bytes().startswith(b'%PDF-'):
        raise ValueError('TeX did not produce a PDF')
    return {'name': pdf.name, 'bytes': pdf.stat().st_size,
            'sha256': hashlib.sha256(pdf.read_bytes()).hexdigest(), 'engine': 'tectonic-' + VERSION}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime', required=True, type=Path)
    p.add_argument('--source', type=Path)
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    if args.source:
        if not args.output:
            p.error('--source requires --output')
        print(json.dumps(compile_pdf(args.source, args.output, args.runtime)))
    else:
        print(json.dumps({'binary': str(install(args.runtime)), 'version': VERSION}))


if __name__ == '__main__':
    main()
