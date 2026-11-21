"""Content-free diagnostics for GitHub CLI failures."""
import re


RELEASE_ACTIONS = ('create', 'delete-asset', 'download', 'edit', 'upload', 'view')


def failure(error, args=()):
    if not isinstance(error, RuntimeError):
        return None
    match = re.match(r'GitHub (?:release )?operation failed: (.*)', str(error), re.S)
    return stderr_failure(match[1], args) if match else None


def stderr_failure(stderr, args):
    stderr = stderr.lower()
    status = re.search(r'\(http ([1-5][0-9]{2})\)', stderr) or re.search(r'\bhttp ([1-5][0-9]{2})\b', stderr)
    if status:
        detail = 'HTTP ' + status[1]
    else:
        detail = next((word for word in ('already_exists', 'release not found', 'no assets match')
                       if word in stderr), 'operation failed')
    if args[:1] == ('api',):
        return 'gh api ' + detail
    if len(args) >= 2 and args[0] == 'release' and args[1] in RELEASE_ACTIONS:
        return 'gh release ' + args[1] + ' ' + detail
    return 'GitHub ' + detail
