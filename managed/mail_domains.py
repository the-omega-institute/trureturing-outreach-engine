"""Domain matching shared by mailbox triage and outgoing mail policy."""

HOME_RECIPIENT_REASON = 'home-institution recipient'


def home_domains(config):
    configured = config.get('home_domains', config.get('mailbox', {}).get('home_domains', []))
    return {domain.strip().casefold().rstrip('.') for domain in configured}


def matches_domain(domain, suffixes):
    return any(domain == suffix or domain.endswith('.' + suffix) for suffix in suffixes)


def home_recipient_override(packet, suffixes):
    if not packet or not suffixes:
        return None
    addresses = [address for address in packet.get('to', []) + packet.get('cc', [])
                 if matches_domain(address.rpartition('@')[2].casefold().rstrip('.'), suffixes)]
    if addresses:
        return {'reason': HOME_RECIPIENT_REASON, 'addresses': addresses}
    return None
