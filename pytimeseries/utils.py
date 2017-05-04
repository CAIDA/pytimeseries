
def graphite_safe_node(key):
    """
    Convert a string into a graphite-safe node key. 
    Periods are replaced by hyphens, slashes by underscores.
        
    :param key: string
    :return: graphite-safe string
    """
    return str(key).replace('.', '-').replace('/', '_')


def graphite_safe_ip(ip_addr):
    """
    Convert a string IP address into the format used by Charthouse
    
    TODO: sanity checks to make sure ip_addr is an IP address
    
    :param ip_addr: string
    :return: charthouse-formatted IP node
    """
    return '__IP_' + graphite_safe_node(str(ip_addr))


def graphite_safe_pfx(pfx):
    """
    Convert a string prefix into the format used by Charthouse
    
    TODO: sanity checks to make sure pfx is a correctly formed prefix
    
    :param pfx: string
    :return: charthouse-formatted prefix node
    """
    return '__PFX_' + graphite_safe_node(str(pfx))
