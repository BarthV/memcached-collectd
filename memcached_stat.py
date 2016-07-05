import collectd
import socket
import re

# Verbose logging on/off. Override in config by specifying 'Verbose'.
VERBOSE_LOGGING = False

# Global settings, one entry per instance
CONFIGS = []

def get_memcached_stats(conf):
    stats={}
    # connect to memcached
    try:
        # TODO: use python-memcached to get these stats, available in centos7
        # (centos6 doesn't have a recent version that supports more than basic stats)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((conf['host'], conf['port']))
        log_verbose("connected to memcached port %d"%conf['port'])
    except socket.error, e:
        collectd.error('memcached_stat plugin: Error connecting to %d - %r'
                       % (conf['port'], e))
    fp=s.makefile()
    for verb in ['', 'slabs', 'items']:
        stats[verb]=[]
        s.sendall("stats %s\n"%verb)
        log_verbose("query stats %s"%verb)
        while True:
            line=fp.readline()
            if line.startswith("END"):
                break
            # skip the 'STAT' word at the beginning of the line
            stats[verb].append(line.split()[1:3])
    log_verbose("done extracting stats")
    return stats

def configure_callback(conf):
    """Receive configuration block"""
    host = None
    port = None
    instance = None
    filtered_stat_types={}
    global CONFIGS
    log_verbose("memcached_stats plugin: reading configuration (%d elements)"%(len(conf.children)))
    for node in conf.children:
        key = node.key.lower()
        val = node.values[0]
        statfilter = re.search(r'filter_(.*)$', key)

        if key == 'verbose':
            global VERBOSE_LOGGING
            VERBOSE_LOGGING = bool(val) or VERBOSE_LOGGING
        elif key == 'host':
            host = val
        elif key == 'port':
            port = int(val)
        elif key == 'instance':
            instance = val
        elif statfilter:
            log_verbose('Matching expression found: key: %s - value: %s' % (statfilter.group(1), val))
            filtered_stat_types[statfilter.group(1)] = val
        else:
            collectd.warning('memcached_stat plugin: Unknown config key: %s.' % key )
            continue
    CONFIGS.append( { 'host': host, 'port': port, 'instance': instance, 'filtered_stat_types':  filtered_stat_types } )
    log_verbose("memcached_stats plugin: configured pluging instance %s"%instance)

def make_synthetic_stats(stats):
    # create the slab memory size (chunk_size*total_chunks for each slabs)
    chunk_sizes={}
    total_chunks={}
    for s in stats['slabs']:
        if ':' not in s[0]:
            continue
        slabid, key = s[0].split(':')
        if key == 'chunk_size':
            chunk_sizes[slabid] = s[1]
        elif key == 'total_chunks':
            total_chunks[slabid] = s[1]
    # prepare the intersection of keys in both maps
    slabids = set(chunk_sizes.keys()) & set(total_chunks.keys())
    log_verbose("preparing synthetic metrics for %d slabs"%len(slabids))
    for slabid in slabids:
        stats['slabs'].append(["synth:%s:slab_size"%slabid, int(chunk_sizes[slabid])*int(total_chunks[slabid])])
        log_verbose("memcached_stats plugin: craeted a synthetic slab_size metric for slabid %s"%slabid)

def read_callback():
    for conf in CONFIGS:
        stats=get_memcached_stats(conf)
        make_synthetic_stats(stats)
        for verb,stat in stats.iteritems():
            stat = filter(lambda s: s[0].split(':')[-1] in conf['filtered_stat_types'], stat)
            # fix keys by prepending the verb if it doesn't exist
            stat=map(lambda s: [verb+':'+s[0],s[1]] if not s[0].startswith(verb) else s, stat)
            for s in stat:
                log_verbose("Having a stat[%s]=%s"%(s[0], s[1]))
                dispatch_value(s[0],conf['filtered_stat_types'][s[0].split(':')[-1]],s[1], conf['instance'])

def dispatch_value(key, type, value, plugin_instance=None, type_instance=None):
    """Read a key from info response data and dispatch a value"""
    if plugin_instance is None:
        plugin_instance = 'unknown memcached'
        collectd.error('memcached_stats plugin: plugin_instance is not set, Info key: %s' % key)

    if not type_instance:
        type_instance = key

    log_verbose('Sending value: %s=%s' % (type_instance, value))

    val = collectd.Values(plugin='memcached')
    val.type = type
    val.type_instance = type_instance
    val.plugin_instance = plugin_instance
    val.values = [value]
    val.dispatch()

def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('memcached plugin [verbose]: %s' % msg)

# register callbacks
collectd.register_config(configure_callback)
collectd.register_read(read_callback)
