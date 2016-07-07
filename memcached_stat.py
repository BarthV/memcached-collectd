import bmemcached
import collectd
import socket
import re

# Verbose logging on/off. Override in config by specifying 'Verbose'.
VERBOSE_LOGGING = False

# Global settings, one entry per instance
CONFIGS = []

class EmptyString:
  def __str__(self):
    return ''

def get_memcached_stats(conf):
    stats={}
    # connect to memcached
    try:
        connection_string = "%s:%d"%(conf['host'],conf['port'])
        client = bmemcached.Client(connection_string, conf['instance'], EmptyString())
        log_verbose("Connection string=%s (instance=%s)"%(connection_string, conf['instance']))
        for verb in ['', 'slabs', 'items']:
            s = client.stats(verb)
            assert(len(s) == 1) # we have only one host
            stats[verb]=s[connection_string]
    except socket.error, e:
        collectd.error('memcached_stat plugin: Error connecting to %d - %r'
                       % (conf['port'], e))
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
    for k,v in stats['slabs'].iteritems():
        if ':' not in k:
            continue
        slabid, key = k.split(':')
        if key == 'chunk_size':
            chunk_sizes[slabid] = int(v)
        elif key == 'total_chunks':
            total_chunks[slabid] = int(v)
    # prepare the intersection of keys in both maps
    slabids = set(chunk_sizes.keys()) & set(total_chunks.keys())
    log_verbose("preparing synthetic metrics for %d slabs"%len(slabids))
    for slabid in slabids:
        stats['slabs']["synth:%s:slab_size"%slabid] = chunk_sizes[slabid] * total_chunks[slabid]
        log_verbose("memcached_stats plugin: created a synthetic slab_size metric for slabid %s"%slabid)

def read_callback():
    for conf in CONFIGS:
        stats=get_memcached_stats(conf)
        make_synthetic_stats(stats)
        for verb,stat in stats.iteritems():
            # filtering stats that are mentionned in the configuration
            fstat = dict((k, v) for k, v in stat.iteritems() if k.split(':')[-1] in conf['filtered_stat_types'])
            # fix keys by prepending the verb if it doesn't exist
            ffstat = dict((verb+':'+k if not k.startswith(verb) else k, v) for k, v in fstat.iteritems())
            for k, v in ffstat.iteritems():
                dispatch_value(k, conf['filtered_stat_types'][k.split(':')[-1]], v, conf['instance'])

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
