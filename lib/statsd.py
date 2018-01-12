import os
import subprocess
from m2ee import logger, munin
import buildpackutil
import toml


METRIX_AGENT_FILENAME = 'metrix-assembly-0.0.6-SNAPSHOT.jar'
METRIX_AGENT_FILEPATH = '.local/' + METRIX_AGENT_FILENAME
TELEGRAF_FILENAME = 'telegraf-1.5.0'
TELEGRAF_FILEPATH = '.local/' + TELEGRAF_VERSION
TELEGRAF_CONFIG_FILEPATH = '.local/telegraf-config.toml'


def _get_influx_config():
    return json.loads(os.getenv('TELEGRAF_INFLUX', '{}'))


def _get_datadog_config():
    return json.loads(os.getenv('TELEGRAF_DATADOG', '{}'))


def _write_telegraf_config(app_name):
    influx_config = _get_influx_config()
    datadog_config = _get_datadog_config()
    if not (influx_config or datadog_config):
        return

    config = {}

    config['global_tags'] = {
        'container_id': os.getenv('CF_INSTANCE_GUID'),
    }

    config['agent'] = {
        'interval': '10s',
        'round_interval': True,
        'metric_batch_size': 1000,
        'metric_buffer_limit': 10000,
        'collection_jitter': '0s',
        'flush_interval': '10s',
        'flush_jitter': '3s',
        'debug': False,
        'quiet': True,
        'logfile': '',
        'omit_hostname': False,
        'hostname': app_name,
    }

    config['inputs'] = {}
    config['outputs'] = {}

    config['inputs']['statsd'] = [{
        'service_address': ':8125',
        'delete_gauges': True,
        'delete_counters': True,
        'delete_sets': True,
        'delete_timings': True,
        'percentiles': [90],
        'metric_separator': '.',
        'parse_data_dog_tags': True,
        'allowed_pending_messages': 10000,
        'percentile_limit': 1000,
    }]

    if influx_config:
        config['outputs']['influxdb'] = [influx_config]

    if datadog_config:
        config['outputs']['datadog'] = [datadog_config]

    with open(TELEGRAF_CONFIG_FILE, 'w') as fh:
        toml.dump(config, fh)


def download_resources_in_compile_phase():
    if not _needs_to_be_applied():
        return

    telegraf_file = os.path.join(DOT_LOCAL_LOCATION, TELEGRAF_VERSION)

    buildpackutil.download(
        buildpack_util.get_blobstore_url(
            '/mx-buildpack/{}'.format(METRIX_AGENT_FILE),
        ),
        os.path.join(DOT_LOCAL_LOCATION, METRIX_AGENT_FILE),
    )
    buildpackutil.download(
        buildpack_util.get_blobstore_url(
            '/mx-buildpack/telegraf-alpha-0.0.1/{}'.format(TELEGRAF_VERSION),
        ),
        telegraf_file,
    )
    os.chmod(telegraf_file, 0755)


def _set_up_java_agent(m2ee):
    jar = os.path.abspath(METRIX_AGENT_FILEPATH)
    m2ee.config._conf['m2ee']['javaopts'].extend([
        '-javaagent:{}'.format(jar),
        '-Xbootclasspath/a:{}'.format(jar),
    ])


def _needs_to_be_applied():
    return any(lambda key: key.startswith('TELEGRAF_'), os.environ.keys())


def start_telegraf_sidecar_and_configure_java_agent(app_name, m2ee):
    if not _needs_to_be_applied():
        return

    logger.info('starting telegraf service')

    for filepath in [
        TELEGRAF_FILEPATH,
        METRIX_AGENT_FILEPATH,
    ]:
        if not os.path.isfile(filepath):
            raise Exception(
                'file {} not found, so can not start telegraf and java agent '
                'you might need to restage the app'.format(
                    filepath
                )
            )

    _write_telegraf_config(app_name)
    _set_up_java_agent(m2ee)

    subprocess.Popen([
        TELEGRAF_FILEPATH, '--config', TELEGRAF_CONFIG_FILEPATH,
    ])
