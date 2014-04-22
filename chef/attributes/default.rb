default['slurp']['package']['name'] = 'slurp'
default['slurp']['package']['version'] = nil

default['slurp']['user'] = 'slurp'
default['slurp']['group'] = 'slurp'

default['slurp']['conf_dir'] = '/etc/slurp'
default['slurp']['state_dir'] = '/var/lib/slurp'
default['slurp']['includes_dir'] = "#{default['slurp']['conf_dir']}/conf.d"

default['slurp']['newrelic_file'] = nil
default['slurp']['newrelic_env'] = nil
default['slurp']['read_size'] = 4096
default['slurp']['buffer_size'] = 1048576
default['slurp']['batch_size'] = 200
default['slurp']['track'] = false
default['slurp']['backfill'] = false
default['slurp']['strict'] = false
default['slurp']['strict_slack'] = 0
