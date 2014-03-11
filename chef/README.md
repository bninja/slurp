slurp Cookbook
==============

Installs and configures slurp.


Requirements
------------

A slurp package.

### Tested Platforms

- Ubuntu (12.04)


Attributes
----------

- `node['slurp']['package']['name']` package name, defaults to 'slurp'
- `node['slurp']['package']['version']` package version, defaults to latest
- `node['slurp']['user']` user name created by package, defaults to 'slurp'
- `node['slurp']['group']` group name created by package, defaults to 'slurp'
- `node['slurp']['conf_dir']` conf directory created by package, defaults to '/etc/slurp'
- `node['slurp']['state_dir']` state directory created by package, defaults to '/var/lib/slurp'
- `node['slurp']['includes_dir']` conf directory created by package, defaults to '/etc/slurp/conf.d'
- `node['slurp']['newrelic_file']` a newrelic configuration file to use for stats collection, defauls to `nil`
- `node['slurp']['newrelic_env']` the newrelic environment to use for stats collection, defauls to `nil`
- `node['slurp']['read_size']` default source read size in bytes
- `node['slurp']['buffer_size']` default source buffer size in bytes 
- `node['slurp']['batch_size']` default batch size in blocks 
- `node['slurp']['track']` default channel tracking flag 
- `node['slurp']['backfill']` default channel backfill flag
- `node['slurp']['strict']` default source and channel strict-ness flag
- `node['slurp']['strict_slack']` default channel strict-ness slack count

Recipes
-------

### default.rb

Installs the slurp package and renders its configuration to `node['slurp']['conf_dir']`/slurp.conf. 
