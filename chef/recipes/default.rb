include_recipe 'balanced-apt'

package node['slurp']['package']['name'] do
  action :upgrade
  version node['slurp']['package']['version']
end

template "#{node['slurp']['conf_dir']}/slurp.conf" do
  source 'slurp.conf.erb'
  owner node['slurp']['user']
  group node['slurp']['group']
  mode '0644'
end
