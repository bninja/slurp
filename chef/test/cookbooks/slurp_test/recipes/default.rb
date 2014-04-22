include_recipe 'slurp'

%w(test.conf).each do |name|
  cookbook_file "#{node['slurp']['conf_dir']}/conf.d/#{name}" do
    source name
    owner node['slurp']['user']
    group node['slurp']['group']
    mode '0644'
  end
end

directory "/tmp/test-slurp" do
    owner node['slurp']['user']
    group node['slurp']['group']
    mode '0755'
end

%w(test.log).each do |name|
  cookbook_file "/tmp/test-slurp/#{name}" do
      source name
      owner node['slurp']['user']
      group node['slurp']['group']
      mode '0666'
  end
end

include_recipe 'sysctl'

sysctl_param 'fs.inotify.max_user_watches' do
  value 16384
end
