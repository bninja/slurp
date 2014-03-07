require 'net/http'
require 'serverspec'

include Serverspec::Helper::Exec
include Serverspec::Helper::DetectOS

describe command('slurp -v') do
  it { should return_exit_status 0 }
end

describe command('slurp sources') do
  it { should return_stdout 'test-access' }
end

describe command('slurp channels') do
  it { should return_stdout 'test-access-stats' }
end

describe command('slurp channel consume /tmp/slurp-test') do
  it { should return_exit_status 0 }
end
