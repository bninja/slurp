require 'net/http'
require 'serverspec'

include Serverspec::Helper::Exec
include Serverspec::Helper::DetectOS

describe command('slurp -v') do
  it { should return_exit_status 0 }
end

describe command('slurp channels') do
  it { should return_exit_status 0 }
  it {
      should return_stdout 'test-channel'
    }
end

describe command('slurp sources') do
  it { should return_exit_status 0 }
  it {
      should return_stdout 'test-source'
    }
end

describe command('slurp consume -b -t /tmp/test-slurp/test.log') do
  it { should return_exit_status 0 }
  its(:stdout) {
    should include <<EOF
bytes: 1950
count: 6
test-channel test-source /tmp/test-slurp/test.log 6 1950 0
EOF
  }
end
