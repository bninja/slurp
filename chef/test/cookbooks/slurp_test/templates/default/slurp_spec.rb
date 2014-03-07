require 'net/http'
require 'serverspec'

include Serverspec::Helper::Exec
include Serverspec::Helper::DetectOS

describe command('slurp -v') do
  it { should return_exit_status 0 }
  it { should return_stderr '' }
end

describe command('slurp channels') do
  it { should return_exit_status 0 }
  it { should return_stderr '' }
  it {
      should return_stdout <<-EOF
test-channel
EOF
    }
end

describe command('slurp sources') do
  it { should return_exit_status 0 }
  it { should return_stderr '' }
  it {
      should return_stdout <<-EOF
test-source
EOF
    }
end

describe command('slurp consume -b -t /tmp/test-slurp') do
  it { should return_exit_status 0 }
  it { should return_stderr '' }
  it {
    should return_stdout <<-EOF
bytes: 1140
count: 3
nginx-error-search nginx-error tests/fixtures/sources/nginx-error.log 3 1140 0
EOF
  }
end
