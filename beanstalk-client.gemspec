# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'beanstalk-client/version'

Gem::Specification.new do |gem|
  gem.name          = "beanstalk-client"
  gem.version       = Beanstalk::VERSION
  gem.authors       = ["Keith Rarick"]
  gem.email         = ["kr@xph.us"]
  gem.summary       = "Ruby client for beanstalkd"
  gem.description   = "Ruby client for beanstalkd."
  gem.homepage      = "http://github.com/kr/beanstalk-client-ruby"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency 'minitest', "~> 2.6.1"
  gem.add_development_dependency 'rake'
end
