# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'hbase-jruby/version'

Gem::Specification.new do |gem|
  gem.name          = "hbase-jruby"
  gem.version       = HBase::JRuby::VERSION
  gem.authors       = ["Junegunn Choi"]
  gem.email         = ["junegunn.c@gmail.com"]
  gem.description   = %q{A JRuby binding for HBase}
  gem.summary       = %q{A JRuby binding for HBase}
  gem.homepage      = "https://github.com/junegunn/hbase-jruby"
  gem.platform      = 'java'
  gem.license       = 'MIT'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency 'test-unit'
  gem.add_development_dependency 'simplecov'
end
