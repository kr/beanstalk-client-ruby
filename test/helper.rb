ENV["TEST"] = 'true'
require 'rubygems'
require 'minitest/autorun'
$:.unshift File.expand_path("../../lib")
require 'beanstalk-client'