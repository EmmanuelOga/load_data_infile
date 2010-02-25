SPEC_PATH = File.dirname(__FILE__)
$LOAD_PATH.unshift(SPEC_PATH)
$LOAD_PATH.unshift(File.join(SPEC_PATH, '..', 'lib'))
require 'spec'
require 'spec/autorun'
require 'rubygems'
require 'active_record'
require 'load_data_infile'
require 'active_record_helper'
require File.join(SPEC_PATH, "..", "rails", "init.rb")

FIXTURE_WITH_HEADERS = File.join(SPEC_PATH, "fixtures", "csv_with_headers.csv")
FIXTURE_WITHOUT_HEADERS = File.join(SPEC_PATH, "fixtures", "csv_without_headers.csv")

Spec::Runner.configure do |config|
end
