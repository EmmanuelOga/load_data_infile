require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe LoadDataInfile do
  before :each do
    Thing.truncate_table
  end

  it "loads data from a csv file with headers into an ActiveRecord table" do
    Thing.with_keys_disabled do
      Thing.load_data_infile(
        :path          => FIXTURE_WITH_HEADERS,
        :columns       => %w|id field_a field_b field_c|,
        :terminated_by => ",",
        :ignore        => 1
      )
    end
    Thing.all.map(&:attributes).should == [{
      "id"      => 71,
      "field_a" => "Hello",
      "field_b" => "Brother",
      "field_c" => 42
    }]
  end

  it "loads data from a csv file without headers into an ActiveRecord table" do
    Thing.with_keys_disabled do
      Thing.load_data_infile(
        :path          => FIXTURE_WITHOUT_HEADERS,
        :terminated_by => ",",
        :columns       => %w|id field_a field_b field_c|
      )
    end
    Thing.all.map(&:attributes).should == [{
      "id"      => 61,
      "field_a" => "live",
      "field_b" => "from",
      "field_c" => 2400
    }]
  end
end
