require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe LoadDataInfile do
  before :each do
    Thing.truncate_table
  end

  it "loads data from a csv file with headers into an ActiveRecord table" do
    Thing.load_data_infile(
      :path          => FIXTURE_WITH_HEADERS,
      :columns       => %w|id field_a field_b field_c|,
      :terminated_by => ",",
      :ignore        => 1
    )
    Thing.all.map(&:attributes).should == [{
      "id"      => 71,
      "field_a" => "Hello",
      "field_b" => "Brother",
      "field_c" => 42
    }]
  end

  it "loads data from a csv file without headers into an ActiveRecord table" do
    Thing.load_data_infile(
      :path          => FIXTURE_WITHOUT_HEADERS,
      :terminated_by => ",",
      :columns       => %w|id field_a field_b field_c|
    )
    Thing.all.map(&:attributes).should == [{
      "id"      => 61,
      "field_a" => "live",
      "field_b" => "from",
      "field_c" => 2400
    }]
  end

  it "loads data from a csv file with mapping" do
    Thing.load_data_infile(
      :path          => FIXTURE_WITHOUT_HEADERS,
      :terminated_by => ",",
      :columns       => %w|id @field_a @field_b @field_c|,
      :mappings      => {
                          :field_a => "CONCAT('So ', @field_a)",
                          :field_b => "CONCAT('Much ', @field_b)",
                          :field_c => "@field_c * 10",
                        }
    )
    Thing.all.map(&:attributes).should == [{
      "id"      => 61,
      "field_a" => "So live",
      "field_b" => "Much from",
      "field_c" => 24000
    }]
  end
end
