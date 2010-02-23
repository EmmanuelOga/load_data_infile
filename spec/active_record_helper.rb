ActiveRecord::Base.establish_connection(:adapter => "mysql", :database => "load_data_infile_test", :user => "root", :password => "")

ActiveRecord::Base.logger = Logger.new(STDOUT)

ActiveRecord::Schema.define do
  create_table "things", :force => true do |t|
    t.string :field_a, :field_b
    t.integer :field_c
  end
end

class Thing < ActiveRecord::Base
end
