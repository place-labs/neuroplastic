require "./helper"

describe Neuroplastic::Elastic do
  describe "#count" do
    it "performs a count query on an index" do
      recreate_index(Basic.table_name)
      query = Basic.elastic.query
      count = Basic.elastic.count(query)
      count.should eq 0
    end
  end

  describe "#search" do
    it "performs a generic search" do
      recreate_index(Basic.table_name)
      query = Basic.elastic.query
      records = Basic.elastic.search(query)
      records[:total].should eq 0
      records[:results].size.should eq 0
    end

    it "accepts a format block" do
      recreate_index(Basic.table_name)
      query = Basic.elastic.query
      records = Basic.elastic.search(query) { |r| r }
      records[:total].should eq 0
      records[:results].size.should eq 0
    end
  end

  pending "has_parent query" do
    it "queries the parent index" do
    end
  end

  pending "has_child query" do
  end
end
