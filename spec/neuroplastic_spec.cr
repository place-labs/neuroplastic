require "./helper"

describe Neuroplastic::Elastic do
  describe "#count" do
    it "performs a count query on an index" do
      TM.reindex(Basic.name)
      query = Basic.elastic.query
      count = Basic.elastic.count(query)
      count.should eq 0
    end
  end

  describe "#search" do
    it "performs a generic search" do
      TM.reindex(Basic.name)
      query = Basic.elastic.query
      records = Basic.elastic.search(query)
      records[:total].should eq 0
      records[:results].size.should eq 0
    end

    it "accepts a format block" do
      TM.reindex(Basic.name)
      query = Basic.elastic.query
      records = Basic.elastic.search(query) { |r| r }
      records[:total].should eq 0
      records[:results].size.should eq 0
    end
  end

  describe "relations" do
    it "#query.has_parent performs a has_parent query against the parent index" do
      query = Kid.elastic.query({q: "bill"}).has_parent(parent: Goat, parent_index: Goat.table_name)
      records = Kid.elastic.search(query)
      records[:total].should eq 1
      records[:results].size.should eq 1
    end

    it "#query.has_child performs a has_child query" do
      query = Goat.elastic.query({q: "cuso4"}).has_child(child: Kid)
      records = Goat.elastic.search(query)
      records[:total].should eq 1
      records[:results].size.should eq 1
    end
  end
end
