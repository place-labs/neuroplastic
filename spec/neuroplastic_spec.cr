require "./helper"

NAME_SORT_ASC = {"name.keyword" => {order: :asc}}

describe Neuroplastic::Elastic do
  describe "#count" do
    it "performs a count query on an index" do
      query = Base.elastic.query
      count = Base.elastic.count(query)
      count.should eq 1
    end
  end

  describe "#search" do
    it "performs a generic search" do
      query = Basic.elastic.query
      records = Basic.elastic.search(query)
      records[:total].should eq 3
      records[:results].size.should eq 3
    end

    it "accepts a format block" do
      query = Basic.elastic.query
      updated_name = "Ugg"
      records = Basic.elastic.search(query) do |r|
        r.name = updated_name
        r
      end

      records[:total].should eq 3
      records[:results].size.should eq 3
      records[:results][0].name.should eq updated_name
    end

    it "should paginate" do
      query = Basic.elastic.query({"limit" => "1"})
      query.sort NAME_SORT_ASC
      records = Basic.elastic.search(query)
      records[:total].should eq 3
      records[:results].size.should eq 1
      records[:ref].should_not be_nil
      first_result = records[:results].first.name

      # Grab the next page using search_after
      query = Basic.elastic.query({"limit" => "1", "ref" => records[:ref].not_nil!})
      query.sort NAME_SORT_ASC
      records = Basic.elastic.search(query)
      records[:total].should eq 3
      records[:results].size.should eq 1
      records[:ref].should_not be_nil
      second_result = records[:results].first.name

      first_result.should_not eq second_result

      # Grab the 3rd page of results
      query = Basic.elastic.query({"limit" => "1", "ref" => records[:ref].not_nil!})
      query.sort NAME_SORT_ASC
      records = Basic.elastic.search(query)
      records[:total].should eq 3
      records[:results].size.should eq 1
      final_result = records[:results].first.name

      final_result.in?({first_result, second_result}).should be_false
    end

    it "#must_not on a embedded document" do
      elastic = Child::Kid.elastic
      query = elastic.query
      query.must_not({"visits" => ["monthly"]})

      records = elastic.search(query)
      records[:total].should eq 0
    end

    it "#must_not on a embedded document" do
      elastic = Child::Kid.elastic
      query = elastic.query
      query.must({"visits" => ["monthly", "yearly"]})

      records = elastic.search(query)
      records[:total].should eq 1
    end
  end

  describe "relations" do
    it "#count returns correct count for associated models" do
      query = Goat.elastic.query
      count = Goat.elastic.count(query)
      count.should eq 1
    end

    it "#query.has_parent performs a has_parent query against the parent index" do
      query = Child::Kid.elastic.query({"q" => "bill"}).has_parent(parent: Goat, parent_index: Goat.table_name)
      records = Child::Kid.elastic.search(query)
      records[:total].should eq 1
      records[:results].size.should eq 1
    end

    it "#query.has_child performs a has_child query" do
      query = Goat.elastic.query({"q" => "cuso4"}).has_child(child: Child::Kid)
      records = Goat.elastic.search(query)
      records[:total].should eq 1
      records[:results].size.should eq 1
    end
  end
end
