require "./helper"

NAME_SORT_ASC = {"name.keyword" => {order: :asc}}

describe Neuroplastic::Elastic do
  describe "#count" do
    it "performs a count query on an index" do
      query = TestBase.elastic.query
      count = TestBase.elastic.count(query)
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

    it "limits the search to specific fields" do
      goat = Goat.create!(name: "Kim", job: "Big goat with 5 teeth", teeth: 5)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      goat = Goat.create!(name: "Kylie", job: "Fat goat with 3 teeth", teeth: 3)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      goat = Goat.create!(name: "Kendall Fat", job: "Big goat with 5 teeth", teeth: 5)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      sleep 1

      query = Goat.elastic.query({"q" => "Fat", "fields" => ["name"]})
      records = Goat.elastic.search(query)
      records[:total].should eq(1)

      query = Goat.elastic.query({"q" => "Fat", "fields" => ["name", "job^3"]})
      records = Goat.elastic.search(query)
      records[:total].should eq(2)

      query = Goat.elastic.query({"q" => "goat", "fields" => ["name"]})
      records = Goat.elastic.search(query)
      records[:total].should eq(0)
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

    it "#should with nil matches documents where field is missing" do
      # Create a goat WITH a nickname (lowercase to match ES text analysis)
      goat_with = Goat.create!(name: "Named Goat", nickname: "nanny", teeth: 99)
      SearchIngest::Elastic.create_document(
        document: goat_with,
        index: Goat.table_name,
      )

      # Create a goat WITHOUT a nickname (nil)
      goat_without = Goat.create!(name: "Unnamed Goat", teeth: 99)
      SearchIngest::Elastic.create_document(
        document: goat_without,
        index: Goat.table_name,
      )

      sleep 1

      elastic = Goat.elastic
      query = elastic.query
      query.should({"nickname" => ["nanny", nil]})
      query.minimum_should_match(1)

      records = elastic.search(query)
      ids = records[:results].map(&.id)
      ids.should contain(goat_with.id)
      ids.should contain(goat_without.id)
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
      # Query for the goat created in setup (name contains "bill")
      query = Goat.elastic.query({"q" => "bill"})
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
      # Search for goats with teeth=0 (only the setup goat) that have children matching "cuso4"
      query = Goat.elastic.query({"q" => "cuso4"}).has_child(child: Child::Kid)
      query.filter({"teeth" => [0]})
      records = Goat.elastic.search(query)
      records[:total].should eq 1
      records[:results].size.should eq 1
    end

    it "#query with terms" do
      query = Basic.elastic.query({"fields" => ["name.keyword"]})
      query.filter({"$name.keyword" => ["Kim", "Kyle"]})
      records = Basic.elastic.search(query)
      records[:total].should eq 0
    end
  end
end
