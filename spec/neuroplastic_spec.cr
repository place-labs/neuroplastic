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
      token = "fat#{Random.rand(1_000_000)}"
      job_only_token = "jobonly#{Random.rand(1_000_000)}"

      goat = Goat.create!(name: "Kim", job: "Big goat #{job_only_token} with 5 teeth", teeth: 5)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      goat = Goat.create!(name: "#{token} Kylie", job: "Goat with 3 teeth", teeth: 3)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      goat = Goat.create!(name: "Kendall", job: "Big goat #{token} with 5 teeth", teeth: 5)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      records = Goat.elastic.search(query)
      records[:total].should eq(1)

      query = Goat.elastic.query({"q" => token, "fields" => ["name", "job^3"]})
      records = Goat.elastic.search(query)
      records[:total].should eq(2)

      query = Goat.elastic.query({"q" => job_only_token, "fields" => ["name"]})
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

      sleep 1.seconds

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

    it "returns aggregations with hydrated results" do
      token = "aggreturn#{Random.rand(1_000_000)}"
      goat = Goat.create!(name: "#{token} goat", job: "#{token}-job", teeth: 14)
      SearchIngest::Elastic.create_document(
        document: goat,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.terms("jobs", "job.keyword", size: 10)
      records = Goat.elastic.search(query)

      records[:results].size.should eq 1
      records[:aggregations].should_not be_nil
      records[:aggregations].not_nil!.dig("jobs", "buckets", 0, "key").should eq "#{token}-job"
    end

    it "supports metric and missing aggregations" do
      token = "aggmetric#{Random.rand(1_000_000)}"
      goat_with = Goat.create!(name: "#{token} with", nickname: "present", teeth: 12)
      SearchIngest::Elastic.create_document(
        document: goat_with,
        index: Goat.table_name,
      )

      goat_without = Goat.create!(name: "#{token} without", teeth: 4)
      SearchIngest::Elastic.create_document(
        document: goat_without,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.avg("avg_teeth", "teeth")
      query.max("max_teeth", "teeth")
      query.stats("teeth_stats", "teeth")
      query.missing("missing_nickname", "nickname.keyword")

      records = Goat.elastic.search(query)
      aggs = records[:aggregations].not_nil!

      aggs.dig("avg_teeth", "value").as_f.should eq 8.0
      aggs.dig("max_teeth", "value").as_f.should eq 12.0
      aggs.dig("teeth_stats", "count").as_i.should eq 2
      aggs.dig("teeth_stats", "sum").as_f.should eq 16.0
      aggs.dig("missing_nickname", "doc_count").as_i.should eq 1
    end

    it "scopes aggregations to the active query and filters" do
      token = "aggscope#{Random.rand(1_000_000)}"
      goat_one = Goat.create!(name: "#{token} one", job: "scope-a", teeth: 41)
      SearchIngest::Elastic.create_document(
        document: goat_one,
        index: Goat.table_name,
      )

      goat_two = Goat.create!(name: "#{token} two", job: "scope-a", teeth: 42)
      SearchIngest::Elastic.create_document(
        document: goat_two,
        index: Goat.table_name,
      )

      goat_three = Goat.create!(name: "#{token} three", job: "scope-b", teeth: 99)
      SearchIngest::Elastic.create_document(
        document: goat_three,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.must_not({"teeth" => [99]})
      query.terms("jobs", "job.keyword", size: 10)

      records = Goat.elastic.search(query)
      buckets = records[:aggregations].not_nil!.dig("jobs", "buckets").as_a

      buckets.size.should eq 1
      buckets[0]["key"].should eq "scope-a"
      buckets[0]["doc_count"].should eq 2
    end

    it "returns aggregations when there are no matching results" do
      query = Goat.elastic.query({"q" => "aggnoresults", "fields" => ["name"]})
      query.terms("jobs", "job.keyword", size: 10)

      records = Goat.elastic.search(query)

      records[:total].should eq 0
      records[:results].should be_empty
      records[:aggregations].should_not be_nil
      records[:aggregations].not_nil!.dig("jobs", "buckets").as_a.should be_empty
    end

    it "supports nested aggregations" do
      token = "aggnested#{Random.rand(1_000_000)}"
      goat_one = Goat.create!(name: "#{token} one", job: "nested-job", teeth: 11)
      SearchIngest::Elastic.create_document(
        document: goat_one,
        index: Goat.table_name,
      )

      goat_two = Goat.create!(name: "#{token} two", job: "nested-job", teeth: 5)
      SearchIngest::Elastic.create_document(
        document: goat_two,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.terms("jobs", "job.keyword", size: 10) do |agg|
        agg.avg("avg_teeth", "teeth")
      end

      records = Goat.elastic.search(query)
      bucket = records[:aggregations].not_nil!.dig("jobs", "buckets", 0)

      bucket["key"].should eq "nested-job"
      bucket.dig("avg_teeth", "value").as_f.should eq 8.0
    end

    it "supports filter aggregations" do
      token = "aggfilter#{Random.rand(1_000_000)}"
      goat_one = Goat.create!(name: "#{token} one", job: "filter-job", teeth: 21)
      SearchIngest::Elastic.create_document(
        document: goat_one,
        index: Goat.table_name,
      )

      goat_two = Goat.create!(name: "#{token} two", job: "filter-job", teeth: 3)
      SearchIngest::Elastic.create_document(
        document: goat_two,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.filter("high_teeth", {"teeth" => [21]}) do |agg|
        agg.avg("avg_teeth", "teeth")
      end

      records = Goat.elastic.search(query)
      high_teeth = records[:aggregations].not_nil!.dig("high_teeth")

      high_teeth["doc_count"].as_i.should eq 1
      high_teeth.dig("avg_teeth", "value").as_f.should eq 21.0
    end

    it "supports filters aggregations" do
      token = "aggfilters#{Random.rand(1_000_000)}"
      goat_one = Goat.create!(name: "#{token} one", job: "filters-a", teeth: 31)
      SearchIngest::Elastic.create_document(
        document: goat_one,
        index: Goat.table_name,
      )

      goat_two = Goat.create!(name: "#{token} two", job: "filters-a", teeth: 32)
      SearchIngest::Elastic.create_document(
        document: goat_two,
        index: Goat.table_name,
      )

      goat_three = Goat.create!(name: "#{token} three", job: "filters-b", teeth: 33)
      SearchIngest::Elastic.create_document(
        document: goat_three,
        index: Goat.table_name,
      )

      sleep 1.seconds

      query = Goat.elastic.query({"q" => token, "fields" => ["name"]})
      query.filters("job_groups", {
        "group_a" => {"job.keyword" => ["filters-a"]},
        "group_b" => {"job.keyword" => ["filters-b"]},
      }, other_bucket: true, other_bucket_key: "other")

      records = Goat.elastic.search(query)
      buckets = records[:aggregations].not_nil!.dig("job_groups", "buckets")

      buckets.dig("group_a", "doc_count").as_i.should eq 2
      buckets.dig("group_b", "doc_count").as_i.should eq 1
      buckets.dig("other", "doc_count").as_i.should eq 0
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

    it "aggregates children by parent id" do
      parent_one = Goat.create!(name: "aggparent one")
      parent_two = Goat.create!(name: "aggparent two")

      [parent_one, parent_two].each do |parent|
        SearchIngest::Elastic.create_document(
          document: parent,
          index: Goat.table_name,
          parents: Schemas.parents(Neuroplastic::Utils.document_name(Goat)),
          no_children: Schemas.children(Neuroplastic::Utils.document_name(Goat)).empty?
        )
      end

      [parent_one, parent_one, parent_two].each do |parent|
        child = Child::Kid.new(age: 7, hoof_treatment: "aggparentchild", visits: ["quarterly"])
        child.goat = parent
        child.save

        SearchIngest::Elastic.create_document(
          document: child,
          index: Child::Kid.table_name,
          parents: Schemas.parents(Neuroplastic::Utils.document_name(Child::Kid)),
          no_children: Schemas.children(Neuroplastic::Utils.document_name(Child::Kid)).empty?
        )
      end

      sleep 1.seconds

      query = Child::Kid.elastic.query
      query.must({"hoof_treatment" => ["aggparentchild"]})
      query.terms("children_by_parent", "goat_id", size: 10_000)
      records = Child::Kid.elastic.search(query)
      buckets = records[:aggregations].not_nil!.dig("children_by_parent", "buckets").as_a

      bucket_counts = buckets.to_h do |bucket|
        {bucket["key"].as_s, bucket["doc_count"].as_i}
      end

      bucket_counts[parent_one.id.to_s].should eq 2
      bucket_counts[parent_two.id.to_s].should eq 1
    end
  end
end
