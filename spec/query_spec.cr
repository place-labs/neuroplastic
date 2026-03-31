require "./helper"

describe Neuroplastic::Query do
  it "builds an elasticsearch query" do
    query_body = Basic.elastic.query.build
    query_body.keys.should eq({:query, :filter, :offset, :limit, :sort, :search_after, :aggs})
    query_body[:aggs]?.should be_nil
  end

  describe "asscociations" do
    it "#has_parent" do
      parent_index = Goat.table_name
      query = Child::Kid.elastic.query({"q" => "some goat"}).has_parent(parent: Goat, parent_index: parent_index)
      query_body = query.build[:query]

      goat_document_type = Neuroplastic::Utils.document_name(Goat)

      query.parent.should eq goat_document_type
      query.index.should eq parent_index
      JSON.parse(query_body.to_json).dig("should", 1, "has_parent", "parent_type").should eq goat_document_type
    end

    it "#has_child" do
      query = Goat.elastic.query({"q" => "some kid"}).has_child(child: Child::Kid)
      query_body = query.build[:query]

      JSON.parse(query_body.to_json).dig("should", 1, "has_child", "type").should eq Neuroplastic::Utils.document_name(Child::Kid)
    end
  end

  describe "filters" do
    it "#should" do
      query = Goat.elastic.query({"q" => "SCREAMS"})
      teeth = [1, 3, 5, 7, 11]

      query.should({"teeth" => teeth})
      filter_field = query.build[:filter].not_nil!

      expected = teeth.map { |t| ({:term => {"teeth" => t}}) }
      filter_field.dig(:filter, :bool, :should).should eq expected
    end

    it "#should with nil values" do
      query = Goat.elastic.query({"q" => "SCREAMS"})
      teeth = [1, nil] of Int32?

      query.should({"teeth" => teeth})
      query.minimum_should_match(1)
      filter_field = query.build[:filter].not_nil!

      should_clauses = filter_field.dig(:filter, :bool, :should)
      parsed = JSON.parse(should_clauses.to_json)

      # Should contain a term filter for value 1
      parsed[0].should eq({"term" => {"teeth" => 1}})
      # Should contain a must_not exists filter for nil
      parsed[1].should eq({"bool" => {"must_not" => {"exists" => {"field" => "teeth"}}}})
    end

    it "#must" do
      query = Goat.elastic.query({"q" => "stands on mountain"})
      query.must({"name" => ["billy"]})
      filter_field = query.build[:filter].not_nil!

      filter_field.dig(:filter, :bool, :must).should eq [{:term => {"name" => "billy"}}]
    end

    it "#must_not" do
      query = Goat.elastic.query({"q" => "makes good cheese"})
      query.must_not({"name" => ["gruff"]})
      filter_field = query.build[:filter].not_nil!

      filter_field.dig(:filter, :bool, :must_not).should eq [{:term => {"name" => "gruff"}}]
    end

    it "#range" do
      query = Goat.elastic.query({"q" => "cheese time"})
      query.range({
        "teeth" => {
          :lte => 5,
        },
      })
      filter_field = query.build[:filter].not_nil!
      bool_filter = filter_field.dig(:filter, :bool, :filter).as(Array)
      bool_filter.should contain({range: {"teeth" => {:lte => 5}}})
    end

    it "#fields" do
      query = Goat.elastic.query({"q" => "makes good cheese", "fields" => ["name", "teeth"]})
      query_body = query.build[:query]
      JSON.parse(query_body.to_json).dig("must", "simple_query_string", "fields").should eq ["name", "teeth"]
    end
  end

  describe "aggregations" do
    it "#terms" do
      query = Child::Kid.elastic.query
      query.terms("children_by_parent", "goat_id", size: 10_000)

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.should eq(
        {
          "children_by_parent" => {
            "terms" => {
              "field" => "goat_id",
              "size"  => 10_000,
            },
          },
        }
      )
    end

    it "supports metric aggregations" do
      query = Goat.elastic.query
      query.avg("avg_teeth", "teeth")
      query.stats("teeth_stats", "teeth")

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("avg_teeth", "avg", "field").should eq "teeth"
      aggs.dig("teeth_stats", "stats", "field").should eq "teeth"
    end

    it "supports nested aggregations" do
      query = Goat.elastic.query
      query.terms("jobs", "job.keyword", size: 10) do |agg|
        agg.avg("avg_teeth", "teeth")
      end

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("jobs", "aggs", "avg_teeth", "avg", "field").should eq "teeth"
    end

    it "supports aggregation range and filter range together" do
      query = Goat.elastic.query({"q" => "range agg"})
      query.range({
        "teeth" => {
          :gte => 2,
        },
      })
      query.range("teeth_bands", "teeth", [{"to" => 5}, {"from" => 5}])

      built = query.build
      JSON.parse(built[:aggs].not_nil!.to_json).dig("teeth_bands", "range", "field").should eq "teeth"
      JSON.parse(built[:filter].not_nil!.to_json).dig("filter", "bool", "filter", 1, "range", "teeth", "gte").should eq 2
    end

    it "supports date_histogram" do
      query = Goat.elastic.query
      query.date_histogram("created", "created_at", calendar_interval: "day", min_doc_count: 0)

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("created", "date_histogram", "calendar_interval").should eq "day"
      aggs.dig("created", "date_histogram", "field").should eq "created_at"
    end

    it "supports filter aggregations" do
      query = Goat.elastic.query
      query.filter("has_teeth", {"teeth" => [5]}) do |agg|
        agg.avg("avg_teeth", "teeth")
      end

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("has_teeth", "filter", "bool", "filter", 0, "term", "teeth").should eq 5
      aggs.dig("has_teeth", "aggs", "avg_teeth", "avg", "field").should eq "teeth"
    end

    it "supports filters aggregations" do
      query = Goat.elastic.query
      query.filters("jobs", {
        "a" => {"job.keyword" => ["scope-a"]},
        "b" => {"job.keyword" => ["scope-b"]},
      }, other_bucket: true, other_bucket_key: "other")

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("jobs", "filters", "filters", "a", "bool", "filter", 0, "term", "job.keyword").should eq "scope-a"
      aggs.dig("jobs", "filters", "other_bucket").should eq true
      aggs.dig("jobs", "filters", "other_bucket_key").should eq "other"
    end

    it "supports nested and reverse_nested aggregations" do
      query = Goat.elastic.query
      query.nested("zones", "zones") do |agg|
        agg.terms("types", "zones.type.keyword", size: 10) do |subagg|
          subagg.reverse_nested("buildings") do |root|
            root.terms("by_building", "building.keyword", size: 10)
          end
        end
      end

      aggs = JSON.parse(query.build[:aggs].not_nil!.to_json)
      aggs.dig("zones", "nested", "path").should eq "zones"
      aggs.dig("zones", "aggs", "types", "aggs", "buildings", "reverse_nested").as_h.empty?.should be_true
      aggs.dig("zones", "aggs", "types", "aggs", "buildings", "aggs", "by_building", "terms", "field").should eq "building.keyword"
    end
  end
end
