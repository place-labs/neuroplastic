require "./helper"

describe Neuroplastic::Query do
  it "builds an elasticsearch query" do
    query_body = Basic.elastic.query.build
    query_body.keys.should eq ({:query, :filter, :offset, :limit, :sort})
  end

  describe "asscociations" do
    it "#has_parent" do
      parent_index = Goat.table_name
      query = Kid.elastic.query({q: "some goat"}).has_parent(parent: Goat, parent_index: parent_index)
      query_body = query.build[:query]

      query.parent.should eq Goat.name
      query.index.should eq parent_index

      JSON.parse(query_body.to_json).dig("should", 0, "has_parent", "parent_type").should eq Goat.name
    end

    it "#has_child" do
      query = Goat.elastic.query({q: "some kid"}).has_child(child: Kid)
      query_body = query.build[:query]

      JSON.parse(query_body.to_json).dig("should", 0, "has_child", "type").should eq Kid.name
    end
  end

  describe "filters" do
    it "#should" do
      query = Goat.elastic.query({q: "SCREAMS"})
      teeth = [1, 3, 5, 7, 11]

      query.should({"teeth" => teeth})
      filter_field = query.build[:filter]

      expected = teeth.map { |t| ({:term => {"teeth" => t}}) }
      filter_field.dig(:bool, :should).should eq expected
    end

    it "#must" do
      query = Goat.elastic.query({q: "stands on mountain"})
      query.must({"name" => ["billy"]})
      filter_field = query.build[:filter]

      filter_field.dig(:bool, :must).should eq [{:term => {"name" => "billy"}}]
    end

    it "#must_not" do
      query = Goat.elastic.query({q: "makes good cheese"})
      query.must_not({"name" => ["gruff"]})
      filter_field = query.build[:filter]

      filter_field.dig(:bool, :must_not).should eq [{:term => {"name" => "gruff"}}]
    end

    it "#range" do
      query = Goat.elastic.query({q: "cheese time"})
      query.range({
        "teeth" => {
          :lte => 5,
        },
      })
      filter_field = query.build[:filter]
      filter_field.dig(:bool, :filter).should eq [{range: {"teeth" => {:lte => 5}}}]
    end
  end
end
