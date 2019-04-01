require "./helper"

describe Neuroplastic::Query do
  it "builds an elasticsearch query" do
    query_body = Basic.elastic.query.build
    query_body.keys.should eq ({:query, :filter, :offset, :limit, :sort})
  end

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
