require "./helper"

describe Neuroplastic::Query do
  it "builds an elasticsearch query" do
    elastic = Basic.elastic
    query_body = elastic.query.build
    query_body.keys.should eq ({:query, :sort, :filters, :offset, :limit})
    pp! query_body
  end
end
