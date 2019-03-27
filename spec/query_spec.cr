require "./helper"

describe Neuroplastic::Elastic::Query do
  it "builds an elasticsearch query" do
    elastic = Neuroplastic::Elastic(Basic).new
    query_body = elastic.query.build
    query_body.keys.should eq ({:query, :sort, :filters, :offset, :limit})
    pp! query_body
  end
end
