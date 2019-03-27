require "./helper"

describe Neuroplastic::Elastic::Query do
  it "builds an elasticquery from query params" do
    elastic = Neuroplastic::Elastic(Basic).new
    query = elastic.query
    q = query.build
    pp! q
  end
end
