require "./helper"

describe Neuroplastic::Elastic do
  pending "#count" do
  end

  pending "#search" do
    recreate_index(BasicModel.table_name)
    query = Basic.elastic.query
    Basic.elastic.search(query)
  end

  pending "has_parent query" do
    it "queries the parent index" do
    end
  end

  pending "has_child query" do
  end
end
