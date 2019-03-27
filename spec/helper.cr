require "spec"

require "elasticsearch-crystal/elasticsearch/api"
require "rethinkdb-orm"
require "rubber-soul"

require "../src/neuroplastic"
require "../src/neuroplastic/*"

# spec models
####################################################################################################
class Base < RethinkORM::Base
  include Neuroplastic
end

class Basic < Base
  attribute name : String
end

class Parent < Base
  attribute name : String
end

class Child < Base
  attribute age : Int32
  belongs_to Parent
end

# ES
####################################################################################################

macro indices(models)
  [
  {% for model in models %}
  {{ model.id }}.table_name,
  {% end %}
  ]
end

INDICES = indices([Basic, Parent, Child])
CLIENT  = Elasticsearch::API::Client.new({:host => "localhost", :port => 9200})

def recreate_index(index)
  CLIENT.indices.delete({:index => index})
  CLIENT.indices.create({:index => index})
end

def recreate_test_indices
  INDICES.each do |i|
    recreate_index(i)
  end
end

def create_es_data(index, body, routing)
  CLIENT.create({
    :type  => "_doc",
    :index => index,
    :body  => body,
  })
end

recreate_test_indices
