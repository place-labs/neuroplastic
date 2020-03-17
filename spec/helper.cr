require "spec"

require "../src/neuroplastic"
require "../src/neuroplastic/*"

require "./spec_models"
require "rubber-soul/rubber-soul/table_manager"

# ES
####################################################################################################

RubberSoul::MANAGED_TABLES = [Base, Basic, Goat, Child::Kid] # ameba:disable Style/ConstantNames
TM         = RubberSoul::TableManager.new(watch: false, backfill: false)

CLIENT = Elasticsearch::API::Client.new({:host => "localhost", :port => 9200})

def recreate_test_indices
  RubberSoul::MANAGED_TABLES.each { |k| TM.reindex(k) }
end

# Creates a random parent child across the child and parent indices
def create_parent_child
  parent = Goat.create(name: "bill the #{Random.rand(100)}th")
  child = Child::Kid.new(age: Random.rand(18), hoof_treatment: "CuSO4", visits: ["yearly", "monthly"])
  child.goat = parent
  child.save

  parent_name = Neuroplastic::Utils.document_name(Goat)
  child_name = Neuroplastic::Utils.document_name(Child::Kid)
  parent_index = Goat.table_name
  child_index = Child::Kid.table_name

  RubberSoul::Elastic.create_document(
    document: parent,
    index: parent_index,
    parents: TM.parents(parent_name),
    no_children: TM.children(parent_name).empty?
  )

  RubberSoul::Elastic.create_document(
    document: child,
    index: child_index,
    parents: TM.parents(child_name),
    no_children: TM.children(child_name).empty?
  )
end

def create_basic
  basic = Basic.create!(name: {"Kim", "Kylie", "Kendall"}.sample)
  RubberSoul::Elastic.create_document(
    document: basic,
    index: Basic.table_name,
  )
end

def create_base
  base = Base.create!
  RubberSoul::Elastic.create_document(
    document: base,
    index: Base.table_name,
  )
end

recreate_test_indices
create_parent_child
create_basic
create_base

sleep 1
