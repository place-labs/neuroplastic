require "spec"

require "../src/neuroplastic"
require "../src/neuroplastic/*"

require "./spec_models"
require "search-ingest/search-ingest/table_manager"

# ES
####################################################################################################

SearchIngest::MANAGED_TABLES = [Base, Basic, Goat, Child::Kid]
TableManager = SearchIngest::TableManager.new(watch: false, backfill: false)
Schemas      = SearchIngest::Schemas.new

def recreate_test_indices
  SearchIngest::MANAGED_TABLES.each { |k| TableManager.reindex(k) }
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

  SearchIngest::Elastic.create_document(
    document: parent,
    index: parent_index,
    parents: Schemas.parents(parent_name),
    no_children: Schemas.children(parent_name).empty?
  )

  SearchIngest::Elastic.create_document(
    document: child,
    index: child_index,
    parents: Schemas.parents(child_name),
    no_children: Schemas.children(child_name).empty?
  )
end

def create_basic
  basic = Basic.create!(name: {"Kim", "Kylie", "Kendall"}.sample)
  SearchIngest::Elastic.create_document(
    document: basic,
    index: Basic.table_name,
  )
end

def create_base
  base = Base.create!
  SearchIngest::Elastic.create_document(
    document: base,
    index: Base.table_name,
  )
end

Spec.before_suite do
  ::Log.setup("*", level: :debug)

  recreate_test_indices
  create_parent_child
  create_basic
  create_base
  sleep 1
end
