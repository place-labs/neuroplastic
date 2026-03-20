require "./utils"
require "base64"

module Neuroplastic
  class Query
    alias Sort = Hash(String, NamedTuple(order: Symbol)) | String | Hash(String, String)
    alias FilterValue = Array(Int32) | Array(Int32?) | Array(Float32) | Array(Float32?) | Array(Bool) | Array(Bool?) | Array(String) | Array(String?) | Nil
    alias Filter = Hash(String, FilterValue)

    getter search : String

    getter offset : Int32

    getter limit : Int32

    alias SearchAfter = Array(String | Int64 | Float64)
    getter search_after : SearchAfter?

    getter query_settings : Hash(String, String)?

    getter sort : Array(Sort) = [] of Sort

    getter fields : Array(String) = [] of String

    private def get_or_default(params, key, default_value)
      case typeof(default_value)
      when String then params[key]?.as(String) || default_value
      when Int    then params[key]?.as(String).to_i || default_value
      end
    end

    def initialize(params : HTTP::Params | Hash(String, String | Array(String)) | Hash(Symbol, String | Array(String)) = {} of String => String)
      params = params.transform_keys(&.to_s) if params.is_a?(Hash(Symbol, String | Array(String)))

      query = params["q"]?.as?(String) || "*"
      @search = query.ends_with?('*') ? query : "#{query}*"

      @limit = params["limit"]?.as?(String).try(&.to_i) || 100
      @limit = 10000 if @limit > 10000

      @offset = params["offset"]?.as?(String).try(&.to_i) || 0
      @offset = 1000000 if @offset > 1000000

      if search_ref = params["ref"]?.as?(String)
        @search_after = SearchAfter.from_json(Base64.decode_string(search_ref))
      else
        @search_after = nil
      end

      if (fields = params["fields"]?) && fields.is_a?(Array(String))
        @fields = fields.as(Array(String))
      end
    end

    def initialize(@search : String, @limit : Int32 = 100, @offset : Int32 = 0)
      @search = @search.ends_with?('*') ? @search : "#{@search}*"

      @limit = 10000 if @limit > 10000
    end

    def search_field(field)
      fields.unshift(field)
      self
    end

    getter child : String?
    getter parent : String?
    getter index : String?

    # Applies the query to child objects
    def has_child(child : Class)
      @child = Utils.document_name(child)

      self
    end

    # "has_parent" query.
    #
    # Sets the index to the parent
    def has_parent(parent : Class, parent_index : String)
      @parent = Utils.document_name(parent)
      @index = parent_index

      self
    end

    getter minimum_should_match : Int32? = nil

    def minimum_should_match(count : Int)
      @minimum_should_match = count.to_i
      self
    end

    # Filters
    ###############################################################################################

    getter filters : Filter do
      Filter.new
    end

    getter should : Filter do
      Filter.new
    end

    getter must_not : Filter do
      Filter.new
    end

    getter must : Filter do
      Filter.new
    end

    getter range : RangeQuery do
      RangeQuery.new
    end

    def filter(filters : Filter)
      self.filters.merge!(filters)

      self
    end

    # Like filter, but at least one should match in absence of `filter`/`must` hits
    def should(filters : Filter)
      self.should.merge!(filters)

      self
    end

    # Like filter, but all hits must match each filter
    def must(filters : Filter)
      self.must.merge!(filters)

      self
    end

    # The opposite of filter, essentially a not
    def must_not(filters : Filter)
      self.must_not.merge!(filters)

      self
    end

    def sort(sort : Sort)
      self.sort << sort

      self
    end

    alias RangeQuery = Hash(String, Hash(Symbol, RangeValue))
    alias RangeValue = Int32 | Float32 | Bool | String | Int64 | Float64

    RANGE_PARAMS = {:gte, :gt, :lte, :lt, :boost}

    def range(filter)
      invalid_args = filter.values.flat_map(&.keys).reject do |k|
        RANGE_PARAMS.includes? k
      end

      # Crystal fails to merge Hash(String, Int) into Hash(String, String | Int | etc)
      # This transformation is necessary to satisfy the union type RangeValue.
      # FIXME: potential efficiency savings here
      transformed : RangeQuery = filter.transform_values do |filter_hash|
        filter_hash.transform_values &.as(RangeValue)
      end

      unless invalid_args.empty?
        raise Error::MalformedQuery.new("Invalid range query arguments: #{invalid_args.join(",")}")
      end

      self.range.merge!(transformed)

      self
    end

    # Call to add fields that should be missing
    # Effectively adds a filter that ensures a field is missing
    def missing(*fields)
      @missing ||= Set(String).new
      @missing.concat(fields)
      self
    end

    def exists(*fields)
      @exists ||= Set(String).new
      @exists.concat(fields)
      self
    end

    def build
      {
        query:        build_query,
        filter:       build_filter,
        offset:       offset,
        limit:        limit,
        sort:         sort,
        search_after: search_after,
      }
    end

    # Generates a bool field in the query context
    protected def build_query
      # Query all documents if no search term
      return {must: {match_all: {} of Nil => Nil}} unless @search.size > 1

      base_query = {
        :simple_query_string => {
          query:  @search,
          fields: @fields || [] of String,
        },
      }

      # Define bool field for `has_parent` and `has_child`
      if @parent || @child
        # Merge user defined query settings to the base query
        query_settings = @query_settings
        query = query_settings.nil? ? base_query : base_query.merge(query_settings)
        # Generate should field
        should = [query, parent_query, child_query].compact
        {
          minimum_should_match: minimum_should_match,
          should:               should,
        }
      else
        {
          must: base_query,
        }
      end
    end

    protected def parent_query
      unless @parent.nil?
        {
          has_parent: {
            parent_type: @parent,
            query:       {
              simple_query_string: {
                query:  @search,
                fields: @fields,
              },
            },
          },
        }
      end
    end

    protected def child_query
      unless @child.nil?
        {
          has_child: {
            type:  @child,
            query: {
              simple_query_string: {
                query:  @search,
                fields: @fields,
              },
            },
          },
        }
      end
    end

    # Construct a filter field
    protected def build_filter
      filters = @filters.try { |f| build_field_filter(f) }
      range = @range.try { |r| ({range: r}) }
      missing = @missing.try(&.map { |field| {missing: {field: field}} })
      exists = @exists.try(&.map { |field| {exists: {field: field}} })

      # Combine filters, remove nils and flatten a single level
      filters = [filters, range, missing, exists].compact.flatten
      filters = nil if filters.empty?
      should = @should.try { |f| build_field_filter(f) }
      must = @must.try { |f| build_field_filter(f) }
      must_not = @must_not.try { |f| build_field_filter(f) }

      # Construct bool field, remove nil keys
      bool = {
        :filter   => filters,
        :must     => must,
        :must_not => must_not,
        :should   => should,
      }

      # Add minimum_should_match if should clauses exist and it's set
      if should && (msm = minimum_should_match)
        bool = bool.merge({:minimum_should_match => msm})
      end

      bool = bool.compact

      {filter: {bool: bool}} unless bool.empty?
    end

    # Generate filter field
    protected def build_field_filter(filters : Filter)
      return nil if filters.nil?

      field_filter = filters.flat_map do |key, value|
        build_sub_filters(key, value)
      end

      field_filter.empty? ? nil : field_filter
    end

    alias NotExistsFilter = NamedTuple(bool: NamedTuple(must_not: NamedTuple(exists: Hash(String, String))))
    alias Subfilter = Hash(Symbol, FilterTerm) | NotExistsFilter

    # Generate a sub filter
    protected def build_sub_filters(key, values : FilterValue) : Array(Subfilter)
      return [missing_term_filter(key).as(Subfilter)] if values.nil?

      values.map do |var|
        if var.nil?
          missing_term_filter(key).as(Subfilter)
        else
          term_filter(key, var).as(Subfilter)
        end
      end
    end

    alias FilterTerm = Hash(String, (Int32 | Float32 | Bool | String))

    protected def missing_term_filter(key)
      {bool: {must_not: {exists: {"field" => key}}}}
    end

    protected def term_filter(key, value)
      sub = Hash(Symbol, FilterTerm).new
      ft = FilterTerm.new
      ft[key] = value
      sub[:term] = ft

      sub
    end
  end
end
