module Neuroplastic
  class Query
    # Default sort from Engine, currently unused
    DEFAULT_SORT = [{
      "doc.created_at" => {
        order: :desc,
      },
    }]

    alias FilterValue = Array(Int32) | Array(Float32) | Array(Bool) | Array(String) | Nil
    alias Filter = Hash(String, FilterValue)

    property :offset, :limit, :sort, :fields, :query_settings

    @query_settings : Hash(String, String)?
    @sort = [] of Hash(String, NamedTuple(order: Symbol))

    def initialize(params = {} of Symbol => String)
      @fields = ["_all"]
      @filters = Filter.new

      @search = "#{params[:q]?}*"

      @limit = params[:limit]?.try(&.to_i) || 20
      @limit = 500 if @limit > 500

      @offset = params[:offset]?.try(&.to_i) || 0
      @offset = 10000 if @offset > 10000
    end

    def search_field(field)
      @fields.unshift(field)

      self
    end

    @child : String | Nil
    @parent : String | Nil
    @index : String | Nil
    getter :child, :parent, :index

    # Applys the query to child objects
    def has_child(child : Class)
      @child = child.name

      self
    end

    # has_parent query
    # Sets the index to the parent
    # TODO: Check first that the class is actually a parent of the model
    def has_parent(parent : Class, parent_index : String)
      @parent = parent.name
      @index = parent_index

      self
    end

    def filter(filters : Filter)
      @filters = @filters.try &.merge(filters)

      self
    end

    # Like filter, but at least one should match in absence of `filter`/`must` hits
    def should(filters : Filter)
      @should ||= Filter.new
      @should = @should.try &.merge(filters)

      self
    end

    # Like filter, but all hits must match each filter
    def must(filters : Filter)
      @must ||= Filter.new
      @must = @must.try &.merge(filters)

      self
    end

    # The opposite of filter, essentially a not
    def must_not(filters : Filter)
      @must_not ||= Filter.new
      @must_not = @must_not.try &.merge(filters)

      self
    end

    alias RangeQuery = Hash(String, Hash(Symbol, RangeValue))
    alias RangeValue = Int32 | Float32 | Bool | String

    RANGE_PARAMS = {:gte, :gt, :lte, :lt, :boost}

    def range(filter : RangeQuery)
      @range ||= RangeQuery.new

      invalid_args = filter.keys.reject do |k|
        RANGE_PARAMS.includes? k
      end

      unless invalid_args.empty?
        raise Error::MalformedQuery.new("Invalid range query arguments: #{invalid_args.join(",")}")
      end

      @range = @range.try &.merge(filter)

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
        query:  build_query,
        filter: build_filter,
        offset: @offset,
        limit:  @limit,
        sort:   @sort,
      }
    end

    # Generates a bool field in the query context
    protected def build_query
      # Query all documents if no search term
      return {must: {match_all: {} of Symbol => String}} unless @search.size > 1

      base_query = {
        :simple_query_string => {
          query:  @search,
          fields: @fields,
        },
      }

      # Define bool field for `has_parent` and `has_child`
      if @parent || @child
        # Merge user defined query settings to the base query
        query_settings = @query_settings
        query = base_query.merge(query_settings) unless query_settings.nil?
        # Generate should field
        should = [query, parent_query, child_query].compact
        {
          should: should,
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
      range = @range.try(&.map { |value| {range: value} })
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
      }.compact!

      {bool: bool}
    end

    # Generate filter field
    protected def build_field_filter(filters : Filter)
      return nil if filters.nil?

      field_filter = filters.flat_map do |key, value|
        build_sub_filters(key, value)
      end

      field_filter.empty? ? nil : field_filter
    end

    alias Subfilter = Hash(Symbol, FilterTerm)

    # Generate a sub filter
    protected def build_sub_filters(key, values : FilterValue) : Array(Subfilter)
      return [missing_term_filter(key)] if values.nil?

      values.map do |var|
        if var.nil?
          missing_term_filter(key)
        else
          term_filter(key, var)
        end
      end
    end

    alias FilterTerm = Hash(String, (Int32 | Float32 | Bool | String))

    protected def missing_term_filter(key)
      sub = Subfilter.new
      ft = FilterTerm.new
      ft["field"] = key
      sub[:missing] = ft

      sub
    end

    protected def term_filter(key, value)
      sub = Subfilter.new
      ft = FilterTerm.new
      ft[key] = value
      sub[:term] = ft

      sub
    end
  end
end