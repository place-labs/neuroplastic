class Neuroplastic::Elastic(T)
  class Query
    DEFAULT_SORT = [{
                      "doc.created_at" => {
                        order: :desc,
                      },
                    }]

    setter :offset, :limit, :sort, :fields, :query_settings

    @query_settings : Hash(String, String)?
    @sort = DEFAULT_SORT

    def initialize(params = {} of Symbol => String)
      @fields = ["_all"]
      @filters = {} of Symbol => Array(String)

      @search = "#{params[:q]?}*"

      @limit = params[:limit]?.try(&.to_i) || 20
      @limit = 500 if @limit > 500

      @offset = params[:offset]?.try(&.to_i) || 0
      @offset = 10000 if @offset > 10000
    end

    def raw_filter(filter)
      @raw_filter ||= [] of String
      @raw_filter << filter
      self
    end

    def search_field(field)
      @fields.unshift(field)
    end

    @child : String | Nil

    # Applys the query to child objects
    def has_child(name)
      @child = name
    end

    @parent : String | Nil
    getter :parent

    # has_parent query
    # - Set the index to the parent, check first that the class is actually a parent of the model
    def has_parent(name)
      @parent = name
    end

    # filters is in the form {fieldname1: ["var1","var2",...], fieldname2: ["var1","var2"...]}
    # NOTE: may overwrite an existing filter in merge
    def filter(filters)
      @filters.merge!(filters)
      self
    end

    # Like filter however all keys are OR's instead of AND's
    def or_filter(filters)
      @or_filter ||= {} of Symbol => Array(String)
      @or_filter.merge!(filters)
      self
    end

    def and_filter(filters)
      @and_filter ||= {} of Symbol => Array(String)
      @and_filter.merge!(filters)
      self
    end

    def range(filter)
      @range_filter ||= [] of Hash(String, String)
      @range_filter << filter
      self
    end

    # Call to add fields that should be missing
    # Effectively adds a filter that ensures a field is missing
    def missing(*fields)
      @missing ||= Set(String).new
      @missing.concat(fields)
      self
    end

    # The opposite of filter
    def not(filters)
      @nots ||= {} of Symbol => Array(String)
      @nots.merge!(filters)
      self
    end

    def exists(*fields)
      @exists ||= Set(String).new
      @exists.concat(fields)
      self
    end

    def build
      filters = build_filters
      if @search.size > 1
        {
          query:   build_query,
          filters: filters,
          offset:  @offset,
          limit:   @limit,
        }
      else
        {
          query:   {match_all: {} of Symbol => String},
          sort:    @sort,
          filters: filters,
          offset:  @offset,
          limit:   @limit,
        }
      end
    end

    protected def build_query
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
          bool: {
            should: should,
          },
        }
      else
        base_query
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

    protected def build_filters
      filters_field = @filters.try { |f| build_field_filter(f, :or) }
      or_filter_field = @or_filter.try { |f| build_field_filter(f, :or) }
      and_filter_field = @and_filter.try { |f| build_field_filter(f, :and) }

      not_filter_field = @nots.try do |nots|
        field_filter = build_field_filter(nots, :or)
        field_filter.try { |f| {not: {filter: f}} }
      end

      range_filter_field = @range_filter.try(&.map { |value| {range: value} })
      missing_field_filter = @missing.try(&.map { |field| {missing: {field: field}} })
      exists_field_filter = @exists.try(&.map { |field| {exists: {field: field}} })

      # Extremely heterogenous array..
      # This method allows easier construction through automatic typing
      [
        filters_field,
        or_filter_field,
        and_filter_field,
        not_filter_field,
        range_filter_field,
        missing_field_filter,
        exists_field_filter,
      ].compact.flatten
    end

    # Generate filter field
    protected def build_field_filter(filters, field : Symbol)
      return nil if filters.nil?

      field_filter = filters.flat_map do |key, value|
        build_sub_filter(key, value)
      end

      field_filter.empty? ? nil : {field => field_filter}
    end

    # Generate a sub filter
    protected def build_sub_filter(key, values)
      values.map do |var|
        if var.nil?
          {missing: {field: key}}
        else
          {term: {key => var}}
        end
      end
    end
  end

  def self.client
    Neuroplastic::Client.client
  end

  def self.search(*args)
    self.client.search *args
  end

  def self.count(*args)
    self.client.count *args
  end

  COUNT = "count"
  HITS  = "hits"
  TOTAL = "total"
  ID    = "_id"
  SCORE = ["_score"]
  INDEX = (ENV["ELASTIC_INDEX"] || "default")

  @index : String = T.table_name
  def initialize(index : String? = nil)
    @index = index unless index.nil?
  end

  # Safely build the query
  def query(params = {} of Symbol => String, filters = nil)
    builder = Query.new(params)
    builder.filter(filters) if filters

    builder
  end

  # Query elasticsearch with a query builder object
  # Accepts a formatter block to transform/annotate loaded results
  def search(builder, &block)
    query = generate_body(builder)

    # if a formatter block is supplied, each loaded record is passed to it
    # allowing annotation/conversion of records using data from the model
    # and current request (e.g groups are annotated with "admin" if the
    # currently logged in user is an admin of the group). nils are removed
    # from the list.
    result = self.client.search(query)

    ids = result[HITS][HITS].map(&.fetch(ID, defaullt: nil)).compact
    records = T.find_all(ids)

    results = block_given? ? (records.map { |record| yield record }).compact : records

    # Ensure the total is accurate
    total = result[HITS][TOTAL]? || 0
    total = total - (records.length - results.length) # adjust for compaction

    # We check records against limit (pre-compaction) and total against actual result length
    # Worst case senario is there is one additional request for results at an offset that returns no results.
    # The total results number will be accurate on the final page of results from the clients perspective.
    total = results.length + builder.offset if records.length < builder.limit && total > (results.length + builder.offset)
    {
      total:   total,
      results: results,
    }
  end

  def count(builder)
    query = generate_body(builder)

    # Simplify the query
    query[:body].delete(:from)
    query[:body].delete(:size)
    query[:body].delete(:sort)

    Elastic.count(query)[COUNT]
  end

  def generate_body(builder)
    opt = builder.build

    # Allow override of index for parent queries
    index = builder.parent || @index

    sort = (opt[:sort]? || [] of Array(String)) + SCORE

    queries = opt[:queries]? || [] of String
    queries.unshift(opt[:query])

    filters = opt[:filters]? || [] of Hash(String, String)

    unless @filter.nil?
      filters.unshift({type: {value: @filter}})
    end

    {
      index: index,
      body:  {
        sort:  sort,
        query: {
          bool: {
            must: {
              query: {
                bool: {
                  must: queries,
                },
              },
            },
            filter: {
              bool: {
                must: filters,
              },
            },
          },
        },
        from: opt[:offset],
        size: opt[:limit],
      },
    }
  end
end
