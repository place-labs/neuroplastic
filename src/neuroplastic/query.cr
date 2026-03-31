require "./utils"
require "base64"
require "json"

module Neuroplastic
  class Query
    alias Sort = Hash(String, NamedTuple(order: Symbol)) | String | Hash(String, String)
    alias FilterValue = Array(Int32) | Array(Int32?) | Array(Float32) | Array(Float32?) | Array(Bool) | Array(Bool?) | Array(String) | Array(String?) | Nil
    alias Filter = Hash(String, FilterValue)
    alias AggScalar = String | Int32 | Int64 | Float64 | Bool | Nil
    alias AggValue = AggScalar | Array(AggValue) | Hash(String, AggValue)
    alias AggHash = Hash(String, AggValue)
    alias Aggs = Hash(String, AggHash)
    alias AggRanges = Array(Hash(String, AggScalar))
    alias NamedFilters = Hash(String, Filter)

    getter search : String

    getter offset : Int32

    getter limit : Int32

    alias SearchAfter = Array(String | Int64 | Float64)
    getter search_after : SearchAfter?

    getter query_settings : Hash(String, String)?

    getter sort : Array(Sort) = [] of Sort

    getter fields : Array(String) = [] of String

    getter aggs : Aggs do
      Aggs.new
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

    def filter(name : String, filters : Filter)
      filter(name, filters) { }
    end

    def filter(name : String, filters : Filter, &)
      register_agg(name, agg_body("filter", serialize_filter(filters), validate_field: false)) do |nested|
        yield nested
      end
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
      transformed : RangeQuery = filter.transform_values do |filter_hash|
        filter_hash.transform_values &.as(RangeValue)
      end

      unless invalid_args.empty?
        raise Error::MalformedQuery.new("Invalid range query arguments: #{invalid_args.join(",")}")
      end

      self.range.merge!(transformed)

      self
    end

    def range(name : String, field : String, ranges : AggRanges, *, keyed : Bool? = nil)
      range(name, field, ranges, keyed: keyed) { }
    end

    def range(name : String, field : String, ranges : AggRanges, *, keyed : Bool? = nil, &)
      raise Error::MalformedQuery.new("Range aggregation requires at least one range") if ranges.empty?

      register_agg(name, agg_body("range", {
        "field"  => field.as(AggValue),
        "ranges" => serialize_ranges(ranges),
        "keyed"  => keyed.as(AggValue),
      })) do |nested|
        yield nested
      end
    end

    def terms(name : String, field : String, *, size : Int? = nil, order : Hash(String, String)? = nil, min_doc_count : Int? = nil)
      terms(name, field, size: size, order: order, min_doc_count: min_doc_count) { }
    end

    def terms(name : String, field : String, *, size : Int? = nil, order : Hash(String, String)? = nil, min_doc_count : Int? = nil, &)
      register_agg(name, agg_body("terms", {
        "field"         => field.as(AggValue),
        "size"          => size.try(&.to_i32).as(AggValue),
        "order"         => serialize_hash(order),
        "min_doc_count" => min_doc_count.try(&.to_i32).as(AggValue),
      })) do |nested|
        yield nested
      end
    end

    def filters(name : String, filters : NamedFilters, *, other_bucket : Bool? = nil, other_bucket_key : String? = nil)
      filters(name, filters, other_bucket: other_bucket, other_bucket_key: other_bucket_key) { }
    end

    def filters(name : String, filters : NamedFilters, *, other_bucket : Bool? = nil, other_bucket_key : String? = nil, &)
      raise Error::MalformedQuery.new("Filters aggregation requires at least one named filter") if filters.empty?

      register_agg(name, agg_body("filters", {
        "filters"          => serialize_named_filters(filters),
        "other_bucket"     => other_bucket.as(AggValue),
        "other_bucket_key" => other_bucket_key.as(AggValue),
      }, validate_field: false)) do |nested|
        yield nested
      end
    end

    def date_histogram(name : String, field : String, *, calendar_interval : String? = nil, fixed_interval : String? = nil, format : String? = nil, min_doc_count : Int? = nil)
      date_histogram(name, field, calendar_interval: calendar_interval, fixed_interval: fixed_interval, format: format, min_doc_count: min_doc_count) { }
    end

    def date_histogram(name : String, field : String, *, calendar_interval : String? = nil, fixed_interval : String? = nil, format : String? = nil, min_doc_count : Int? = nil, &)
      if calendar_interval.nil? && fixed_interval.nil?
        raise Error::MalformedQuery.new("Date histogram aggregation requires calendar_interval or fixed_interval")
      end

      register_agg(name, agg_body("date_histogram", {
        "field"             => field.as(AggValue),
        "calendar_interval" => calendar_interval.as(AggValue),
        "fixed_interval"    => fixed_interval.as(AggValue),
        "format"            => format.as(AggValue),
        "min_doc_count"     => min_doc_count.try(&.to_i32).as(AggValue),
      })) do |nested|
        yield nested
      end
    end

    def nested(name : String, path : String)
      nested(name, path) { }
    end

    def nested(name : String, path : String, &)
      raise Error::MalformedQuery.new("Nested aggregation path cannot be empty") if path.empty?

      register_agg(name, agg_body("nested", {
        "path" => path.as(AggValue),
      }, validate_field: false)) do |nested|
        yield nested
      end
    end

    def reverse_nested(name : String, path : String? = nil)
      reverse_nested(name, path) { }
    end

    def reverse_nested(name : String, path : String? = nil, &)
      register_agg(name, agg_body("reverse_nested", {
        "path" => path.as(AggValue),
      }, validate_field: false)) do |nested|
        yield nested
      end
    end

    # Call to add fields that should be missing
    # Effectively adds a filter that ensures a field is missing
    def missing(*fields)
      @missing ||= Set(String).new
      @missing.concat(fields)
      self
    end

    def missing(name : String, field : String)
      register_metric_agg(name, "missing", field)
    end

    def exists(*fields)
      @exists ||= Set(String).new
      @exists.concat(fields)
      self
    end

    def avg(name : String, field : String)
      register_metric_agg(name, "avg", field)
    end

    def sum(name : String, field : String)
      register_metric_agg(name, "sum", field)
    end

    def min(name : String, field : String)
      register_metric_agg(name, "min", field)
    end

    def max(name : String, field : String)
      register_metric_agg(name, "max", field)
    end

    def stats(name : String, field : String)
      register_metric_agg(name, "stats", field)
    end

    def cardinality(name : String, field : String)
      register_metric_agg(name, "cardinality", field)
    end

    def build
      {
        query:        build_query,
        filter:       build_filter,
        offset:       offset,
        limit:        limit,
        sort:         sort,
        search_after: search_after,
        aggs:         aggs.empty? ? nil : aggs,
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
        query_settings = @query_settings
        query = query_settings.nil? ? base_query : base_query.merge(query_settings)
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
      missing = @missing.try(&.map { |field| {bool: {must_not: {exists: {field: field}}}} })
      exists = @exists.try(&.map { |field| {exists: {field: field}} })

      filters = [filters, range, missing, exists].compact.flatten
      filters = nil if filters.empty?
      should = @should.try { |f| build_field_filter(f) }
      must = @must.try { |f| build_field_filter(f) }
      must_not = @must_not.try { |f| build_field_filter(f) }

      bool = {
        :filter   => filters,
        :must     => must,
        :must_not => must_not,
        :should   => should,
      }

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

    private def register_metric_agg(name : String, type : String, field : String)
      register_agg(name, agg_body(type, {"field" => field.as(AggValue)}))
    end

    private def register_agg(name : String, body : AggHash)
      register_agg(name, body) { }
    end

    private def register_agg(name : String, body : AggHash, &)
      validate_agg_name(name)

      nested = AggregationBuilder.new
      yield nested
      body["aggs"] = serialize_aggs(nested.aggs) unless nested.aggs.empty?

      aggs[name] = body
      self
    end

    private def validate_agg_name(name : String)
      raise Error::MalformedQuery.new("Aggregation name cannot be empty") if name.empty?
    end

    private def validate_agg_field(field : String)
      raise Error::MalformedQuery.new("Aggregation field cannot be empty") if field.empty?
    end

    private def agg_body(type : String, values : AggHash, *, validate_field : Bool = true) : AggHash
      raise Error::MalformedQuery.new("Aggregation type cannot be empty") if type.empty?
      if validate_field && (field = values["field"]?)
        validate_agg_field(field.as(String))
      end

      body = AggHash.new
      body[type] = without_nil_values(values).as(AggValue)
      body
    end

    private def without_nil_values(values : AggHash) : AggHash
      compacted = AggHash.new
      values.each do |key, value|
        compacted[key] = value unless value.nil?
      end
      compacted
    end

    private def serialize_hash(hash : Hash(String, String)?)
      hash.try(&.transform_values(&.as(AggValue))).as(AggValue)
    end

    private def serialize_filter(filters : Filter) : AggHash
      clauses = build_field_filter(filters)
      raise Error::MalformedQuery.new("Aggregation filter requires at least one clause") if clauses.nil? || clauses.empty?

      from_json_any(JSON.parse({bool: {filter: clauses}}.to_json)).as(AggHash)
    end

    private def serialize_named_filters(filters : NamedFilters)
      filters.transform_values do |clauses|
        serialize_filter(clauses).transform_values(&.as(AggValue)).as(AggValue)
      end.as(AggValue)
    end

    private def serialize_ranges(ranges : AggRanges)
      ranges.map do |range_def|
        raise Error::MalformedQuery.new("Range aggregation definitions cannot be empty") if range_def.empty?
        range_def.transform_values(&.as(AggValue)).as(AggValue)
      end.as(AggValue)
    end

    private def serialize_aggs(aggregations : Aggs)
      aggregations.transform_values(&.as(AggValue)).as(AggValue)
    end

    private def from_json_any(value : JSON::Any) : AggValue
      if hash = value.as_h?
        hash.transform_values { |child| from_json_any(child) }.as(AggValue)
      elsif array = value.as_a?
        array.map { |child| from_json_any(child) }.as(AggValue)
      elsif bool = value.as_bool?
        bool.as(AggValue)
      elsif int = value.as_i?
        int.to_i64.as(AggValue)
      elsif float = value.as_f?
        float.as(AggValue)
      elsif string = value.as_s?
        string.as(AggValue)
      else
        nil.as(AggValue)
      end
    end

    class AggregationBuilder
      getter aggs : Aggs do
        Aggs.new
      end

      def terms(name : String, field : String, *, size : Int? = nil, order : Hash(String, String)? = nil, min_doc_count : Int? = nil)
        terms(name, field, size: size, order: order, min_doc_count: min_doc_count) { }
      end

      def terms(name : String, field : String, *, size : Int? = nil, order : Hash(String, String)? = nil, min_doc_count : Int? = nil, &)
        register_agg(name, agg_body("terms", {
          "field"         => field.as(AggValue),
          "size"          => size.try(&.to_i32).as(AggValue),
          "order"         => serialize_hash(order),
          "min_doc_count" => min_doc_count.try(&.to_i32).as(AggValue),
        })) do |nested|
          yield nested
        end
      end

      def filter(name : String, filters : Filter)
        filter(name, filters) { }
      end

      def filter(name : String, filters : Filter, &)
        register_agg(name, agg_body("filter", serialize_filter(filters), validate_field: false)) do |nested|
          yield nested
        end
      end

      def filters(name : String, filters : NamedFilters, *, other_bucket : Bool? = nil, other_bucket_key : String? = nil)
        filters(name, filters, other_bucket: other_bucket, other_bucket_key: other_bucket_key) { }
      end

      def filters(name : String, filters : NamedFilters, *, other_bucket : Bool? = nil, other_bucket_key : String? = nil, &)
        raise Error::MalformedQuery.new("Filters aggregation requires at least one named filter") if filters.empty?

        register_agg(name, agg_body("filters", {
          "filters"          => serialize_named_filters(filters),
          "other_bucket"     => other_bucket.as(AggValue),
          "other_bucket_key" => other_bucket_key.as(AggValue),
        }, validate_field: false)) do |nested|
          yield nested
        end
      end

      def range(name : String, field : String, ranges : AggRanges, *, keyed : Bool? = nil)
        range(name, field, ranges, keyed: keyed) { }
      end

      def range(name : String, field : String, ranges : AggRanges, *, keyed : Bool? = nil, &)
        raise Error::MalformedQuery.new("Range aggregation requires at least one range") if ranges.empty?

        register_agg(name, agg_body("range", {
          "field"  => field.as(AggValue),
          "ranges" => serialize_ranges(ranges),
          "keyed"  => keyed.as(AggValue),
        })) do |nested|
          yield nested
        end
      end

      def date_histogram(name : String, field : String, *, calendar_interval : String? = nil, fixed_interval : String? = nil, format : String? = nil, min_doc_count : Int? = nil)
        date_histogram(name, field, calendar_interval: calendar_interval, fixed_interval: fixed_interval, format: format, min_doc_count: min_doc_count) { }
      end

      def date_histogram(name : String, field : String, *, calendar_interval : String? = nil, fixed_interval : String? = nil, format : String? = nil, min_doc_count : Int? = nil, &)
        if calendar_interval.nil? && fixed_interval.nil?
          raise Error::MalformedQuery.new("Date histogram aggregation requires calendar_interval or fixed_interval")
        end

        register_agg(name, agg_body("date_histogram", {
          "field"             => field.as(AggValue),
          "calendar_interval" => calendar_interval.as(AggValue),
          "fixed_interval"    => fixed_interval.as(AggValue),
          "format"            => format.as(AggValue),
          "min_doc_count"     => min_doc_count.try(&.to_i32).as(AggValue),
        })) do |nested|
          yield nested
        end
      end

      def nested(name : String, path : String)
        nested(name, path) { }
      end

      def nested(name : String, path : String, &)
        raise Error::MalformedQuery.new("Nested aggregation path cannot be empty") if path.empty?

        register_agg(name, agg_body("nested", {
          "path" => path.as(AggValue),
        }, validate_field: false)) do |nested|
          yield nested
        end
      end

      def reverse_nested(name : String, path : String? = nil)
        reverse_nested(name, path) { }
      end

      def reverse_nested(name : String, path : String? = nil, &)
        register_agg(name, agg_body("reverse_nested", {
          "path" => path.as(AggValue),
        }, validate_field: false)) do |nested|
          yield nested
        end
      end

      def missing(name : String, field : String)
        register_metric_agg(name, "missing", field)
      end

      def avg(name : String, field : String)
        register_metric_agg(name, "avg", field)
      end

      def sum(name : String, field : String)
        register_metric_agg(name, "sum", field)
      end

      def min(name : String, field : String)
        register_metric_agg(name, "min", field)
      end

      def max(name : String, field : String)
        register_metric_agg(name, "max", field)
      end

      def stats(name : String, field : String)
        register_metric_agg(name, "stats", field)
      end

      def cardinality(name : String, field : String)
        register_metric_agg(name, "cardinality", field)
      end

      private def register_metric_agg(name : String, type : String, field : String)
        register_agg(name, agg_body(type, {"field" => field.as(AggValue)}))
      end

      private def register_agg(name : String, body : AggHash)
        register_agg(name, body) { }
      end

      private def register_agg(name : String, body : AggHash, &)
        validate_agg_name(name)

        nested = self.class.new
        yield nested
        body["aggs"] = serialize_aggs(nested.aggs) unless nested.aggs.empty?

        aggs[name] = body
        self
      end

      private def validate_agg_name(name : String)
        raise Error::MalformedQuery.new("Aggregation name cannot be empty") if name.empty?
      end

      private def validate_agg_field(field : String)
        raise Error::MalformedQuery.new("Aggregation field cannot be empty") if field.empty?
      end

      private def agg_body(type : String, values : AggHash, *, validate_field : Bool = true) : AggHash
        raise Error::MalformedQuery.new("Aggregation type cannot be empty") if type.empty?
        if validate_field && (field = values["field"]?)
          validate_agg_field(field.as(String))
        end

        body = AggHash.new
        body[type] = without_nil_values(values).as(AggValue)
        body
      end

      private def without_nil_values(values : AggHash) : AggHash
        compacted = AggHash.new
        values.each do |key, value|
          compacted[key] = value unless value.nil?
        end
        compacted
      end

      private def serialize_hash(hash : Hash(String, String)?)
        hash.try(&.transform_values(&.as(AggValue))).as(AggValue)
      end

      private def serialize_filter(filters : Filter) : AggHash
        clauses = build_field_filter(filters)
        raise Error::MalformedQuery.new("Aggregation filter requires at least one clause") if clauses.nil? || clauses.empty?

        from_json_any(JSON.parse({bool: {filter: clauses}}.to_json)).as(AggHash)
      end

      private def serialize_named_filters(filters : NamedFilters)
        filters.transform_values do |clauses|
          serialize_filter(clauses).transform_values(&.as(AggValue)).as(AggValue)
        end.as(AggValue)
      end

      private def serialize_ranges(ranges : AggRanges)
        ranges.map do |range_def|
          raise Error::MalformedQuery.new("Range aggregation definitions cannot be empty") if range_def.empty?
          range_def.transform_values(&.as(AggValue)).as(AggValue)
        end.as(AggValue)
      end

      private def serialize_aggs(aggregations : Aggs)
        aggregations.transform_values(&.as(AggValue)).as(AggValue)
      end

      private def from_json_any(value : JSON::Any) : AggValue
        if hash = value.as_h?
          hash.transform_values { |child| from_json_any(child) }.as(AggValue)
        elsif array = value.as_a?
          array.map { |child| from_json_any(child) }.as(AggValue)
        elsif bool = value.as_bool?
          bool.as(AggValue)
        elsif int = value.as_i?
          int.to_i64.as(AggValue)
        elsif float = value.as_f?
          float.as(AggValue)
        elsif string = value.as_s?
          string.as(AggValue)
        else
          nil.as(AggValue)
        end
      end

      private def build_field_filter(filters : Filter)
        field_filter = filters.flat_map do |key, value|
          build_sub_filters(key, value)
        end

        field_filter.empty? ? nil : field_filter
      end

      private def build_sub_filters(key, values : FilterValue) : Array(Subfilter)
        return [missing_term_filter(key).as(Subfilter)] if values.nil?

        values.map do |var|
          if var.nil?
            missing_term_filter(key).as(Subfilter)
          else
            term_filter(key, var).as(Subfilter)
          end
        end
      end

      private def missing_term_filter(key)
        {bool: {must_not: {exists: {"field" => key}}}}
      end

      private def term_filter(key, value)
        sub = Hash(Symbol, FilterTerm).new
        ft = FilterTerm.new
        ft[key] = value
        sub[:term] = ft

        sub
      end
    end
  end
end
