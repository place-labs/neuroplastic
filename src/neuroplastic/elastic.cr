class Neuroplastic::Elastic
  COUNT  = "count"
  HITS   = "hits"
  TOTAL  = "total"
  SCORE  = ["_score"]
  ID     = "_id"
  SOURCE = "_source"
  TYPE   = "type"

  def initialize(@index : String, @type : String)
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

    # Pick off results that do not match the document type
    ids = result[HITS][HITS].compact_map do |hit|
      doc_type = hit[SOURCE][TYPE].as_s
      doc_type == @type ? hit[ID].as_s : nil
    end

    records = {{ @type.id }}.find_all(ids)

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

  # Reopened and defined in the module index
  def find_all(ids)
    raise "undefined"
  end

  def count(builder)
    query = generate_body(builder)

    # Simplify the query
    query[:body].delete(:from)
    query[:body].delete(:size)
    query[:body].delete(:sort)

    self.count(query)[COUNT]
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
