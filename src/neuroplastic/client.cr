require "db"
require "habitat"
require "http"

require "./error"

class Neuroplastic::Client
  private NUM_INDICES = RethinkORM::Base::TABLES.uniq.size

  # Settings for elastic client
  Habitat.create do
    setting host : String = ENV["ES_HOST"]? || "127.0.0.1"
    setting port : Int32 = ENV["ES_PORT"]?.try(&.to_i) || 9200
    setting pool_size : Int32 = ENV["ES_CONN_POOL"]?.try(&.to_i) || NUM_INDICES
    setting idle_pool_size : Int32 = ENV["ES_IDLE_POOL"]?.try(&.to_i) || NUM_INDICES // 4
    setting pool_timeout : Float64 = ENV["ES_CONN_POOL_TIMEOUT"]?.try(&.to_f64) || 5.0
  end

  def search(arguments = {} of Symbol => String) : JSON::Any
    valid_params = [
      :analyzer,
      :analyze_wildcard,
      :default_operator,
      :df,
      :explain,
      :fielddata_fields,
      :docvalue_fields,
      :stored_fields,
      :fields,
      :from,
      :ignore_indices,
      :ignore_unavailable,
      :allow_no_indices,
      :expand_wildcards,
      :lenient,
      :lowercase_expanded_terms,
      :preference,
      :q,
      :query_cache,
      :request_cache,
      :routing,
      :scroll,
      :search_type,
      :size,
      :sort,
      :source,
      :_source,
      :_source_include,
      :_source_exclude,
      :stored_fields,
      :stats,
      :suggest_field,
      :suggest_mode,
      :suggest_size,
      :suggest_text,
      :terminate_after,
      :timeout,
      :typed_keys,
      :version,
      :batched_reduce_size,
    ]

    index = arguments[:index]? || "_all"
    path = "/#{index}/_search"
    method = "POST"
    body = arguments[:body]?
    params = arguments.to_h.select(valid_params)

    fields = arguments[:fields]?
    if fields
      fields = [fields] unless fields.is_a?(Array)
      params[:fields] = fields.map(&.to_s).join(',')
    end

    fielddata_fields = arguments[:fielddata_fields]?
    if fielddata_fields
      fielddata_fields = [fielddata_fields] unless fielddata_fields.is_a?(Array)
      params[:fielddata_fields] = fielddata_fields.map(&.to_s).join(',')
    end

    perform_request(method: method, path: path, params: params, body: body)
  end

  def count(arguments = {} of Symbol => String)
    valid_params = [
      :ignore_unavailable,
      :allow_no_indices,
      :expand_wildcards,
      :min_score,
      :preference,
      :routing,
      :q,
      :analyzer,
      :analyze_wildcard,
      :default_operator,
      :df,
      :lenient,
      :lowercase_expanded_terms,
    ]

    index = arguments[:index]? || "_all"
    index = index.join(',') if index.is_a?(Array(String))
    path = "/#{index}/_count"
    method = "GET"
    body = arguments[:body]?
    params = arguments.to_h.select(valid_params)

    perform_request(method: method, path: path, params: params, body: body)
  end

  def perform_request(method, path, params = nil, body = nil)
    post_body = body.try(&.to_json)
    response = case method
               when "GET"
                 endpoint = "#{path}?#{normalize_params(params)}"
                 if post_body
                   Client.client &.get(path: endpoint, body: post_body, headers: json_header)
                 else
                   Client.client &.get(path: endpoint)
                 end
               when "POST"
                 Client.client &.post(path: path, body: post_body, headers: json_header)
               when "PUT"
                 Client.client &.put(path: path, body: post_body, headers: json_header)
               when "DELETE"
                 endpoint = "#{path}?#{normalize_params(params)}"
                 Client.client &.delete(path: endpoint)
               when "HEAD"
                 Client.client &.head(path: path)
               else
                 raise "Niche header..."
               end

    if response.success?
      r = JSON.parse(response.body)
      r
    else
      raise Error::ElasticQueryError.new("ES error: #{response.status_code}\n#{response.body}")
    end
  end

  # Normalize params to string and encode
  private def normalize_params(params) : String
    if params
      new_params = params.reduce({} of String => String) do |hash, kv|
        k, v = kv
        hash[k.to_s] = v.to_s
        hash
      end
      HTTP::Params.encode(new_params)
    else
      ""
    end
  end

  private def json_header
    HTTP::Headers{"Content-Type" => "application/json"}
  end

  # Elastic Connection Pooling
  #############################################################################

  @@pool : DB::Pool(PoolHTTP)?

  # Yield an acquired client from the pool
  #
  protected def self.client
    pool = (@@pool ||= DB::Pool(PoolHTTP).new(
      initial_pool_size: settings.pool_size // 4,
      max_pool_size: settings.pool_size,
      max_idle_pool_size: settings.idle_pool_size,
      checkout_timeout: settings.pool_timeout
    ) { elastic_connection }).as(DB::Pool(PoolHTTP))

    client = pool.checkout
    result = yield client
    pool.release(client)

    result
  end

  private def self.elastic_connection
    PoolHTTP.new(host: settings.host, port: settings.port)
  end

  private class PoolHTTP < HTTP::Client
    # DB::Pool stubs
    ############################################################################
    # :nodoc:
    def before_checkout
    end

    # :nodoc:
    def after_release
    end
  end
end
