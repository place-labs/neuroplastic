require "db"
require "habitat"
require "http"
require "log"

require "./error"

class Neuroplastic::Client
  Log = ::Log.for(self)

  private NUM_INDICES = RethinkORM::Base::TABLES.uniq.size

  # Settings for elastic client
  Habitat.create do
    setting uri : URI? = ENV["ES_URI"]?.try(&->URI.parse(String))
    setting host : String = ENV["ES_HOST"]? || "127.0.0.1"
    setting port : Int32 = ENV["ES_PORT"]?.try(&.to_i) || 9200
    setting tls : Bool = ENV["ES_TLS"]? == "true"
    setting pool_size : Int32 = ENV["ES_CONN_POOL"]?.try(&.to_i) || NUM_INDICES
    setting idle_pool_size : Int32 = ENV["ES_IDLE_POOL"]?.try(&.to_i) || NUM_INDICES // 4
    setting pool_timeout : Float64 = ENV["ES_CONN_POOL_TIMEOUT"]?.try(&.to_f64) || 5.0
  end

  def search(arguments = {} of Symbol => String) : JSON::Any
    valid_params = [
      :_source,
      :_source_exclude,
      :_source_include,
      :allow_no_indices,
      :analyze_wildcard,
      :analyzer,
      :batched_reduce_size,
      :default_operator,
      :df,
      :docvalue_fields,
      :expand_wildcards,
      :explain,
      :fielddata_fields,
      :fields,
      :from,
      :ignore_indices,
      :ignore_unavailable,
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
      :stats,
      :stored_fields,
      :stored_fields,
      :suggest_field,
      :suggest_mode,
      :suggest_size,
      :suggest_text,
      :terminate_after,
      :timeout,
      :typed_keys,
      :version,
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

    Log.debug { "performing search: params=#{params} body=#{body.to_json}" }

    perform_request(method: method, path: path, params: params, body: body)
  end

  def count(arguments = {} of Symbol => String)
    valid_params = [
      :allow_no_indices,
      :analyze_wildcard,
      :analyzer,
      :default_operator,
      :df,
      :expand_wildcards,
      :ignore_unavailable,
      :lenient,
      :lowercase_expanded_terms,
      :min_score,
      :preference,
      :q,
      :routing,
    ]

    index = arguments[:index]? || "_all"
    index = index.join(',') if index.is_a?(Array(String))
    path = "/#{index}/_count"
    method = "POST"
    body = arguments[:body]?
    params = arguments.to_h.select(valid_params)

    perform_request(method: method, path: path, params: params, body: body)
  end

  def perform_request(method, path, params = nil, body = nil)
    post_body = body.try(&.to_json)
    response = case method.upcase
               when "GET"
                 endpoint = "#{path}?#{normalize_params(params)}"
                 if post_body
                   Client.client &.get(path: endpoint, body: post_body, headers: JSON_HEADER)
                 else
                   Client.client &.get(path: endpoint)
                 end
               when "POST"
                 Client.client &.post(path: path, body: post_body, headers: JSON_HEADER)
               when "PUT"
                 Client.client &.put(path: path, body: post_body, headers: JSON_HEADER)
               when "DELETE"
                 endpoint = "#{path}?#{normalize_params(params)}"
                 Client.client &.delete(path: endpoint)
               when "HEAD"
                 Client.client &.head(path: path)
               else
                 raise "Unsupported method: #{method}"
               end

    if response.success?
      JSON.parse(response.body)
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

  private JSON_HEADER = HTTP::Headers{"Content-Type" => "application/json"}

  # Elastic Connection Pooling
  #############################################################################

  protected class_getter pool : DB::Pool(PoolHTTP) {
    DB::Pool(PoolHTTP).new(
      initial_pool_size: settings.pool_size // 4,
      max_pool_size: settings.pool_size,
      max_idle_pool_size: settings.idle_pool_size,
      checkout_timeout: settings.pool_timeout
    ) { elastic_connection }
  }

  # Yield an acquired client from the pool
  #
  protected def self.client
    client = pool.checkout
    result = yield client
    pool.release(client)
    result
  end

  private def self.elastic_connection
    # FIXME: ES_TLS not being pulled from env in habitat settings
    tls_context = if ENV["ES_TLS"]? == "true"
                    context = OpenSSL::SSL::Context::Client.new
                    context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
                    context
                  end

    if (uri = settings.uri).nil?
      PoolHTTP.new(host: settings.host, port: settings.port, tls: tls_context)
    else
      PoolHTTP.new(uri: uri, tls: tls_context)
    end
  end

  private class PoolHTTP < HTTP::Client
    # DB::Pool stubs
    ############################################################################
    def before_checkout
    end

    def after_release
    end
  end
end
