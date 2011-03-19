require 'fiber'
require 'mysql2'
require 'mysql2/em_fiber'
require 'em-synchrony'
require 'arel'

module Relational
    
  def self.Entity(table)
    superclass = Class.new
    superclass.class_eval do
      include InitializationMethods
      extend AttributeMethods
      extend QueryMethods
      
      @@table = table
      
      class << self
        def table
          @@table
        end
      end
    end
    superclass
  end
    
  class ConnectionPool < EventMachine::Synchrony::ConnectionPool
    class NotEstablished < Exception; end
    
    class << self
      def establish!(size, mysql_options)
        @connection_pool = new(size, mysql_options)
      end
      
      def with_connection
        raise NotEstablished unless @connection_pool
        @connection_pool.execute(true) do |mysql|
          yield mysql
        end
      end
    end
    
    def initialize(size, mysql_options)
      super(size: size) do
        Mysql2::EM::Fiber::Client.new(mysql_options)
      end
    end
        
  end
    
  module InitializationMethods
        
    def initialize(user_data={}, db_data=nil)
      @data = if db_data
        Hash.new { |h,k| db_data[k] }
      else
        user_data
      end
    end
        
  end

  module AttributeMethods
    
    def attribute(name)
      (@attributes ||= []) << name
      attribute_accessor(name)
    end
    
    protected
    
    def attribute_accessor(name)
      class_eval(<<-EOS, __FILE__, __LINE__)
        def #{name}
          @data[:#{name}]
        end
        
        def #{name}=(value)
          @data[:#{name}] = value
        end
      EOS
    end
    
  end
  
  module QueryMethods
    
    Star = Arel::SqlLiteral.new('*')
    
    def relation
      @relation ||= Arel::Table.new(self.table)
    end
    
    def select(*attributes)
      projection = attributes.empty? ? QueryMethods::Star : attributes.map { |a| Symbol === a ? self.relation[a] : a }
      sql = self.relation.project(projection).to_sql
      query(sql)
    end
    
    def query(sql)
      ConnectionPool.with_connection do |mysql|
        QueryResult.new(self, mysql.query(sql, symbolize_keys: true))
      end
    end
        
  end
  
  class QueryResult
    
    def initialize(entity, result)
      @entity = entity
      @result = result
    end
    
    def each
      @result.each do |row|
        yield @entity.new(nil, row)
      end
      self
    end
    
  end
    
end
