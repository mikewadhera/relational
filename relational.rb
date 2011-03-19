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
    
    def self.extended(base)
      base.class_eval do
        class << self
          attr_accessor :select_values
        end
      end
    end
        
    def select(*attributes)
      cloned = self.clone
      cloned.select_values ||= []
      cloned.select_values += attributes
      cloned
    end
    
    def query(sql)
      ConnectionPool.with_connection do |mysql|
        QueryResult.new(self, mysql.query(sql, symbolize_keys: true))
      end
    end
    
    def each
      query(self.select_manager.to_sql).each do |object|
        yield object
      end
    end
        
    protected
    
    def select_manager
      @select_manager ||= self.build_select_manager
    end    
    
    def build_select_manager
      relation = Arel::Table.new(self.table)
      
      # projection
      projection = @select_values.empty? ? QueryMethods::Star : @select_values.map { |a| Symbol === a ? relation[a] : a }
      manager = relation.project(projection)
      
      manager
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
