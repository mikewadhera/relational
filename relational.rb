require 'fiber'
require 'mysql2'
require 'mysql2/em_fiber'
require 'em-synchrony'
require 'arel'

module Relational
  
  def self.Entity(table)
    superclass = Class.new do
      include EntityInitialization
      extend AttributeDefinition
      extend SqlAlgebra
      extend SqlGeneration
      extend SqlExecution
      
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
    
    def self.establish!(size, mysql_options)
      @connection_pool = new(size, mysql_options)
    end
    
    def self.with_connection(&block)
      raise NotEstablished unless @connection_pool
      @connection_pool.execute(true, &block)
    end
    
    def initialize(size, mysql_options)
      new_mysql_connection_generator = lambda { Mysql2::EM::Fiber::Client.new(mysql_options) }
      super(size: size, &new_mysql_connection_generator)
    end
        
  end
    
  module EntityInitialization
        
    def initialize(user_attributes={}, db_attributes=nil)
      @data = if db_attributes then Hash.new { |h,k| db_attributes[k] } else user_attributes end
    end
        
  end

  module AttributeDefinition
    
    def attribute(name)
      (@attributes ||= []) << name
      define_attribute_accessor(name)
    end 
    
    protected
    
    def define_attribute_accessor(name)
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
  
  module SqlAlgebra
    
    %w( select where join on limit offset ).each do |operator|
      class_eval(<<-EOS, __FILE__, __LINE__)
        def #{operator}(*args)
          clone_then_update_arel_args(:#{operator}, args)
        end
      EOS
    end
        
    protected
    
    def arel_args
      @arel_args ||= {}
    end    
        
    def clone_then_update_arel_args(operator, args)
      cloned = self.clone
      arel_args = cloned.send(:arel_args)
      arel_args[operator] ||= []
      arel_args[operator] += args
      cloned
    end
    
  end
  
  module SqlGeneration
    
    def [](attribute)
      self.arel_relation[attribute]
    end
    
    protected
    
    def generate_sql
      self.arel_select_manager.to_sql
    end
    
    def arel_relation
      @arel_relation ||= Arel::Table.new(self.table)
    end
    
    def arel_select_manager
      @arel_select_manager ||= self.build_arel_select_manager(self.arel_args)
    end
    
    def build_arel_select_manager(args={})
      # select
      select_args = args[:select] || []
      project_args = select_args.empty? ? Arel::SqlLiteral.new('*') : select_args.map { |a| arel_attribute_for(a) }
      select_manager = arel_relation.project(project_args)
      
      # where
      where_args = args[:where]
      select_manager.where(where_args) if where_args
      
      # join
      join_args = args[:join]
      select_manager.join(join_args) if join_args
      
      # on
      on_args = args[:on]
      select_manager.on(on_args) if on_args
      
      # limit
      take_args = args[:limit]
      select_manager.take(take_args) if take_args
      
      # offset
      skip_args = args[:offset]
      select_manager.skip(skip_args)
      
      select_manager
    end
    
    def arel_attribute_for(thing)
      Arel::Attribute === thing ? thing : self.arel_relation[thing]
    end
    
  end
  
  module SqlExecution

    def each(&block)
      query(self.generate_sql).each(&block)
    end    

    def query(sql)
      ConnectionPool.with_connection do |mysql|
        QueryResult.new(self, mysql.query(sql, symbolize_keys: true))
      end
    end
    
  end
  
  class QueryResult < Struct.new(:entity, :result)
        
    def each
      self.result.each do |row|
        yield self.entity.new(nil, row)
      end
      self
    end
    
  end
  
end