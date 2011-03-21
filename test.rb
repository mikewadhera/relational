
$LOAD_PATH.unshift "."

require 'relational'

class User < Relational::Entity(:users)
  attribute :id
  attribute :type
  attribute :login
  attribute :email
  attribute :name
  attribute :crypted_password
  attribute :display_name
  attribute :zip
  attribute :hometown
  attribute :created_at
  attribute :updated_at
  
end

EM.run do
  
  concurrency = 50
  
  Relational::ConnectionPool.establish!(concurrency, :host => "localhost",
                                                     :username => "root",
                                                     :database => "involver_dev")

  1.upto concurrency do |i|
    Fiber.new {
      # no callbacks!
      result = User.select(:id, :name).where(User[:id].eq(1))
      
      result.each do |user|
        puts user.id
      end
    
      EM.stop_event_loop if i == concurrency
    }.resume
  end
    
  
end
