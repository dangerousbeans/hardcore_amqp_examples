require "amqp"
require 'pqueue'

EventMachine.run do
  connection = AMQP.connect
  channel    = AMQP::Channel.new(connection)

  to_process = PQueue.new {|a,b| a[0] > b[0] }
  mutex = Mutex.new

  requests_queue = channel.queue("test.messages", :exclusive => true, :auto_delete => true)
  

  requests_queue.subscribe(:ack => true) do |metadata, payload|

    puts "Got a request."

    puts "Queue job"
    
    mutex.synchronize { to_process << [0, metadata, payload] }

  end


  Thread.new do
    loop do
      _, metadata, payload = mutex.synchronize { to_process.pop }

      if payload
        puts "#{payload}"
        
        process channel, metadata
      else
        sleep(0.1)
      end
    end
  end

  Signal.trap("INT") { connection.close { EventMachine.stop } }


  def process channel, metadata
    channel.default_exchange.publish("HAI THUR :D",
                                     :routing_key    => metadata.reply_to,
                                     :correlation_id => metadata.message_id,
                                     :mandatory      => true)

    EM.next_tick { metadata.ack }
  end
end