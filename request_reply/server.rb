require "rubygems"
require "amqp"
require "pqueue"

EventMachine.run do
  connection = AMQP.connect(:host => '127.0.0.1')
  
  channel_low  = AMQP::Channel.new(connection)
  channel_high  = AMQP::Channel.new(connection)

  # Attempting to set the prefetch higher on the high priority queue
  channel_low.prefetch(10)
  channel_high.prefetch(20)

  low_queue    = channel_low.queue("low", :auto_delete => false)
  high_queue    = channel_high.queue("high", :auto_delete => false)

  # Our priority queue for buffering messages in the worker's memory
  to_process = PQueue.new {|a,b| a[0] > b[0] }

  # The pqueue gem isn't thread safe
  mutex = Mutex.new

  # Background thread for working blocking operation. We can spin up more of
  # these to increase concurrency.
  Thread.new do
    loop do
      _, header, payload = mutex.synchronize { to_process.pop }

      if payload
        puts "#{payload}"
        slow_task
        # We need to call ack on the EM thread.
        EM.next_tick { header.ack }
      else
        sleep(0.1)
      end
    end
  end

  low_queue.subscribe(:ack => true) do |header, payload|
    mutex.synchronize { to_process << [0, header, payload] }
  end

  high_queue.subscribe(:ack => true) do |header, payload|
    mutex.synchronize { to_process << [10, header, payload] }
  end

  def slow_task
    # Do some slow work
    
    sleep(1)
  end
end