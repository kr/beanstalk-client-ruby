# beanstalk-client/job.rb - client library for beanstalk

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'yaml'

class Beanstalk::Job
  attr_reader :id, :body, :conn

  # Convenience method for getting ybody elements.
  def [](name)
    ybody[name]
  end

  # Convenience method for setting ybody elements.
  def []=(name, val)
    ybody[name] = val
  end

  # Return the object that results from loading the body as a yaml stream.
  # Return nil if the body is not a valid yaml stream.
  def ybody()
    (@ybody ||= [begin YAML.load(body) rescue nil end])[0]
  end

  def initialize(conn, id, body, reserved=true)
    @conn = conn
    @id = id
    @body = body
    @reserved = reserved
  end

  # Deletes the job from the queue
  def delete
    @conn.delete(id)
    @reserved = false
  end


  def put_back(pri=self.pri, delay=0, ttr=self.ttr)
    @conn.put(body, pri, delay, ttr)
  end

  # Releases the job back to the queue so another consumer can get it (call this if the job failed and want it to be tried again)
  def release(newpri=nil, delay=0)
    return if !@reserved
    @conn.release(id, newpri || pri, delay)
    @reserved = false
  end

  def bury(newpri=nil)
    return if !@reserved
    @conn.bury(id, newpri || pri)
    @reserved = false
  end

  # Ping beanstalkd to to tell it you're alive and processing. If beanstalkd doesn't hear from you for more than the ttr seconds (specified by the put command), then it assumes the consumer died and reinserts the job back into the queue for another to process.
  def touch
    return if !@reserved
    @conn.touch(id)
  end

  def stats()
    @conn.job_stats(id)
  end

  def timeouts() stats['timeouts'] end

  # Time left (in seconds) that beanstalk has to process the job. When this time expires, beanstalkd automatically reinserts the job in the queue.  See the ttr parameter for Beanstalk::Pool#put
  def time_left() stats['time-left'] end
  def age() stats['age'] end
  def state() stats['state'] end
  def delay() stats['delay'] end
  def pri() stats['pri'] end
  def ttr() stats['ttr'] end

  def server()
    @conn.addr
  end

  # Don't delay for more than 48 hours at a time.
  DELAY_MAX = 60 * 60 * 48 unless defined?(DELAY_MAX)

  def decay(d=([1, delay].max * 1.3).ceil)
    return bury() if delay >= DELAY_MAX
    release(pri, d)
  end

  def to_s
    "(job #{body.inspect})"
  end

  def inspect
    "(job server=#{server} id=#{id} size=#{body.size})"
  end
end
