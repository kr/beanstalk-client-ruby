# beanstalk-client/connection.rb - client library for beanstalk

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

require 'socket'
require 'fcntl'
require 'yaml'
require 'set'
require 'thread'

module Beanstalk
  class Connection
    attr_reader :addr

    def initialize(addr, default_tube=nil)
      @mutex = Mutex.new
      @tube_mutex = Mutex.new
      @waiting = false
      @addr = addr
      connect
      @last_used = 'default'
      @watch_list = [@last_used]
      self.use(default_tube) if default_tube
      self.watch(default_tube) if default_tube
    end

    def connect
      host, port = addr.split(':')
      @socket = TCPSocket.new(host, port.to_i)

      # Don't leak fds when we exec.
      @socket.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
    end

    def close
      @socket.close
      @socket = nil
    end

    def put(body, pri=65536, delay=0, ttr=120)
      pri = pri.to_i
      delay = delay.to_i
      ttr = ttr.to_i
      body = "#{body}" # Make sure that body.bytesize gives a useful number
      interact("put #{pri} #{delay} #{ttr} #{body.bytesize}\r\n#{body}\r\n",
               %w(INSERTED BURIED))[0].to_i
    end

    def yput(obj, pri=65536, delay=0, ttr=120)
      put(YAML.dump(obj), pri, delay, ttr)
    end

    def peek_job(id)
      interact("peek #{id}\r\n", :job)
    end

    def peek_ready()
      interact("peek-ready\r\n", :job)
    end

    def peek_delayed()
      interact("peek-delayed\r\n", :job)
    end

    def peek_buried()
      interact("peek-buried\r\n", :job)
    end

    def on_tube(tube, &block)
      @tube_mutex.lock
      use tube
      yield self
    ensure
      @tube_mutex.unlock
    end

    def reserve(timeout=nil)
      raise WaitingForJobError if @waiting
      @mutex.lock
      if timeout.nil?
        @socket.write("reserve\r\n")
      else
        @socket.write("reserve-with-timeout #{timeout}\r\n")
      end

      begin
        @waiting = true
        # Give the user a chance to select on multiple fds.
        Beanstalk.select.call([@socket]) if Beanstalk.select
      rescue WaitingForJobError
        # just continue
      ensure
        @waiting = false
      end

      Job.new(self, *read_job('RESERVED'))
    ensure
      @mutex.unlock
    end

    def delete(id)
      interact("delete #{id}\r\n", %w(DELETED))
      :ok
    end

    def release(id, pri, delay)
      id = id.to_i
      pri = pri.to_i
      delay = delay.to_i
      interact("release #{id} #{pri} #{delay}\r\n", %w(RELEASED))
      :ok
    end

    def bury(id, pri)
      interact("bury #{id} #{pri}\r\n", %w(BURIED))
      :ok
    end

    def touch(id)
      interact("touch #{id}\r\n", %w(TOUCHED))
      :ok
    end

    def kick(n)
      interact("kick #{n}\r\n", %w(KICKED))[0].to_i
    end

    def use(tube)
      return tube if tube == @last_used
      @last_used = interact("use #{tube}\r\n", %w(USING))[0]
    rescue BadFormatError
      raise InvalidTubeName.new(tube)
    end

    def watch(tube)
      return @watch_list.size if @watch_list.include?(tube)
      r = interact("watch #{tube}\r\n", %w(WATCHING))[0].to_i
      @watch_list += [tube]
      return r
    rescue BadFormatError
      raise InvalidTubeName.new(tube)
    end

    def ignore(tube)
      return @watch_list.size if !@watch_list.include?(tube)
      r = interact("ignore #{tube}\r\n", %w(WATCHING))[0].to_i
      @watch_list -= [tube]
      return r
    end

    def stats()
      interact("stats\r\n", :yaml)
    end

    def job_stats(id)
      interact("stats-job #{id}\r\n", :yaml)
    end

    def stats_tube(tube)
      interact("stats-tube #{tube}\r\n", :yaml)
    end

    def list_tubes()
      interact("list-tubes\r\n", :yaml)
    end

    def list_tube_used()
      interact("list-tube-used\r\n", %w(USING))[0]
    end

    def list_tubes_watched(cached=false)
      return @watch_list if cached
      @watch_list = interact("list-tubes-watched\r\n", :yaml)
    end

    private

    def interact(cmd, rfmt)
      raise WaitingForJobError if @waiting
      @mutex.lock
      @socket.write(cmd)
      return read_yaml('OK') if rfmt == :yaml
      return found_job if rfmt == :job
      check_resp(*rfmt)
    ensure
      @mutex.unlock
    end

    def get_resp()
      r = @socket.gets("\r\n")
      raise EOFError if r == nil
      r[0...-2]
    end

    def check_resp(*words)
      r = get_resp()
      rword, *vals = r.split(/\s+/)
      if (words.size > 0) and !words.include?(rword)
        raise UnexpectedResponse.classify(rword, r)
      end
      vals
    end

    def found_job()
      Job.new(self, *read_job('FOUND'))
    rescue NotFoundError
      nil
    end

    def read_job(word)
      id, bytes = check_resp(word).map{|s| s.to_i}
      body = read_bytes(bytes)
      raise 'bad trailer' if read_bytes(2) != "\r\n"
      [id, body, word == 'RESERVED']
    end

    def read_yaml(word)
      bytes_s, = check_resp(word)
      yaml = read_bytes(bytes_s.to_i)
      raise 'bad trailer' if read_bytes(2) != "\r\n"
      YAML::load(yaml)
    end

    def read_bytes(n)
      str = @socket.read(n)
      raise EOFError, 'End of file reached' if str == nil
      raise EOFError, 'End of file reached' if str.size < n
      str
    end
  end

  class Pool
    attr_accessor :last_conn

    def initialize(addrs, default_tube=nil)
      @addrs = addrs
      @watch_list = ['default']
      @default_tube=default_tube
      @watch_list = [default_tube] if default_tube
      connect()
    end

    def connect()
      @connections ||= {}
      @addrs.each do |addr|
        begin
          if !@connections.include?(addr)
            @connections[addr] = Connection.new(addr, @default_tube)
            prev_watched = @connections[addr].list_tubes_watched()
            to_ignore = prev_watched - @watch_list
            @watch_list.each{|tube| @connections[addr].watch(tube)}
            to_ignore.each{|tube| @connections[addr].ignore(tube)}
          end
        rescue Errno::ECONNREFUSED
          raise NotConnected
        rescue Exception => ex
          puts "#{ex.class}: #{ex}"
        end
      end
      @connections.size
    end

    def open_connections()
      @connections.values()
    end

    def last_server
      @last_conn.addr
    end

    # Put a job on the queue.
    #
    # == Parameters:
    #
    # * <tt>body</tt>: the payload of the job (use Beanstalk::Pool#yput / Beanstalk::Job#ybody to automatically serialize your payload with YAML)
    # * <tt>pri</tt>: priority. Default 65536 (higher numbers are higher priority)
    # * <tt>delay</tt>: how long to wait until making the job available for reservation
    # * <tt>ttr</tt>: time in seconds for the job to reappear on the queue (if beanstalk doesn't hear from a consumer within this time, assume the consumer died and make the job available for someone else to process).  Default 120 seconds.
    def put(body, pri=65536, delay=0, ttr=120)
      send_to_rand_conn(:put, body, pri, delay, ttr)
    end

    # Like put, but serialize the object with YAML.
    def yput(obj, pri=65536, delay=0, ttr=120)
      send_to_rand_conn(:yput, obj, pri, delay, ttr)
    end

    def on_tube(tube, &block)
      send_to_rand_conn(:on_tube, tube, &block)
    end

    # Reserve a job from the queue.
    #
    # == Parameters
    #
    # * <tt>timeout</tt> - Time (in seconds) to wait for a job to be available. If nil, wait indefinitely.
    def reserve(timeout=nil)
      send_to_rand_conn(:reserve, timeout)
    end

    def use(tube)
      send_to_all_conns(:use, tube)
    end

    def watch(tube)
      r = send_to_all_conns(:watch, tube)
      @watch_list = send_to_rand_conn(:list_tubes_watched, true)
      return r
    end

    def ignore(tube)
      r = send_to_all_conns(:ignore, tube)
      @watch_list = send_to_rand_conn(:list_tubes_watched, true)
      return r
    end

    def raw_stats()
      send_to_all_conns(:stats)
    end

    def stats()
      sum_hashes(raw_stats.values)
    end

    def raw_stats_tube(tube)
      send_to_all_conns(:stats_tube, tube)
    end

    def stats_tube(tube)
      sum_hashes(raw_stats_tube(tube).values)
    end

    def list_tubes()
      send_to_all_conns(:list_tubes)
    end

    def list_tube_used()
      send_to_all_conns(:list_tube_used)
    end

    def list_tubes_watched(*args)
      send_to_all_conns(:list_tubes_watched, *args)
    end

    def remove(conn)
      connection = @connections.delete(conn.addr)
      connection.close if connection
      connection
    end

    # Close all open connections for this pool
    def close
      while @connections.size > 0
        addr = @connections.keys.last
        conn = @connections[addr]
        @connections.delete(addr)
        conn.close
      end
    end

    def peek_ready()
      send_to_each_conn_first_res(:peek_ready)
    end

    def peek_delayed()
      send_to_each_conn_first_res(:peek_delayed)
    end

    def peek_buried()
      send_to_each_conn_first_res(:peek_buried)
    end

    def peek_job(id)
      make_hash(send_to_all_conns(:peek_job, id))
    end

    private

    def call_wrap(c, *args, &block)
      self.last_conn = c
      c.send(*args, &block)
    rescue UnexpectedResponse => ex
      raise ex
    rescue EOFError, Errno::ECONNRESET, Errno::EPIPE => ex
      self.remove(c)
      raise ex
    end

    def retry_wrap(*args)
      yield
    rescue DrainingError
      # Don't reconnect -- we're not interested in this server
      retry
    rescue EOFError, Errno::ECONNRESET, Errno::EPIPE
      connect()
      retry
    end

    def send_to_each_conn_first_res(*args)
      connect()
      retry_wrap{open_connections.inject(nil) {|r,c| r or call_wrap(c, *args)}}
    end

    def send_to_rand_conn(*args, &block)
      connect()
      retry_wrap{call_wrap(pick_connection, *args, &block)}
    end

    def send_to_all_conns(*args)
      connect()
      retry_wrap{compact_hash(map_hash(@connections){|c| call_wrap(c, *args)})}
    end

    def pick_connection()
      open_connections[rand(open_connections.size)] or raise NotConnected
    end

    def make_hash(pairs)
      Hash[*pairs.inject([]){|a,b|a+b}]
    end

    def map_hash(h)
      make_hash(h.map{|k,v| [k, yield(v)]})
    end

    def compact_hash(hash)
      hash.reject{|k,v| v == nil}
    end

    def sum_hashes(hs)
      hs.inject({}){|a,b| a.merge(b) {|k,o,n| combine_stats(k, o, n)}}
    end

    DONT_ADD = Set['name', 'version', 'pid']
    def combine_stats(k, a, b)
      DONT_ADD.include?(k) ? Set[a] + Set[b] : a + b
    end
  end
end
