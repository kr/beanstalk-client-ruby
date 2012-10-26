require 'helper'

describe "beanstalk-client" do
  before do
    @beanstalk = Beanstalk::Pool.new(['127.0.0.1:11300'])
    @tubes = ['one', 'two', 'three']

    # Put something on each tube so they exist
    @beanstalk.use('one')
    @beanstalk.put('one')

    @beanstalk.use('two')
    @beanstalk.put('two')
  end

  describe "test not thread safe" do
    before do
      # Create threads that will execute
      # A: use one
      # B: use one
      # B: put two
      # A: put one
      a = Thread.new do
        @beanstalk.use('one')
        sleep 4
        @beanstalk.put('one')
      end

      b = Thread.new do
        sleep 1
        @beanstalk.use('two')
        @beanstalk.put('two')
      end

      a.join
      b.join
    end

    it "should return correct current-jobs-ready for tube one" do
      one = @beanstalk.stats_tube 'one'
      assert_equal one['current-jobs-ready'], 1
    end

    it "should return correct current-jobs-ready for tube two" do
      two = @beanstalk.stats_tube 'two'
      assert_equal two['current-jobs-ready'], 3
    end
  end

  describe "test thread safe" do
    before do
      a = Thread.new do
        @beanstalk.on_tube('one') do |conn|
          sleep 4
          conn.put('one')
        end
      end

      b = Thread.new do
        @beanstalk.on_tube('two') do |conn|
          sleep 1
          conn.put('two')
        end
      end

      a.join
      b.join
    end

    it "should return correct current-jobs-ready for tube one" do
      one = @beanstalk.stats_tube 'one'
      assert_equal one['current-jobs-ready'], 2
    end

    it "should return correct current-jobs-ready for tube two" do
      two = @beanstalk.stats_tube 'two'
      assert_equal two['current-jobs-ready'], 2
    end
  end

  describe "test delete job in reserved state" do
    before do
      @beanstalk.on_tube('three') do |conn|
        conn.put('one')
      end
      @beanstalk.watch 'three'
      @job = @beanstalk.reserve
    end

    it "should be deleted properly" do
      assert_equal 'one', @job.body
      assert_equal 'one', @beanstalk.peek_job(@job.id).values.first.body
      @job.delete
      assert_nil @beanstalk.peek_job(@job.id).values.first
    end
  end

  describe "test delete job in buried state" do
    before do
      @beanstalk.on_tube('three') do |conn|
        conn.put('two')
      end
      @beanstalk.watch 'three'
      @job = @beanstalk.reserve
    end

    it "should delete job as expected in buried state" do
      assert_equal 'two', @job.body
      @job.bury
      assert_equal 'two', @beanstalk.peek_buried.body

      @job.delete
      assert_nil @beanstalk.peek_job(@job.id).values.first
    end
  end

  after do
    # Clear the tubes
    @tubes.each do |tube|
      begin
        stats = @beanstalk.stats_tube tube
        num_jobs = stats['current-jobs-ready']
        @beanstalk.watch tube
        num_jobs.times do
          job = @beanstalk.reserve
          job.delete
        end
        @beanstalk.ignore tube
      rescue Beanstalk::NotFoundError
        next # skip tube
      end
    end
  end

end
