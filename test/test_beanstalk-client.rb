require 'helper'

class TestBeanstalkClient < Test::Unit::TestCase
  def test_truth
    assert true
  end

  def setup
    @beanstalk = Beanstalk::Pool.new(['127.0.0.1:11300'])
    @tubes = ['one', 'two']

    # Put something on each tube so they exist
    @beanstalk.use('one')
    @beanstalk.put('one')

    @beanstalk.use('two')
    @beanstalk.put('two')
  end

  def test_not_thread_safe
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

    one = @beanstalk.stats_tube 'one'
    two = @beanstalk.stats_tube 'two'

    assert_equal one['current-jobs-ready'], 1
    assert_equal two['current-jobs-ready'], 3
  end

  def test_thread_safe
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

    one = @beanstalk.stats_tube 'one'
    two = @beanstalk.stats_tube 'two'

    assert_equal one['current-jobs-ready'], 2
    assert_equal two['current-jobs-ready'], 2
  end

  def teardown
    # Clear the tubes
    @tubes.each do |tube|
      stats = @beanstalk.stats_tube tube
      num_jobs = stats['current-jobs-ready']
      @beanstalk.watch tube
      num_jobs.times do
        job = @beanstalk.reserve
        job.delete
      end
      @beanstalk.ignore tube
    end
  end

end
