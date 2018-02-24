require 'set'
require 'fileutils'
require 'securerandom'
require 'yaml'
require 'test_queue/stats'
require 'test_queue/test_framework'

module TestQueue
  class Worker
    attr_accessor :pid, :status, :output, :num, :host
    attr_accessor :start_time, :end_time
    attr_accessor :summary, :failure_output

    # Array of TestQueue::Stats::Suite recording all the suites this worker ran.
    attr_reader :suites

    def initialize(pid, num)
      @pid = pid
      @num = num
      @start_time = Time.now
      @output = ''
      @suites = []
    end

    def lines
      @output.split("\n")
    end
  end

  class Runner
    attr_accessor :concurrency, :exit_when_done
    attr_reader :stats

    def initialize(test_framework, concurrency=nil, relay=nil)
      @test_framework = test_framework
      @stats = Stats.new(stats_file)

      if ENV['TEST_QUEUE_EARLY_FAILURE_LIMIT']
        begin
          @early_failure_limit = Integer(ENV['TEST_QUEUE_EARLY_FAILURE_LIMIT'])
        rescue ArgumentError
          raise ArgumentError, 'TEST_QUEUE_EARLY_FAILURE_LIMIT could not be parsed as an integer'
        end
      end

      @procline = $0

      @whitelist = if forced = ENV['TEST_QUEUE_FORCE']
                     forced.split(/\s*,\s*/)
                   else
                     []
                   end
      @whitelist.freeze

      all_files = @test_framework.all_suite_files.to_set
      @queue = @stats.all_suites
        .select { |suite| all_files.include?(suite.path) }
        .sort_by { |suite| -suite.duration }
        .map { |suite| [suite.name, suite.path] }

      if @whitelist.any?
        @queue.select! { |suite_name, path| @whitelist.include?(suite_name) }
        @queue.sort_by! { |suite_name, path| @whitelist.index(suite_name) }
      end

      @awaited_suites = Set.new(@whitelist)
      @original_queue = Set.new(@queue).freeze
      @loaded_queue = []

      @workers = {}
      @completed = []

      @concurrency =
        concurrency ||
        (ENV['TEST_QUEUE_WORKERS'] && ENV['TEST_QUEUE_WORKERS'].to_i) ||
        if File.exist?('/proc/cpuinfo')
          File.read('/proc/cpuinfo').split("\n").grep(/processor/).size
        elsif RUBY_PLATFORM =~ /darwin/
          `/usr/sbin/sysctl -n hw.activecpu`.to_i
        else
          2
        end
      unless @concurrency > 0
        raise ArgumentError, "Worker count (#{@concurrency}) must be greater than 0"
      end

      @assignments = {}

      @exit_when_done = true

      @aborting = false
    end

    # Run the tests.
    #
    # If exit_when_done is true, exit! will be called before this method
    # completes. If exit_when_done is false, this method will return an Integer
    # number of failures.
    def execute
      $stdout.sync = $stderr.sync = true
      @start_time = Time.now

      execute_internal
      exitstatus = summarize_internal

      if exit_when_done
        exit! exitstatus
      else
        exitstatus
      end
    end

    def summarize_internal
      estatus = 0
      results = {
        "completed" => [],
      }

      results["duration"] = Time.now - @start_time
      @completed.each do |worker|
        estatus += (worker.status.exitstatus || 1)
        results["completed"] << worker
        @stats.record_suites(worker.suites)

        summarize_worker(worker)
      end

      File.open("results.yml", "w") do |f|
        f.puts results.to_yaml
      end

      @stats.save

      estatus = @completed.inject(0){ |s, worker| s + (worker.status.exitstatus || 1)}
      [estatus, 255].min
    end

    def stats_file
      ENV['TEST_QUEUE_STATS'] ||
      '.test_queue_stats'
    end

    def execute_internal
      prepare(@concurrency)
      @prepared_time = Time.now
      load_queue
      distribute_queue
    ensure
      kill_subprocesses
    end

    def load_queue
      @queue.flat_map do |_, suite_file|
        @test_framework.suites_from_file(suite_file).flat_map { |_, suites| suites}
      end
    end

    def distribute_queue
      if @loaded_queue.length > 0
        queues = @loaded_queue.each_slice(@loaded_queue.length / @concurrency).to_a
      else
        queues = @concurrency.times.map { [] }
      end
      @concurrency.times do |i|
        num = i+1

        pid = fork do
          iterator = Iterator.new(queues[i], method(:around_filter), early_failure_limit: @early_failure_limit)
          after_fork_internal(num, iterator)
          ret = run_worker(iterator) || 0
          cleanup_worker
          Kernel.exit! ret
        end

        @workers[pid] = Worker.new(pid, num)
      end
    end

    def awaiting_suites?
      case
      when @awaited_suites.any?
        # We're waiting to find all the whitelisted suites so we can run them
        # in the correct order.
        true
      when @queue.empty? && !!@discovering_suites_pid
        # We don't have any suites yet, but we're working on it.
        true
      else
        # It's fine to run any queued suites now.
        false
      end
    end

    def after_fork_internal(num, iterator)
      srand

      output = File.open("/tmp/test_queue_worker_#{$$}_output", 'w')

      $stdout.reopen(output)
      $stderr.reopen($stdout)
      $stdout.sync = $stderr.sync = true

      $0 = "test-queue worker [#{num}]"
      puts
      puts "==> Starting #$0 (#{Process.pid} on #{Socket.gethostname}) - iterating over #{iterator}"
      puts

      after_fork(num)
    end

    # Run in the master before the fork. Used to create
    # concurrency copies of any databases required by the
    # test workers.
    def prepare(concurrency)
    end

    def around_filter(suite)
      yield
    end

    # Prepare a worker for executing jobs after a fork.
    def after_fork(num)
    end

    # Entry point for internal runner implementations. The iterator will yield
    # jobs from the shared queue on the master.
    #
    # Returns an Integer number of failures.
    def run_worker(iterator)
      iterator.each do |item|
        puts "  #{item.inspect}"
      end

      return 0 # exit status
    end

    def cleanup_worker
    end

    def summarize_worker(worker)
      worker.summary = ''
      worker.failure_output = ''
    end

    def reap_workers(blocking=true)
      @workers.delete_if do |_, worker|
        if Process.waitpid(worker.pid, blocking ? 0 : Process::WNOHANG).nil?
          next false
        end

        worker.status = $?
        worker.end_time = Time.now

        collect_worker_data(worker)
        worker_completed(worker)

        true
      end
    end

    def collect_worker_data(worker)
      if File.exist?(file = "/tmp/test_queue_worker_#{worker.pid}_output")
        worker.output = IO.binread(file)
        FileUtils.rm(file)
      end

      if File.exist?(file = "/tmp/test_queue_worker_#{worker.pid}_suites")
        worker.suites.replace(Marshal.load(IO.binread(file)))
        FileUtils.rm(file)
      end
    end

    def worker_completed(worker)
      return if @aborting
      @completed << worker
      puts worker.output if ENV['TEST_QUEUE_VERBOSE'] || worker.status.exitstatus != 0
    end

    def kill_subprocesses
      kill_workers
    end

    def kill_workers
      @workers.each do |pid, worker|
        Process.kill 'KILL', pid
      end

      reap_workers
    end

    # Stop the test run immediately.
    #
    # message - String message to print to the console when exiting.
    #
    # Doesn't return.
    def abort(message)
      @aborting = true
      kill_subprocesses
      Kernel::abort("Aborting: #{message}")
    end

    # Subclasses can override to monitor the status of the queue.
    #
    # For example, you may want to record metrics about how quickly remote
    # workers connect, or abort the build if not enough connect.
    #
    # This method is called very frequently during the test run, so don't do
    # anything expensive/blocking.
    #
    # This method is not called on remote masters when using remote workers,
    # only on the central master.
    #
    # start_time          - Time when the test run began
    # queue_size          - Integer number of suites left in the queue
    # local_worker_count  - Integer number of active local workers
    # remote_worker_count - Integer number of active remote workers
    #
    # Returns nothing.
    def queue_status(start_time, queue_size, local_worker_count, remote_worker_count)
    end
  end
end
