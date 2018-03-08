module TestQueue
  class Iterator
    class TooManyFailures < Exception
      def initialize(failure_count, limit)
        super "Encountered too many failures (#{@failure_count}, max is #{@limit})"
      end
    end

    def initialize(suites, filter=nil, early_failure_limit: nil)
      @done = false
      @suite_stats = []
      @filter = filter
      @failures = 0
      @early_failure_limit = early_failure_limit
      @loaded_suites = suites
    end

    def each
      fail "already used this iterator. previous caller: #@done" if @done

      procline = $0

      @loaded_suites.each do |_, suite|
        # If we've hit too many failures in one worker, assume the entire
        # test suite is broken, and notify master so the run
        # can be immediately halted.
        if @early_failure_limit && @failures >= @early_failure_limit
          raise TooManyFailures.new(@failures, @early_failure_limit)
        end

        $0 = "#{procline} - #{suite.respond_to?(:description) ? suite.description : suite}"
        start = Time.now
        if @filter
          @filter.call(suite){ yield suite }
        else
          yield suite
        end
        @suite_stats << TestQueue::Stats::Suite.new(suite.name, suite.path, Time.now - start, Time.now)
        @failures += suite.failure_count if suite.respond_to? :failure_count
      end
    rescue Errno::ENOENT, Errno::ECONNRESET, Errno::ECONNREFUSED
    ensure
      $0 = procline
      @done = caller.first
      File.open("/tmp/test_queue_worker_#{$$}_suites", "wb") do |f|
        Marshal.dump(@suite_stats, f)
      end
    end

    include Enumerable

    def empty?
      false
    end
  end
end
