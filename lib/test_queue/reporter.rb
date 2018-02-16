module TestQueue
  class Reporter
    def initialize(completed:)
      @completed = completed
      @failures = ''

      @stats = Stats.new(stats_file)
    end

    def summarize
      puts
      puts "==> Summary (#{@completed.size} workers in %.4fs)" % (Time.now-@start_time)
      puts

      @completed.each do |worker|
        @failures << worker.failure_output if worker.failure_output

        puts "    [%2d] %60s      %4d suites in %.4fs      (%s %s)" % [
          worker.num,
          worker.summary,
          worker.suites.size,
          worker.end_time - worker.start_time,
          worker.status.to_s,
        ]
      end

      unless @failures.empty?
        puts
        puts "==> Failures"
        puts
        puts @failures
      end

      puts

      summarize_extra
    end

    def summarize_extra
    end

    def stats_file
      ENV['TEST_QUEUE_STATS'] ||
      '.test_queue_stats'
    end
  end
end
