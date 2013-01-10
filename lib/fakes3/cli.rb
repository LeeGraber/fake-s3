require 'thor'
require 'fakes3/server'
require 'fakes3/version'

module FakeS3
  class CLI < Thor
    default_task("server")

    desc "server", "Run a server on a particular hostname"
    method_option :root, :type => :string, :aliases => '-r', :required => true
    method_option :port, :type => :numeric, :aliases => '-p', :required => true
    method_option :address, :type => :string, :aliases => '-a', :required => false, :desc => "Bind to this address. Defaults to 0.0.0.0"
    method_option :hostname, :type => :string, :aliases => '-h', :desc => "The root name of the host.  Defaults to s3.amazonaws.com."
    method_option :limit, :aliases => '-l', :type => :string, :desc => 'Rate limit for serving (ie. 50K, 1.0M)'
    method_option :exists_delay, :aliases => '-ed', :type => :numeric, :required => false, :desc => "Delay confirmation of uploaded files for 'n' seconds. This helps simulate real S3 behavior"
    method_option :flakiness, :aliases => '-f', :type => :numeric, :required => false, :desc => "Introduce random flakiness (ie 503 errors right now). This simulates potential random failures from S3. Value is a number to represent ~ 1/n calls failing. This is not exact. Value must be >= 10"
    def server
      store = nil
      if options[:root]
        root = ::File.expand_path(options[:root])
        # TODO Do some sanity checking here
        store = FileStore.new(root)
      end

      if store.nil?
        puts "You must specify a root to use a file store (the current default)"
        exit(-1)
      end

      hostname = 's3.amazonaws.com'
      if options[:hostname]
        hostname = options[:hostname]
        # In case the user has put a port on the hostname
        if hostname =~ /:(\d+)/
          hostname = hostname.split(":")[0]
        end
      end

      if options[:limit]
        begin
          store.rate_limit = options[:limit]
        rescue
          puts $!.message
          exit(-1)
        end
      end

      if options[:exists_delay]
        begin 
          store.exists_delay = options[:exists_delay]
        rescue
          puts $!.message
          exit(-1)
        end
      end          

      address = options[:address] || '0.0.0.0'

      puts "Loading FakeS3 with #{root} on port #{options[:port]} with hostname #{hostname}"
      server = FakeS3::Server.new(address,options[:port],store,hostname)

      if options[:flakiness]
        begin 
          raise "Flakiness value must be an integer >= 10" if options[:flakiness] < 10 || !options[:flakiness].is_a?(Integer)
          server.flakiness = options[:flakiness]
        rescue
          puts $!.message
          exit(-1)
        end
      end 

      server.serve
    end

    desc "version", "Report the current fakes3 version"
    def version
      puts <<"EOF"
======================
FakeS3 #{FakeS3::VERSION}

Copyright 2012, Curtis Spencer (@jubos)
EOF
    end
  end
end
