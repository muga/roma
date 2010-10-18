#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'optparse'
require 'roma/config'
require 'roma/stats'
require 'roma/routing/routing_data'
require 'roma/routing/cb_rttable'

module Roma
  module Tools
    class SetData
      attr :storages
      attr :rttable

      def initialize(argv = nil)
        @stats = Roma::Stats.instance
        options(argv)
        initialize_rttable
        initialize_storages
      end

      def options(argv)
        opts = OptionParser.new

        opts.banner="usage:#{File.basename($0)} [options] address"
        opts.on_tail("-h", "--help", "Show this message.") {
          puts opts; exit
        }

        @stats.port = Roma::Config::DEFAULT_PORT.to_s
        opts.on("-p", "--port [PORT]") { |v| @stats.port = v }

        opts.parse!(argv)
        raise OptionParser::ParseError.new if argv.length < 1

        @stats.address = argv[0]

        unless @stats.port =~ /^\d+$/
          raise OptionParser::ParseError.new('Port number is not numeric.')
        end
      rescue OptionParser::ParseError => e
        $stderr.puts e.message
        $stderr.puts opts.help
        exit 1
      end

      def initialize_storages
        @storages = {}
        nid = @stats.ap_str
        st = Roma::Config::STORAGE_CLASS.new
        st.storage_path = "#{Roma::Config::STORAGE_PATH}/#{nid}/roma"
        st.vn_list = @rttable.vnodes
        st.divnum = Roma::Config::STORAGE_DIVNUM
        st.option = Roma::Config::STORAGE_OPTION
        @storages[nid] = st
      end

      def initialize_rttable
        raise "#{@stats.ap_str}.route not found." unless File::exist?("#{@stats.ap_str}.route")
        rd = Roma::Routing::RoutingData::load("#{@stats.ap_str}.route")
        raise "It failed in loading the routing table data." unless rd
        @rttable = Roma::Routing::ChurnbasedRoutingTable.new(rd,"#{@stats.ap_str}.route")
      end

      def open
        @storages.each { |hashname, st|
          st.opendb
        }
      end

      def close
        @storages.each { |hashname, st|
          st.closedb
        }
      end

      def set key, expt, val
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes.include?(@stats.ap_str)
          res = storages[@stats.ap_str].set(vn, key, d, 0x7fffffff, val)
          puts "set k=#{key}, #{res}"
        end
      end
    end # class SetData
  end # module Tools
end # module Roma

