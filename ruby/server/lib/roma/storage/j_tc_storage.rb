require 'java'
#require '../../java/server/target/ROMA-java-server-0.8.5-jar-with-dependencies.jar'
java_import Java::jp.co.rakuten.rit.roma.storage::TCHashDataStore
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class JavaTCStorage < BasicStorage

      def initialize
        super
        @ext_name = 'tc'
      end

      def get_stat
        ret = super
        @hdb.each_with_index{|hdb,idx|
          ret["storage[#{idx}].path"] = File.expand_path(hdb.path)
          ret["storage[#{idx}].rnum"] = hdb.rnum
          ret["storage[#{idx}].fsiz"] = hdb.fsiz
        }
        ret
      end
      
      def set(vn, k, d, expt, v)
        buf0 = @hdb[@hdiv[vn]].get(k.to_java_bytes)
        buf = String.from_java_bytes buf0 if buf0
        clk = 0
        if buf
          data = unpack_data(buf)
          clk = (data[2] + 1) & 0xffffffff
        end

        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if @hdb[@hdiv[vn]].put(k.to_java_bytes, pack_data(*ret).to_java_bytes)
        nil
      end
      
      def get(vn, k, d)
        buf0 = @hdb[@hdiv[vn]].get(k.to_java_bytes)
        buf = String.from_java_bytes buf0
        return nil unless buf
        vn, t, clk, expt, v = unpack_data(buf)
        return nil if Time.now.to_i > expt
        v
      end

      protected

      def set_options(hdb)
        prop = parse_options

        prop.each_key{|k|
          unless /^(bnum|apow|fpow|opts|xmsiz|rcnum|dfunit)$/ =~ k
            raise RuntimeError.new("Syntax error, unexpected option #{k}")
          end
        }
        
        opts = 0
        if prop.key?('opts')
          opts |= HDB::TLARGE if prop['opts'].include?('l')
          opts |= HDB::TDEFLATE if prop['opts'].include?('d')
          opts |= HDB::TBZIP if prop['opts'].include?('b')
          opts |= HDB::TTCBS if prop['opts'].include?('t')
        end

        hdb.tune(prop['bnum'].to_i,prop['apow'].to_i,prop['fpow'].to_i,opts)

        hdb.setxmsiz(prop['xmsiz'].to_i) if prop.key?('xmsiz')
        hdb.setcache(prop['rcnum'].to_i) if prop.key?('rcnum')
        hdb.setdfunit(prop['dfunit'].to_i) if prop.key?('dfunit')
      end

      private

      def parse_options
        return Hash.new(-1) unless @option
        buf = @option.split('#')
        prop = Hash.new(-1)
        buf.each{|equ|
          if /(\S+)\s*=\s*(\S+)/ =~ equ
            prop[$1] = $2
          else
            raise RuntimeError.new("Option string parse error.")
          end
        }
        prop
      end

      def open_db(fname)
        hdb = TCHashDataStore::new

        set_options(hdb)

        if !hdb.open(fname, TCHashDataStore::OWRITER | TCHashDataStore::OCREAT | TCHashDataStore::ONOLCK)
          ecode = hdb.ecode
          raise RuntimeError.new("tcdb open error #{hdb.errmsg(ecode)}")
        end
        hdb
      end

      def close_db(hdb)
        if !hdb.close
          ecode = hdb.ecode
          raise RuntimeError.new("tcdb close error #{hdb.errmsg(ecode)}")
        end        
      end

    end # class TCStorage

  end # module Storage
end # module Roma
