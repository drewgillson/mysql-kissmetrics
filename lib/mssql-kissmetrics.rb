require "mssql-kissmetrics/version"

module MssqlKissmetrics
    
    require 'dbi'
    require 'createsend'
    require 'km'
    require 'date'

    def self.initialize(profile, username, password, allowed_history_days = 0)
        @allowed_history_days = allowed_history_days
        @now = DateTime.now.to_time.to_i

        DBI.connect('dbi:ODBC:' << profile, username, password) do |dbh|
        dbh.select_all("SELECT increment_id FROM sales_flat_order LIMIT 0,10") do |row|
            puts row['increment_id']
        end
    end

end
